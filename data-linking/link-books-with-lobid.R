# title: Link books to LOBID
# desc: Für welche ISBN-Nummern gibt es eine jsonld-Datei bei lobid?
# input:
# output:

library(tidyverse)
library(kabrutils)
library(DBI)
library(httr)
library(jsonlite)

source("data-linking/lobid-functions.R")

# Get books data ----------------------------------------------------------

con <- connect_db(credential_name = "db_clean")
books <- tbl(con, "books_wide") %>%
  collect() %>%
  mutate(isbn = str_remove_all(isbn, " "))
DBI::dbDisconnect(con)
rm(con)

no_isbn <- books %>% filter(is.na(isbn))
with_isbn <- books %>% filter(!is.na(isbn))

get_components <- ".member[].subject[].componentList[]? | {gnd_id: select(.type?).gndIdentifier, type: select(.gndIdentifier?).type[], label: select(.type?).label}"

get_agents <- ".member[].contribution[]?  | {type: .agent?.type[], label: .agent?.label, role:.role?.id, gnd_id: .agent?.gndIdentifier}"

books_authors_gnd_ids <- books %>%
  filter(!is.na(isbn)) %>%
  distinct(book_id, isbn, name, title) %>% 
  rowid_to_column() %>% 
  pmap_dfr(function(...) {
    
    current <- tibble(...)
    
    cli::cli_alert("id: {current$rowid}")
    
    if(!is.na(current$isbn)) {
      component_list <- call_lobid_api(query = current$isbn, parameter = "isbn", verbose = T) %>%
        get_field_values(get_components) %>%
        purrr::map(jsonlite::fromJSON) %>%
        enframe() %>%
        unnest_wider(value)
      
      contribution_agent <- call_lobid_api(query = current$isbn, parameter = "isbn", verbose = F) %>%
        get_field_values(get_agents) %>%
        purrr::map(jsonlite::fromJSON) %>%
        enframe() %>%
        unnest_wider(value)
    } else if (is.na(current$isbn)) {

      # component_list <- call_lobid_api(query = paste0(current$name, current$title), verbose = T) %>%
      #   get_field_values(get_components) %>%
      #   purrr::map(jsonlite::fromJSON) %>%
      #   enframe() %>%
      #   unnest_wider(value)
      # 
      # contribution_agent <- call_lobid_api(query = paste0(current$name, current$title), verbose = F) %>%
      #   get_field_values(get_agents) %>%
      #   purrr::map(jsonlite::fromJSON) %>%
      #   enframe() %>%
      #   unnest_wider(value)
    }

    data_component <- tibble(
      book_id = current$book_id,
      isbn = current$isbn,
      relation_type = "componentList"
    ) %>%
      bind_cols(component_list) %>%
      dplyr::select(-name)

    data_agent <- tibble(
      book_id = current$book_id,
      isbn = current$isbn,
      relation_type = "contribution_agent"
    ) %>%
      bind_cols(contribution_agent) %>%
      dplyr::select(-name)

    data <- bind_rows(data_component, data_agent)
  }) %>%
  distinct() %>%
  inner_join(books %>% distinct(book_id, isbn) %>% filter(!is.na(isbn)), by = c("book_id", "isbn"))



# Get json / lobid from isbn search -----------------------------------------------

books_authors_gnd_ids_json <- books %>%
  filter(!is.na(isbn)) %>%
  distinct(book_id, isbn, name, title) %>% 
  rowid_to_column() %>% 
  sample_n(1) %>% 
  pmap_dfr(function(...) {
    current <- tibble(...)
    
    cli::cli_alert("id: {current$rowid}")
    
      get_lobid <- call_lobid_api(query = current$isbn, parameter = "isbn", verbose = T) %>%
        get_field_values(".member[] | {lobid: .id}") %>%
        purrr::map(jsonlite::fromJSON) %>%
        enframe() %>%
        unnest_wider(value) %>% 
        select(-name)
      
      data <- bind_cols(current, get_lobid) 
    })
