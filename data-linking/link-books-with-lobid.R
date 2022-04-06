# title: Link entities and books to LOBID
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
  distinct(book_id, id, isbn, name, title) %>%
  collect() %>%
  mutate(
    isbn = str_remove_all(isbn, " "),
    book_id = as.character(book_id))

el_matches <- tbl(con, "el_matches") %>% collect()
DBI::dbDisconnect(con)
rm(con)

get_agents <- ".member[].contribution[]?  | {type: .agent?.type[], label: .agent?.label, role:.role?.id, gnd_id: .agent?.gndIdentifier}"


# Pipeline to fetch data from lobid ---------------------------------------


books %>%
  filter(!is.na(isbn)) %>%
  # remove those that were already matched
  anti_join(el_matches, by = c("book_id" = "entity_id_combination", "id" = "entity_id")) %>% 
  rowid_to_column() %>%
  pmap_dfr(function(...) {
    current <- tibble(...)
    
    cli::cli_alert("id: {current$rowid}")
    
    res <- call_lobid_api(query = current$isbn, parameter = "isbn", verbose = T) %>% curl::curl()
    
    check_items <- get_field_values(input = res, input_type = "curl_response", jq_syntax = ".totalItems")
    cli::cli_h1("check_items raw")
    print(check_items)
    
    # Get lobid author/contributor info ---------------------------------------
    
    if (check_items > 0) {
      print("dsf")
      contribution_agent <- res %>%
        get_field_values(input_type = "curl_response", get_agents) %>%
        purrr::map(jsonlite::fromJSON) %>%
        enframe() %>%
        unnest_wider(value)
      
      # cli::cli_h1("author raw")
      # print(contribution_agent)
      
      data_agent <- tibble(
        entity_id = current$id,
        entity_id_combination = current$book_id,
        entity_id_combination_type = "book"
      ) %>%
        bind_cols(contribution_agent) %>%
        dplyr::select(-name) %>% 
        distinct()
      
      # cli::cli_h1("author")
      # print(data_agent)
      
      if (nrow(data_agent) > 0) {
        data <- data_agent %>% 
          rename(external_id = gnd_id, 
                 external_id_desc = type, 
                 external_id_label = label,
                 property_type = role)
      } else {
        data <- tibble(
          entity_id = current$id,
          entity_id_type = "entities",
          external_id = NA_character_,
          external_id_type = "gnd",
          entity_id_combination = NA_character_,
          entity_id_combination_type = "book",
          external_id_desc = NA_character_,
          external_id_label = NA_character_,
          property_type = NA_character_,
          source = NA_character_,
          source_id = NA_character_
        )
      }
      
      # cli::cli_h1("data after parsing")
      # print(data)  

# Combine all data --------------------------------------------------------


    # Sometimes there are multiple labels per person, e.g. Sade as 1) Sade and 2) Marquis de Sade
    if (is.list(data$label) == TRUE) {
      data <- data %>% unnest(label)
    } else {
      data
    }
    
    
    # Prepare data for import -------------------------------------------------
    
    
    import <- data %>%
      mutate(
        entity_id = current$id,
        entity_id_type = "entities",
        external_id_type = "gnd",
        source = "lobid via book isbn"
      ) %>%
      distinct(entity_id, entity_id_type, entity_id_combination, entity_id_combination_type, external_id, external_id_type, external_id_desc, external_id_label, property_type, source, source_id)
    
    cli::cli_h1("data after cleaning")
    print(import)
    
    
    # Write data in DB --------------------------------------------------------
    
    if (nrow(data) > 0) {
      con <- connect_db("db_clean")
      DBI::dbAppendTable(con, "el_matches", import)
      DBI::dbDisconnect(con)
      rm(con)
    }
    } 
    
  })
