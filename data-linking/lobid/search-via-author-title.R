# title: Get lobid/GND id's via a title-author search
# desc: Example call: https://lobid.org/resources/search?q=contribution.agent.label%3AMelville+AND+title:Moby&format=json
# input:
# output:

library(tidyverse)
library(kabrutils)
library(DBI)
library(httr)
library(jsonlite)
library(fuzzyjoin)


# config ------------------------------------------------------------------

# help debugging: print data tables during API calls
verbose <- F 
# how should resulting data be described in db?
source_desc <- "lobid via author book search"
# abbreviation of external id
external_id_abbr<- "gnd"

# I parse the jsons by using the great jq JSON processor
# these are the 3 filters I use:

get_agents <- ".member[].contribution[]?  | {type: .agent?.type[], label: .agent?.label, role:.role?.id, gnd_id: .agent?.gndIdentifier}"
get_components <- ".member[].subject[].componentList[]? | {gnd_id: select(.type?).gndIdentifier, type: select(.gndIdentifier?).type[], label: select(.type?).label}"
get_lobid_ressource_id <- ".member[] | {source_id: .id}"

# load books, entities and already fetched/matched data --------------------

con <- connect_db(credential_name = "db_clean")

books <- tbl(con, "books_wide") %>%
  anti_join(
    tbl(con, "el_matches") %>%
      filter(entity_id_type == "books") %>%
      distinct(entity_id),
    by = c("book_id" = "entity_id")
  ) %>%
  collect() %>%
  mutate(
    isbn = str_remove_all(isbn, " "),
    book_id = as.character(book_id)
  ) %>%
  kabrutils::clean_string("title")

DBI::dbDisconnect(con)
rm(con)


# call lobid API ----------------------------------------------------------

books %>%
  rowid_to_column() %>%
  #filter(rowid == 1457) %>%
  # sample_n(2) %>%
  pmap(function(...) {
    current <- tibble(...)

    current_meta <- current %>% inner_join(books, by = c("id", "book_id", "name", "title", "subtitle", "isbn"))
    
    cli::cli_alert("id: {current$rowid}")

    query <- URLencode(paste0("contribution.agent.label:", current$name, "+AND+title:", current$title, "&format=json"))

    res <- httr::GET(call_lobid_api(query = query, verbose = T)) %>%
      httr::content(., as = "text")


    check_items <- get_field_values(input = res, input_type = "response", jq_syntax = ".totalItems")


    if (verbose == TRUE) {
      cli::cli_alert("number of items: {check_items}")
    }

    if (check_items > 0) {
      contribution_agent <- res %>%
        get_field_values(input_type = "response", get_agents) %>%
        purrr::map(jsonlite::fromJSON) %>%
        enframe() %>%
        unnest_wider(value)

      if (verbose == TRUE) {
        cli::cli_h1("contribution_agent")
        print(contribution_agent)
      }

      # contribution_agent_names <-
      contribution_agent_names <- contribution_agent %>%
        select(-name) %>%
        distinct() %>%
        mutate(label2 = label) %>%
        separate(label2, ",", into = c("lastname", "rest_of_name")) %>%
        mutate(
          rest_of_name = trimws(rest_of_name),
          name_compare = paste(rest_of_name, lastname)
        ) %>%
        # compare names with fuzzy join
        fuzzyjoin::stringdist_inner_join(
          current_meta %>% select(entity_id = id, name),
          by = c("name_compare" = "name"), max_dist = 3
        )

      data_agent <- tibble(
        entity_id = current_meta$id,
        entity_id_combination = current$book_id,
        entity_id_combination_type = "book",
        entity_id_type = "entities"
      ) %>%
        left_join(contribution_agent_names %>%
          select(-lastname, -rest_of_name, -name_compare), by = "entity_id")

      if (verbose == TRUE) {
        cli::cli_h1("data_agent/person")
        print(data_agent)
      }


      # get lobid ressource id --------------------------------------------------

      lobid_ressource_id <- res %>%
        get_field_values(input_type = "response", get_lobid_ressource_id) %>%
        purrr::map(jsonlite::fromJSON) %>%
        enframe() %>%
        unnest_wider(value) %>%
        select(-name) %>%
        mutate(source_id = glue::glue_collapse(source_id, sep = ", ")) %>%
        distinct()

      if (verbose == TRUE) {
        cli::cli_h1("lobid_ressource_id")
        print(lobid_ressource_id)
      }

      # Get book topics ---------------------------------------------------------

      component_list <- res %>%
        get_field_values(input_type = "response", get_components) %>%
        purrr::map(jsonlite::fromJSON) %>%
        enframe() %>%
        unnest_wider(value)

      data_component <- tibble(
        entity_id = current$book_id,
        entity_id_combination = NA_character_,
        entity_id_combination_type = NA_character_,
        property_type = "topic",
        entity_id_type = "books"
      ) %>%
        bind_cols(component_list) %>%
        dplyr::select(-name) %>%
        distinct()

      if (verbose == TRUE) {
        cli::cli_h1("topic")
        print(data_component)
      }

      # Combine all data --------------------------------------------------------

      if (nrow(data_component) > 0 & nrow(data_agent) > 0) {
        data <-
          bind_rows(
            data_agent %>%
              rename(
                external_id = gnd_id,
                external_id_desc = type,
                external_id_label = label,
                property_type = role
              ),
            data_component %>%
              rename(
                external_id = gnd_id,
                external_id_desc = type,
                external_id_label = label
              ),
          ) %>% bind_cols(lobid_ressource_id)
      } else if (nrow(data_component) > 0 & nrow(data_agent) == 0) {
        data <- data_component %>%
          rename(
            external_id = gnd_id,
            external_id_desc = type,
            external_id_label = label
          ) %>%
          bind_cols(lobid_ressource_id)
      } else if (nrow(data_component) == 0 & nrow(data_agent) > 0) {
        data <- data_agent %>%
          rename(
            external_id = gnd_id,
            external_id_desc = type,
            external_id_label = label,
            property_type = role
          ) %>%
          bind_cols(lobid_ressource_id)
      } else {
        data <- tibble(
          entity_id = current$id,
          entity_id_type = "entities",
          external_id = NA_character_,
          external_id_type = external_id_abbr,
          entity_id_combination = NA_character_,
          entity_id_combination_type = "book",
          external_id_desc = NA_character_,
          external_id_label = NA_character_,
          property_type = NA_character_,
          source = NA_character_,
          source_id = NA_character_
        )
        # stop("not implemented")
      }


      # Prepare data for import -------------------------------------------------

      import <- data %>%
        filter(!is.na(external_id) | !is.na(external_id_desc)) %>%
        mutate(
          external_id_type = external_id_abbr,
          source = source_desc,
        ) %>%
        distinct(
          entity_id, entity_id_type, entity_id_combination, entity_id_combination_type,
          external_id, external_id_type, external_id_desc, external_id_label,
          property_type, source, source_id
        )

      if (verbose == TRUE) {
        cli::cli_h1("import")
        print(import)
      }

      # Write data in DB --------------------------------------------------------

      if (nrow(data) > 0) {
        con <- connect_db("db_clean")
        DBI::dbAppendTable(con, "el_matches", import)
        DBI::dbDisconnect(con)
        rm(con)
      }
    }
  })
