# title: Get lobid/GND id's via GND entities data
# desc: lobid also offers a gateway to GND entityfacts: https://lobid.org/gnd/api. I utilize that to find publishers. Example API call: https://lobid.org/gnd/search?q=preferredName:Max%20Spohr+AND+type:CorporateBody&format=json. Some data is stored in a json format for flexibility
# input: lgbtiq_kg_clean.entities
# output: lgbtiq_kg.el_matches

library(tidyverse)
library(kabrutils)
library(DBI)
library(httr)
library(jsonlite)
library(fuzzyjoin)


# config ------------------------------------------------------------------

# help debugging: print data tables during API calls
verbose <- T
# how should resulting data be described in db?
source_desc <- "lobid via publisher in entity search"
# abbreviation of external id
external_id_abbr <- "gnd"


# JSON parsers ------------------------------------------------------------
# get gnd_id and label/name in GND
get_id_and_label <- ".member[] | {external_id_label: .preferredName, external_id: .gndIdentifier}"
# all same_as IDs, also incl. deprecated gnd_id's
get_same_as <- ".member[] | {same_as: .sameAs[].id}"
# type, e.g PERSON or corporate body
get_entity_type <- ".member[] | {external_id_desc: .type[]}"
# publisher needs to be of "Buchwissenschaft, Buchhandel"
get_gnd_subject_cat <- ".member[] | {gndSubjectCategory: .gndSubjectCategory[].id}"
get_additional_data <- ".member[] | {id: .sameAs[].id, gndSubjectCategory: .gndSubjectCategory[].id}"

# Load input data ---------------------------------------------------------

con <- connect_db("db_clean")
entities <- tbl(con, "entities") %>% collect()
DBI::dbDisconnect(con)
rm(con)

con <- connect_db()

el_matches <- tbl(con, "el_matches") %>%
  filter(entity_id_type == "entities") %>%
  distinct(entity_id, external_id, external_id_label) %>%
  collect()

DBI::dbDisconnect(con)
rm(con)

publishers <- entities %>%
  left_join(el_matches %>% filter(!is.na(external_id)), by = c("id" = "entity_id")) %>%
  filter(is.na(external_id), str_detect(id, "publisher")) %>%
  mutate(
    name = str_replace_all(name, "\\[|\\]|\\(|\\)|\\/|\\:|-|!|\\.|,|\\?", " "),
    name = str_replace_all(name, "&", "and")
  ) %>%
  select(id, name) %>%
  arrange(name) %>%
  rowid_to_column() %>% 
  #remove when name just numbers (has to be error in input data) 
  filter(!str_detect(name, "^[0-9]{1,10}"))

# call lobid API ----------------------------------------------------------

message(paste0("search for: "), nrow(publishers), " entities")

publishers %>%
  #filter(id == "book_publisher_102") %>% # Max Spohr Verlag
  filter(rowid == 13) %>%
  # sample_n(2) %>%
  pmap(function(...) {
    current <- tibble(...)

    current_meta <- current %>% inner_join(publishers, by = c("id", "name"))

    cli::cli_alert("id: {current$rowid}")

    # https://lobid.org/gnd/search?q=preferredName:Max%20Spohr+AND+type:CorporateBody&format=json

    query <- URLencode(paste0("preferredName:", current$name, "+AND+type:CorporateBody"))

    res <- httr::GET(call_lobid_api(query = query, lobid_api_type = "gnd", verbose = T)) %>%
      httr::content(., as = "text")


    check_items <- get_field_values(input = res, input_type = "response", jq_syntax = ".totalItems")


    if (verbose == TRUE) {
      cli::cli_alert("number of items: {check_items}")
    }

    if (check_items > 0) {


      # Initialize empty dataframe ----------------------------------------------

      df_skeleton <- tibble(
        entity_id = current$id,
        entity_id_type = "entities",
        external_id_type = external_id_abbr,
        source = source_desc
      )
      
      # For reference: Full skeleton is
      # df_skeleton <- tibble(
      #   entity_id = character(),
      #   entity_id_type = character(),
      #   external_id = character(),
      #   external_id_type = external_id_abbr,
      #   entity_id_combination = character(),
      #   entity_id_combination_type = character(),
      #   external_id_desc = character(),
      #   external_id_label = character(),
      #   property_type = character(),
      #   source = character(),
      #   source_id = character(),
      #   additional_data = character()
      # )

      if (verbose == TRUE) {
        cli::cli_h1("df_skeleton")
        print(df_skeleton)
      }



      # Get label and gnd_id ----------------------------------------------------


      gnd_id_label <- res %>%
        get_field_values(input_type = "response", get_id_and_label) %>%
        transform_json_to_dataframe(unnest_type = "wide", keep_name = FALSE) %>% 
        mutate(source_id = paste0("http://lobid.org/gnd/", external_id))

      if (verbose == TRUE) {
        cli::cli_h1("gnd_id_label")
        print(gnd_id_label)
      }


      # get entity type  --------------------------------------------------------

      entity_type <- res %>%
        get_field_values(input_type = "response", get_entity_type) %>%
        transform_json_to_dataframe(unnest_type = "wide", keep_name = T) %>% 
        mutate(external_id_desc = as.character(glue::glue_collapse(external_id_desc, sep = ", "))) %>%
        distinct()

      if (verbose == TRUE) {
        cli::cli_h1("entity_type")
        print(entity_type)
      }

      # Get all sameAs id's -----------------------------------------------------
      #
      #       same_as <- res %>%
      #         get_field_values(input_type = "response", get_same_as) %>%
      #         transform_json_to_dataframe() %>%
      #         jsonlite::toJSON()
      #
      #       if (verbose == TRUE) {
      #         cli::cli_h1("same_as")
      #         print(str(same_as))
      #         print(same_as)
      #       }


      # get GND Subject Categories ----------------------------------------------

      # gnd_subject_cat <- res %>%
      #   get_field_values(input_type = "response", get_gnd_subject_cat) %>%
      #   transform_json_to_dataframe()
      #
      # if (verbose == TRUE) {
      #   cli::cli_h1("gnd_subject_cat")
      #   print(gnd_subject_cat)
      # }


      # get additional json data ------------------------------------------------

      additional_data <- res %>%
        get_field_values(input_type = "response", get_additional_data) %>%
        transform_json_to_dataframe(unnest_type = "wide", keep_name = T) %>% 
        nest(same = id) %>%
        jsonlite::toJSON()

      if (verbose == TRUE) {
        cli::cli_h1("additional_data as json column")
        print(additional_data)
      }


      # All data ----------------------------------------------------------------

      import <<- bind_cols(df_skeleton, gnd_id_label, entity_type, tibble(additional_data = additional_data))

      if (verbose == TRUE) {
        cli::cli_h1("import: df_all_data")
        print(import)
      }
      
      # Write data in DB --------------------------------------------------------
      
      # if (nrow(import) > 0) {
      #   con <- connect_db()
      #   DBI::dbAppendTable(con, "el_matches", import)
      #   DBI::dbDisconnect(con)
      #   rm(con)
      # }
    }
  })


cli::cli_h1("Well, I am done ...")
