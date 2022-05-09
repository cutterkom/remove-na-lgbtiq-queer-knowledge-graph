# title: get Wikidata-QIDs from previously fetched GND-IDs.
# desc: in the previous scripts I fetched and cleaned the matches between internal entities and GND/lobid and save the GND_IDs. This scripts takes those GND_IDs and searches for the corresponding Wikidata IDs. If no Wikidata ID is found, this info is saved as well, in order to make explicit if item has just not been searched for or no Wikidata item exists (is NA!!)
# input: lgbtiq_kg_clean.el_matches
# output: lgbtiq_kg_clean.el_matches with external_id_type == "wikidata"


library(tidyverse)
library(kabrutils)
library(DBI)
library(httr)
library(jsonlite)


# config ------------------------------------------------------------------

# help debugging: print data tables during API calls
verbose <- F

# how should resulting data be described in db?
source_desc <- "lobid via entity search using gnd_id"

# abbreviation of external id
external_id_abbr <- "wikidata"

# Load input data ---------------------------------------------------------

con <- connect_db("db_clean")
el_matches <- tbl(con, "el_matches") %>%
  collect()

DBI::dbDisconnect(con)
rm(con)

input_data <- el_matches %>%
  # remove those that were already searched for
  anti_join(el_matches %>% filter(external_id_type == "wikidata"), by = "id") %>% 
  distinct(id, external_id_type, external_id) %>%
  filter(external_id_type %in% c("lobid", "gnd"), !is.na(external_id))

# call lobid API ----------------------------------------------------------

message(paste0("search for: "), nrow(input_data), " entities")

input_data %>%
  pmap(function(...) {
    current <- tibble(...)

    cli::cli_alert("id: {current$external_id}")

    # https://lobid.org/gnd/3004283-5.json

    query <- URLencode(current$external_id)

    res <- httr::GET(call_lobid_api(query = query, lobid_api_type = "gnd_id_search", verbose = T)) %>%
      httr::content(., as = "text")
    
    if (str_detect(res, "Not found") == FALSE) {

    # JSON parsers ------------------------------------------------------------

    # get gnd_id and label/name in GND

    get_wikidata_id <- paste0('.sameAs[] | if select(.collection.abbr == "WIKIDATA") then {external_id: .id} else {external_id: "no_wikidata_qid"} end')

    # Initialize empty dataframe ----------------------------------------------

    df_skeleton <- tibble(
      id = current$id,
      entity_id_type = "entities",
      external_id_type = external_id_abbr,
      source = source_desc
    )

    # For reference: full skeleton
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


    # Get Wikidata QID --------------------------------------------------------

    wikidata_id <- res %>%
      get_field_values(input_type = "response", get_wikidata_id) %>%
      transform_json_to_dataframe(unnest_type = "wide", keep_name = FALSE)
    
    if (nrow(wikidata_id) == 0) {
      wikidata_id <- bind_rows(wikidata_id, tibble(external_id = "no_wikidata_id"))
    }

    if (verbose == TRUE) {
      cli::cli_h1("wikidata_id")
      print(wikidata_id)
    }


    # All data ----------------------------------------------------------------

    import <- bind_cols(df_skeleton, wikidata_id)

    if (verbose == TRUE) {
      cli::cli_h1("import: df_all_data")
      print(import)
    }

    # Write data in DB --------------------------------------------------------

    if (nrow(import) > 0) {
      con <- connect_db("db_clean")
      DBI::dbAppendTable(con, "el_matches", import)
      DBI::dbDisconnect(con)
      rm(con)
    }
  } else {print("ddd")}
    
  })


cli::cli_h1("Well, I am done ...")
