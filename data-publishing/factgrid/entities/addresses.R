# title: Write entities to Factgrid
# desc: 
# input: 
# output: 


library(tidyverse)
library(kabrutils)
library(WikidataR) # https://github.com/cutterkom/WikidataR
library(SPARQL)
library(tidygeocoder)
library(googlesheets4)


# Some config -------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

munich_label <- config$import_helper$munich_label

# gs_auth()
statements <- read_sheet(config$statements$gs_table, sheet = "statements")

# Functions ---------------------------------------------------------------
# later move to kabrutils

sparql_to_tibble <- function(query, endpoint, useragent) {
  useragent <- paste("Wikibase", R.version.string)
  res <- SPARQL(endpoint, query, curl_args = list(useragent = useragent))
  tibble(res$results) 
}

#' Add a statement to an item
#' 
#' This function helps to add certain statements. A statement consists of a PID-QID combination that is added to a dataframe. The PID will be the column name, QID the row content.
#' It assumes there is a dataframe that has at least three columns: (1) `statements`, (2) `pid`, (3) `qid`.
#' @param data dataframe to add the new column
#' @param available_statements dataframe with all statements that can be added with this method. It assumes there is a dataframe that has at least three columns: (1) `statement`, (2) `pid`, (3) `qid`.
#' @param new_statement string of new statement. Must exist in `available_statements$statement`. If statement is `coordinates`, then a column `longitude` and a column `latitude` is expected.
#' @param verbose show in terminal what was added
#' @param qid_from_row boolean; default `FALSE` - then QID is taken from dataframe `statements`, if `TRUE` id QID value should be taken from another row
#' @param col_for_row_content string; name of column in dataframe `data` that contains the QID values
#' @export
#' @examples 
#' statements <- data.frame(statement = c("my_statement"), pid = c("P2"), qid = c("Q1"))
#' data <- data.frame(item = "my item")
#' data %>% add_statement(available_statements = statements, new_statement = "my_statement")

add_statement <- function(data = NULL, 
                          available_statements = statements, 
                          new_statement = NULL,
                          verbose = TRUE, 
                          qid_from_row = FALSE,
                          col_for_row_content = NULL) {
  
  new_statement_df <- filter(available_statements, statement == new_statement)
  
  if(nrow(new_statement_df) == 0) {
    stop("Your statement can't be found. Please check if it exists in the table `available_statements` or if there's a typo.")
  }
  
  pid_as_column_name <- rlang::sym(new_statement_df$pid)
  
  if(qid_from_row == TRUE) {
    
    if (new_statement == "coordinates") {
      latitude <- "latitude"
      longitude <- "longitude"
      data <- data %>% mutate(
        !!pid_as_column_name := 
          case_when(
            !is.na(.data[[latitude]]) ~ paste0("@", .data[[latitude]], "/", .data[[longitude]]),
            TRUE ~ NA_character_
          ))
    } else {
      data <- mutate(data, !!pid_as_column_name := .data[[col_for_row_content]])  
    }
    
  } else {
    data <- mutate(data, !!pid_as_column_name := new_statement_df$qid)
  }
  
  if (verbose == TRUE) {
    cli::cli_h1('add "{new_statement}" statement')
    cli::cli_bullets(
      c(
        "*" = "PID = {new_statement_df$pid}",
        "*" = "QID = {new_statement_df$qid}"
      )
    )
  }
  
  return(data)
}

con <- connect_db("db_clean")
entities <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)


# Get addresses from Factgrid ---------------------------------------------

# there are already some in Factgrid
addresses_in_munich <- 
'
SELECT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P2 wd:Q16200.
  ?item wdt:P47 wd:Q10427.
}
'

addresses_in_munich_in_factgrid <- addresses_in_munich %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+")) %>% 
  left_join(entities %>% select(id, name), by = c("itemLabel" = "name"))


# Write in DB  ------------------------------------------------------------

import <- addresses_in_munich_in_factgrid %>% 
  filter(!is.na(id)) %>% 
  select(id, external_id) %>% 
  mutate(
    entity_id_type = "entities",
    external_id_type = "factgrid", source = "forum", hierarchy = 1)

import

# con <- connect_db("db_clean")
# DBI::dbAppendTable(con, "el_matches", import)
# DBI::dbDisconnect(con); rm(con)



# Get addreses to write in DB ---------------------------------------------

con <- connect_db("db_clean")
factgrid_ids <- tbl(con, "el_matches") %>% 
  filter(external_id_type == "factgrid") %>% 
  select(id, external_id) %>% 
  collect()
DBI::dbDisconnect(con); rm(con)

new_addresses <- entities %>% 
  filter(address == 1) %>% 
  anti_join(factgrid_ids, by = "id") %>% 
  distinct(id, name) %>% 
  mutate(city = munich_label$de) %>% 
  geocode(street = name, city = city, method = "osm", lat = latitude, long = longitude)





api <- new_addresses %>% 
  mutate(
    # Labels
    Lde = paste0(munich_label$de, ", ", name),
    Len = paste0(munich_label$en, ", ", name),
    Lfr = paste0(munich_label$fr, ", ", name),
    Les = paste0(munich_label$es, ", ", name),
    # Description
    Dde = paste0('Straße in ' , munich_label$de),
    Den = paste0('Street in ', munich_label$en),
    Dfr = paste0('Rue en ', munich_label$fr),
    Des = paste0('Calle en ', munich_label$es)) %>% 
  add_statement(statements, "instance_of_address") %>% 
  add_statement(statements, "location_in_munich") %>% 
  add_statement(statements, "research_project") %>% 
  add_statement(statements, "research_area") %>% 
  add_statement(statements, "external_id_forum", qid_from_row = TRUE, col_for_row_content = "id") %>% 
  add_statement(statements, "address_as_string", qid_from_row = TRUE, col_for_row_content = "name") %>% 
  add_statement(statements, "coordinates", qid_from_row = TRUE)


api

# 
# quickstatements <- new_addresses %>% 
#   mutate(
#     # Labels
#     Lde = paste0('"', munich_label$de, ', ', name, '"'),
#     Len = paste0('"', munich_label$en, ', ', name, '"'),
#     Lfr = paste0('"', munich_label$fr, ', ', name, '"'),
#     Les = paste0('"', munich_label$es, ', ', name, '"'),
#     # Description
#     Dde = paste0('"Straße in ' , munich_label$de, '"'),
#     Den = paste0('"Street in ', munich_label$en, '"'),
#     Dfr = paste0('"Rue en ', munich_label$fr, '"'),
#     Des = paste0('"Calle en ', munich_label$es, ''))



# write_wikibase(
#   items = "Q24",
#   properties = "Lde",
#   values = "ein deutsches label",
#   format = "api",
#   api.username = config$api_username,
#   api.token = config$api_token,
#   api.format = "v1",
#   api.batchname = "entities_addresses",
#   api.submit = TRUE,
#   quickstatements.url = config$quickstatements_url
# )

