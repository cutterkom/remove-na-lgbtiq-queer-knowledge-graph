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

add_statement <- function(data, available_statements = statements, new_statement, verbose = TRUE) {
  
  new_statement_df <- filter(available_statements, statement == new_statement)
  
  pid_as_column_name <- rlang::sym(new_statement_df$pid)
  data <- mutate(data, !!pid_as_column_name := new_statement_df$qid)
  
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
    Des = paste0('Calle en ', munich_label$es),
    P48 =
      case_when(
        !is.na(latitude) ~ paste0("@", latitude, "/", longitude),
        TRUE ~ NA_character_
      )
  ) %>%
  add_statement(statements, "instance_of_address") %>% 
  add_statement(statements, "location_in_munich") %>% 
  add_statement(statements, "research_area") %>% 
  add_statement(statements, "research_project")
  
  
  add_statement(qid = statements$research_area$qid, pid = statements$research_area$pid) %>% 
  add_statement(qid = statements$research_project$qid, pid = statements$research_project$pid) %>%
  add_statement(qid = statements$instance_of_address$qid, pid = statements$instance_of_address$pid) %>% add_statement(qid = statements$location_in_munich$qid, pid = statements$location_in_munich$pid)
   



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

