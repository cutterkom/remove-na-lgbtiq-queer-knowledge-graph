# title: Write LAW entities to Factgrid
# desc: First iteration of a working process for bulk import to Factgrid
# input: lgbtiq_kg_clean.entities
# output: Factgrid items


library(tidyverse)
library(kabrutils)
library(WikidataR) # https://github.com/cutterkom/WikidataR
library(SPARQL)
library(tidygeocoder)
library(googlesheets4)


# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- read_sheet(config$statements$gs_table, sheet = "statements")


# Get data ----------------------------------------------------------------

con <- connect_db("db_clean")

entities_raw <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  filter(external_id_type == "factgrid") %>% 
  #distinct(id, external_id) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)


# Entities not yet in Factgrid --------------------------------------------

new_entities <- anti_join(entities_raw, el_matches, by = "id")

import <- new_entities %>% 
  filter(law == 1) %>% 
  distinct(id, name) %>% 
  mutate(
    # create new item 
    item = paste0("CREATE_", id),
    # Labels
    Lde = name,
    Len = name,
    Lfr = name,
    Les = name,
    # Description
    Dde = paste0('Juristische Entität in Deutschland'),
    Den = paste0('Legal entity in Germany'),
    Dfr = paste0('Entité juridique en Allemagne'),
    Des = paste0('Entidad legal en Alemania')) %>% 
  add_statement(statements, "instance_of_law") %>% 
  add_statement(statements, "external_id_forum", qid_from_row = TRUE, col_for_row_content = "id") %>% 
  select(item, starts_with("L"), starts_with("D"), starts_with("P")) %>% 
  distinct() %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*"))

import_list <- import %>% group_split(item, .keep = TRUE)


# Write to factgrid -------------------------------------------------------

purrr::iwalk(
  .x = import_list,
  .f = function(data, object_name) {
    write_wikibase(
      items = data$item,
      properties = data$property,
      values = data$value,
      format = "api",
      api.username = config$connection$api_username,
      api.token = config$connection$api_token,
      api.format = "v1",
      api.batchname = "entities_party",
      api.submit = T,
      quickstatements.url = config$connection$quickstatements_url
    )
  }
)


# Get data from Factgrid --------------------------------------------------

query <- 
  '
SELECT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P2 wd:Q220580
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))


external_ids_compare <- import %>% 
  filter(property == "Lde") %>% 
  mutate(id = trimws(str_remove(item, "CREATE_"))) %>% 
  distinct(id, name = value) %>% 
  left_join(query_res, by = c("name" = "itemLabel")) %>% 
  left_join(el_matches %>% distinct(id, external_id), by = "id", suffix = c("_new", "_in_db")) %>% 
  # fix minor
  mutate(
    external_id_new = ifelse(id == "chronik_195", "Q401944", external_id_new),
    external_id_new = ifelse(id == "chronik_502", "Q401947", external_id_new))



# Locatisation Deutschland und RemoveNA Kennzeichnungen hinzufügen --------

import <- external_ids_compare %>% 
  distinct(item = external_id_new) %>% 
  add_statement(statements, "location_in_germany_over_time") %>% 
  add_statement(statements, "research_project") %>% 
  add_statement(statements, "research_area") %>% 
  select(item, starts_with("L"), starts_with("D"), starts_with("P")) %>% 
  distinct() %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*"))

import_list <- import %>% group_split(item, .keep = TRUE)

# Write to factgrid -------------------------------------------------------

purrr::iwalk(
  .x = import_list,
  .f = function(data, object_name) {
    write_wikibase(
      items = data$item,
      properties = data$property,
      values = data$value,
      format = "api",
      api.username = config$connection$api_username,
      api.token = config$connection$api_token,
      api.format = "v1",
      api.batchname = "entities_party",
      api.submit = T,
      quickstatements.url = config$connection$quickstatements_url
    )
  }
)
  

# Write Factgrid ID in DB -------------------------------------------------

import <- external_ids_compare %>% 
  select(id, external_id = external_id_new) %>% 
  filter(!is.na(external_id)) %>% 
  mutate(
    entity_id_type = "entities",
    external_id_type = "factgrid", 
    source = "forum", 
    hierarchy = 1)

con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)
