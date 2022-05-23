library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "statements")

tw_enable_cache()
tw_set_cache_folder(path = fs::path(fs::path_home_r(), "R", "tw_data"))
tw_set_language(language = "de")
tw_create_cache_folder(ask = FALSE)

# Get input data ----------------------------------------------------------

con <- connect_db("db_clean")

entities_raw <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  select(-contains("_at"), -entity_id_type, -entity_id_combination, -entity_id_combination_type, external_id_label) %>% 
  collect()

chronik <- tbl(con, "chronik") %>% collect()

DBI::dbDisconnect(con); rm(con)

entities_raw %>% 
  anti_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, external_id), by = "id") %>% View


input <- entities_raw %>% 
  anti_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, external_id), by = "id") %>% 
  filter(country == 1 | district == 1) %>% select(id, name) %>% clipr::write_clip()


munich_districts <- tw_get_wikipedia_page_section_links(
    url = "https://de.wikipedia.org/wiki/Stadtbezirke_M%C3%BCnchens", 
    language = "de", 
    section_title = "Aktuelle Stadtbezirke")

input_statements <- munich_districts %>% 
  mutate(
    item = paste0("CREATE_", row_number()),
    Lde = wikipedia_title,
   Len = wikipedia_title, 
   Les = wikipedia_title,
   Lfr = wikipedia_title,
   Lde = "Stadtbezirk in München",
   Len = "city district in Munich",
   Lfr = "Arrondissement de Munich",
   Les = "Distrito de la ciudad de Múnich",
  Sdewiki = paste0(config$import_helper$single_quote, wikipedia_title, config$import_helper$single_quote),
   Swikidatawiki = paste0(config$import_helper$single_quote, qid, config$import_helper$single_quote)) %>% 
  add_statement(statements, "instance_of_locality") %>% 
  add_statement(statements, "territorial_affiliation_is_munich") %>% 
  rename(P297_2 = P297) %>% 
  add_statement(statements, "territorial_affiliation_is_germany")

# Prepare import ----------------------------------------------------------

import <- input_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*")) %>% 
  filter(!is.na(value)) %>% 
  distinct()

import
# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/munich_districts.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_clubs",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)



query <- '
SELECT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P297 wd:Q10427.
  ?item wdt:P2 wd:Q8.
}'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))


import <- query_res %>% 
  select(item = external_id) %>% 
  # Stadtbezirk in München
  mutate(P2 = "Q405946", P2_2 = "Q164344", P2_3 = "Q12") %>% 
  # teil von 
  mutate(P8 = "Q10427", P319 = "Q10427") %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*")) %>% 
  filter(!is.na(value)) %>% 
  distinct()
  
write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/munich_districts.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_clubs",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# remove



import <- query_res %>% 
  select(item = external_id) %>% 
  mutate(P2 = "Q11214",
         P2_2 = "Q147167",
         P83 = "Q10427") %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*")) %>% 
  filter(!is.na(value)) %>% 
  distinct()


write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/munich_districts.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_clubs",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# Reconcile ---------------------------------------------------------------

project <- "entities_country_district"
ids <- rrefine::refine_export(project) 

import <- ids %>% 
  mutate(
    factgrid = ifelse(id == "chronik_220", "Q405921", factgrid),
    factgrid = ifelse(id == "chronik_399", "Q405952", factgrid),
    factgrid = ifelse(id == "chronik_76", "Q405922", factgrid),
    factgrid = ifelse(id == "chronik_649", "Q405951", factgrid),
    factgrid = ifelse(id == "er_1860", "Q405950", factgrid)
  ) %>% 
  pivot_longer(3:4, names_to = "external_id_type", values_to = "external_id") %>% 
  filter(!is.na(external_id)) %>% 
  mutate(
    entity_id_type = "entities",
    source = "forum", 
    hierarchy = 1) %>% 
  select(-name)


con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)
