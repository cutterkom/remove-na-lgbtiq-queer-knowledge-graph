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
  filter(city == 1) %>% select(id, name) %>% clipr::write_clip()


# Reconcile ---------------------------------------------------------------

project <- "entites_city"
ids <- rrefine::refine_export(project) 


# Save in DB --------------------------------------------------------------

import <- ids %>% 
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