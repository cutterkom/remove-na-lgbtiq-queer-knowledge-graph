# add a class of poster designers to poster authors

# P452 -> Q402093

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


# Query data: Posters with authors ----------------------------------------

query <- 
  '
SELECT ?item ?author ?instanceLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P2 wd:Q408517.
  OPTIONAL { ?item wdt:P21 ?author. }
  OPTIONAL { ?author wdt:P2 ?instance. }
}

'

query_res_raw <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint)

query_res_raw

query_res <- query_res_raw %>% 
  mutate(instanceLabel = str_remove_all(instanceLabel, '\"|\"|, München|@de|München, '),
         poster_item = str_extract(item, "Q[0-9]+"),
         author_item = str_extract(author, "Q[0-9]+"))

import <- query_res %>% 
  filter(instanceLabel != "Mensch") %>% 
  add_statement(statements, "field_of_work_poster_designer") %>% 
  select(item = author_item, P452) %>% 
  long_for_quickstatements()
import

csv_file <- "data-publishing/factgrid/data/add-poster-designer.csv"

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = csv_file,
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)

read_delim(file = csv_file, delim = "\t") %>% clipr::write_clip()


# Add as career statement when a human ------------------------------------

import <- query_res %>% 
  filter(instanceLabel == "Mensch") %>% 
  add_statement(statements, "career_is_poster_creator") %>% 
  select(item = author_item, matches("^P", ignore.case = FALSE)) %>% 
  long_for_quickstatements()
import

csv_file <- "tmp.csv"

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = paste0("data-publishing/factgrid/data/", csv_file),
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)

read_delim(file = paste0("data-publishing/factgrid/data/", csv_file), delim = "\t") %>% clipr::write_clip()


# Remove field of work poster designer if human ---------------------------

import <- query_res %>% 
  filter(instanceLabel == "Mensch") %>% 
  ### !here
  add_statement(statements, "field_of_work_poster_designer") %>% 
  select(item = author_item, matches("^P", ignore.case = FALSE)) %>% 
  long_for_quickstatements()
import

csv_file <- "tmp.csv"

write_wikibase(
  remove = TRUE, 
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = paste0("data-publishing/factgrid/data/", csv_file),
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)

read_delim(file = paste0("data-publishing/factgrid/data/", csv_file), delim = "\t") %>% clipr::write_clip()
