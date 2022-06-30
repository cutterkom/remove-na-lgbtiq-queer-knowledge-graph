# title: Add Wikidata-IDs to Factgrid
# desc: All items I create in Wikidata will get an QID that need to be added to Factgrid afterwards. This script takes care of that. It looks for Factgrid Items that don't have a Wikidata ID, but can be found in Wikidata with its Factgrid ID.
# input: SPARQL-Query: data-publishing/factgrid/queries/get_factgrid_ids_from_wikidata.rq
# output: 


library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

query <- readr::read_file("data-publishing/factgrid/queries/get_factgrid_ids_from_wikidata.rq")

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(across(contains("item"), ~extract_id(.))) 


import <- query_res %>% 
  select(item = fg_item, Swikidatawiki = wd_item) %>% 
  mutate(
    across(Swikidatawiki, ~paste0(config$import_helper$single_quote, .x , config$import_helper$single_quote))
  ) %>% 
  long_for_quickstatements()
  
import

csv_file <- tempfile(fileext = "csv")

write_wikibase( # also write_wikidata possible
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = csv_file
)

# copy it to clipboard
fs::file_show(csv_file)
