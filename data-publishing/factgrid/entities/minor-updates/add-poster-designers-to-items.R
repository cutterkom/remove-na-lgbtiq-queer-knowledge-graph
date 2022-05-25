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
SELECT ?item ?itemLabel ?author WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  ?item wdt:P2 wd:Q408517.
  OPTIONAL { ?item wdt:P21 ?author. }
}
'
query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         poster_item = str_extract(item, "Q[0-9]+"),
         author_item = str_extract(author, "Q[0-9]+"))

import <- query_res %>% 
  add_statement(statements, "field_of_work_poster_designer") %>% 
  select(item = author_item, P452) %>% 
  long_for_quickstatements()
import

csv_file <- "data-publishing/factgrid/data/add-book-class.csv"

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
