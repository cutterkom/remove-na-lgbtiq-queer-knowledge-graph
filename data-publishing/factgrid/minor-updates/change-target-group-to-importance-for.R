# change target group at events to "of importance for"

# https://database.factgrid.de/wiki/Property:P753

library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

# Query data --------------------------------------------------------------

# get all events with target groups

query <- 
  '
#defaultView:Table
SELECT ?item ?itemLabel ?value ?valueLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  ?item wdt:P131 wd:Q400012 .
  {?item  (wdt:P2/wdt:P3*) wd:Q9 .}
  UNION {?item  (wdt:P2/wdt:P3*) wd:Q40514.}
  ?item wdt:P573 ?value 
}
'
query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(item = str_extract(item, "Q[0-9]+"),
         value = str_extract(value, "Q[0-9]+"))


# Add to field of work when not a human ----------------------------------------------------

import <- query_res %>% 
  select(item, P753 = value) %>% 
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
  api.submit = T,
  quickstatements.url = config$connection$quickstatements_url
)

read_delim(file = paste0("data-publishing/factgrid/data/", csv_file), delim = "\t") %>% clipr::write_clip()



# Remove target group  ----------------------------------------------------

import <- query_res %>% 
  select(item, P573 = value) %>% 
  long_for_quickstatements()
import

csv_file <- "tmp.csv"

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  remove = TRUE,
  format = "csv",
  format.csv.file = paste0("data-publishing/factgrid/data/", csv_file),
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.submit = T,
  quickstatements.url = config$connection$quickstatements_url
)

read_delim(file = paste0("data-publishing/factgrid/data/", csv_file), delim = "\t") %>% clipr::write_clip()
