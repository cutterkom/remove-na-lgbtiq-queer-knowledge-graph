# add field of engagement to all authors that are not humans

# humans have authorship in career statement

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

# Query data --------------------------------------------------------------

# get all books within RemoveNA and their author + career statement

query <- 
  '
SELECT ?item ?itemLabel ?author WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  ?item wdt:P2 wd:Q394362;
    wdt:P131 wd:Q400012;
    wdt:P21 ?author.
  MINUS { ?author wdt:P2 wd:Q7. }
}
'
query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         book_item = str_extract(item, "Q[0-9]+"),
         author_item = str_extract(author, "Q[0-9]+"))


# Add to field of work when not a human ----------------------------------------------------

import <- query_res %>% 
  add_statement(statements, "field_of_work_author") %>% 
  select(item = author_item, P452) %>% 
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
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)

read_delim(file = paste0("data-publishing/factgrid/data/", csv_file), delim = "\t") %>% clipr::write_clip()
