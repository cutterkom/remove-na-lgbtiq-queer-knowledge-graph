# title: Add Factgrid-IDs to Wikidata
# desc: Factgrid is an external identifier within Wikidata. This script fetches all RemoveNA items with an Wikidata-ID and looks up if there's an Factgrid-ID. If not, I'll add that afterwards.
# input: SPARQL-Query: data-publishing/factgrid/queries/get_factgrid_ids_from_wikidata.rq
# output: #temporary_batch_1655543529265, "add-missing-fg-ids", 1573 added


library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

# only remove na
#query <- readr::read_file("data-publishing/factgrid/queries/get_factgrid_ids_from_wikidata-removena.rq")
# all factgrid
query <- readr::read_file("data-publishing/factgrid/queries/get_factgrid_ids_from_wikidata.rq")

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint)

query_res <- query_res %>% mutate(has_fg_id = ifelse(!is.na(wd_fg_id), 1, 0))

query_res %>% count(has_fg_id)

# result:
# has_fg_id     n
# <dbl> <int>
# 1         0  1573
# 2         1    28


# Prepare import ----------------------------------------------------------

# external IDs must be strings
triple_quote <- '"""'

input_statements <- query_res %>% 
  filter(has_fg_id == 0) %>% 
  mutate(
    wd_item =  str_extract(wd_item, "Q[0-9]+"),
    fg_item =  str_extract(fg_item, "Q[0-9]+")) %>% 
  select(item = wd_item, P8168 = fg_item) %>% 
  mutate(
    across(P8168, ~paste0(triple_quote, .x , triple_quote))
  )

import <- input_statements %>% long_for_quickstatements()

csv_file <- tempfile(fileext = "csv")

write_wikibase( # also write_wikidata possible
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = csv_file
)

# copy it to clipboard
read_delim(file = csv_file, delim = "\t") %>% clipr::write_clip()
