# Data model Publications https://database.factgrid.de/wiki/FactGrid:Print_publications_data_model


library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)

# Get input data ----------------------------------------------------------

con <- connect_db("db_clean")

el_matches <- tbl(con, "el_matches") %>% 
  select(-contains("_at"), -entity_id_type, -entity_id_combination, -entity_id_combination_type, external_id_label) %>% 
  collect()

books_raw <- tbl(con, "books_wide") %>% collect() %>% 
  distinct(id = book_id, title) %>% 
  mutate(id = paste0("book_", id)) %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, item = external_id), by = "id")

DBI::dbDisconnect(con); rm(con)


import <- books_raw %>% mutate(P2 = "Q394362") %>% long_for_quickstatements(start_at = 3)

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/add-book-class.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)



# add works to entities that have published work --------------------------


query <- 
  '
# get posters and books with their corresponding authors
SELECT ?item ?itemLabel ?author ?authorLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P21 ?author;
    wdt:P131 wd:Q400012.
}
ORDER BY (?itemLabel)
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         book_item = str_extract(item, "Q[0-9]+"),
         author_item = str_extract(author, "Q[0-9]+"))

import <- query_res %>% 
  select(item = author_item, P174 = book_item) %>% 
  long_for_quickstatements()

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/add-works-to-authors.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)



# Build books URL ---------------------------------------------------------
con <- connect_db("db_clean")

books_raw <- tbl(con, "books_wide") %>% collect() %>% 
  select(-id) %>% 
  rename(id = book_id)


books_url <- books_raw %>% 
  mutate(url = stringi::stri_trans_general(title, "de-ASCII; Latin-ASCII"),
         url = str_remove_all(url, "\\?|\\(|\\)|!|\\[|'|\\]"),
         url = str_remove_all(url, '"'),
         url = tolower(url),
         url = str_replace_all(url, "\\s\\s|\\s\\s\\s", " "),
         url = str_replace_all(url, ",", "-"),
         url = str_replace_all(url, "--", "-"),
         url = str_replace_all(url, "\\s|\\.", "-"),
         url = str_replace_all(url, "--|---|----", "-"),
         url = str_replace_all(url, "--", "-"),
         url = paste0("https://archiv.forummuenchen.org/objekt/", url),
         url = ifelse(is.na(title), NA_character_, url)) %>% 
  distinct(id, url) %>% 
  left_join( books_url %>% count(url, sort = T)) %>% 
  group_by(url) %>%
  mutate(rowid = ifelse(n>1, as.character(row_number()), "")) %>% 
  mutate(P156 = ifelse(rowid>1, paste0(url, "-", rowid), url)) %>% 
  ungroup()


import <- books_url  %>% 
  mutate(id = paste0("book_", id)) %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, item = external_id), by = "id") %>% 
  select(id, item, P156) %>% 
  long_for_quickstatements(start_at = 3)

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/book-urls.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)



