# title: add missing wikipedia articles
# desc: a federated query looks up all wikipedia articles of factgrid items in en, de, fr, es and adds them to factgrid
# input: 
# output: 


library(tidyverse)
library(kabrutils)
library(WikidataR)
library(SPARQL)
library(tidywikidatar)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

query <- readr::read_file("data-publishing/factgrid/queries/get_wiki_sitelinks_removena.rq")
query <- readr::read_file("data-publishing/factgrid/queries/get_wiki_sitelinks.rq")

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint)



# Find missing Wikidata Sitelinks -----------------------------------------

# if there is no wikidata qid, but a sitelink, then I can fetch the missing Wikidata QID

no_wd_id_but_sitelink <- query_res %>% 
  filter(is.na(wd_item) & (!is.na(Sdewiki) | !is.na(Senwiki) | !is.na(Sfrwiki) | !is.na(Seswiki))) %>% 
  # clean strings for fetching QIDs
  mutate(across(starts_with("S"), ~str_remove_all(., '"|@.*')))
  
no_wd_id_but_sitelink

wikidata_qids <- no_wd_id_but_sitelink %>% 
  pmap_df(function(...) {
    current <- tibble(...)
    
    if(!is.na(current$Sdewiki)) {
      tmp <- tw_get_wikipedia_page_qid(title = current$Sdewiki, language = "de")
    } else if (!is.na(current$Senwiki)) {
      tmp <- tw_get_wikipedia_page_qid(title = current$Senwiki, language = "en")
    } else if (!is.na(current$Sfrwiki)) {
      tmp <- tw_get_wikipedia_page_qid(title = current$Sfrwiki, language = "fr")
    } else if (!is.na(current$Seswiki)) {
      tmp <- tw_get_wikipedia_page_qid(title = current$Seswiki, language = "es")
    } else {
      stop("language not implemented")
    }
    tmp %>% mutate(fg_item = current$fg_item) %>% select(fg_item, Swikidatawiki = qid)
  })


# Import ------------------------------------------------------------------

import <- wikidata_qids %>%
  mutate(
    item = str_extract(fg_item, "Q[0-9]+"),
    Swikidatawiki = paste0(config$import_helper$single_quote, Swikidatawiki, config$import_helper$single_quote)) %>% 
  select(item, starts_with("S")) %>% 
  long_for_quickstatements()

import

csv_file <- tempfile(fileext = "csv")

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = csv_file
)

fs::file_show(csv_file)

