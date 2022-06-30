library(tidyverse)
library(kabrutils)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "wikidata")


# Get data ----------------------------------------------------------------

query <- 
  '#defaultView:Table

# Prefix standard 
# Factgrid
PREFIX fg: <https://database.factgrid.de/entity/>
PREFIX fgt: <https://database.factgrid.de/prop/direct/>
# DBpedia
PREFIX dbo: <http://dbpedia.org/ontology/> 
PREFIX dbr: <http://dbpedia.org/resource/> 
# Wikidata
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
# misc
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX dct:  <http://purl.org/dc/terms/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX schema: <http://schema.org/>
prefix foaf:  <http://xmlns.com/foaf/0.1/> 

SELECT DISTINCT ?fg_item ?fg_itemLabel ?fg_itemAltLabel ?wd_item ?wd_fg_id where {

    # labels from Factgrid
    SERVICE wikibase:label { bd:serviceParam wikibase:language "de,[AUTO_LANGUAGE],en". }  
    
    ?fg_item fgt:P131 fg:Q400012 .

    # transform wikidata qid in factgrid to wikidata entity iri
    ?link schema:about ?fg_item .
    ?link schema:isPartOf <https://www.wikidata.org/> . 
    ?link schema:name ?qid.
    BIND(IRI(CONCAT(STR(wd:), ?qid)) AS ?wd_item)

} 
'
query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint)

with_alias <- query_res %>% 
  filter(!is.na(fg_itemAltLabel)) %>% 
  mutate(across(c(fg_item, starts_with("wd_")), ~extract_id(.))) %>% 
  mutate(across(c(fg_itemLabel, fg_itemAltLabel), ~remove_lang(.))) %>% 
  separate_rows(fg_itemAltLabel, sep = ", ") %>% 
  mutate(fg_itemAltLabel = trimws(fg_itemAltLabel)) %>% 
  mutate(
    Ade = fg_itemAltLabel,
    Aen = Ade,
    Afr = Ade,
    Aes = Ade,
    Abar = Ade) %>% 
  select(item = wd_item, starts_with("A")) %>% 
  filter(item != "Q112776288")

import <- with_alias %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^A", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  arrange(item) %>% 
  long_for_quickstatements()

csv_file <- tempfile(fileext = "csv")

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = csv_file
)

fs::file_show(csv_file)
