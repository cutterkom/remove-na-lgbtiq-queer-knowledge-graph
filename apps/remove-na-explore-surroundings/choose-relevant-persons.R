# choose personen
# those that do have a wikidata QID

library(tidyverse)
library(kabrutils)
library(SPARQL)

endpoint <- "https://database.factgrid.de/sparql"

query <- 
  '
#defaultView:Table

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

SELECT DISTINCT ?fg_item ?fg_itemLabel ?fg_itemDescription ?wd_item ?Sdewiki ?Senwiki ?Sfrwiki ?Seswiki where {

    # labels from Factgrid
    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }  
    
    # get all from Remove NA
   ?fg_item fgt:P131 fg:Q400012 .
   ?fg_item fgt:P2 fg:Q7.

  OPTIONAL {
  # transform wikidata qid in factgrid to wikidata entity iri
  ?link schema:about ?fg_item .
   ?link schema:isPartOf <https://www.wikidata.org/> . 
   ?link schema:name ?qid.
   BIND(IRI(CONCAT(STR(wd:), ?qid)) AS ?wd_item)
  }
  
  OPTIONAL {
  # transform wikidata qid in factgrid to wikidata entity iri
   ?link_de schema:about ?fg_item .
   ?link_de schema:isPartOf <https://de.wikipedia.org/> . 
   ?link_de schema:name ?Sdewiki.
    }
  OPTIONAL {

          # transform wikidata qid in factgrid to wikidata entity iri
   ?link_en schema:about ?fg_item .
   ?link_en schema:isPartOf <https://en.wikipedia.org/> . 
   ?link_en schema:name ?Senwiki.
    }
  OPTIONAL {
  ?link_es schema:about ?fg_item .
  ?link_es schema:isPartOf <https://es.wikipedia.org/> . 
  ?link_es schema:name ?Seswiki.
  }
   OPTIONAL {
   # transform wikidata qid in factgrid to wikidata entity iri
   ?link_fr schema:about ?fg_item .
   ?link_fr schema:isPartOf <https://fr.wikipedia.org/> . 
   ?link_fr schema:name ?Sfrwiki.
    }
  

}
'


persons_raw <- query %>% 
  sparql_to_tibble(endpoint = endpoint) %>% 
  mutate(across(fg_item, ~extract_id(.))) %>% 
  mutate(across(c(fg_itemLabel, fg_itemDescription, Senwiki, Sdewiki, Seswiki, Sfrwiki), ~remove_lang(.))) %>% 
  rename(item = fg_item, label = fg_itemLabel, desc = fg_itemDescription)

persons <- persons_raw %>% 
  filter(!is.na(wd_item)) %>% 
  anti_join(
    persons_raw %>% filter(is.na(Sdewiki), is.na(Senwiki), is.na(Seswiki), is.na(Sfrwiki))
  ) %>% 
  mutate(
    dbpedia = ifelse(!is.na(Senwiki), paste0("https://dbpedia.org/page/", Senwiki), NA),
    dbpedia = str_replace(dbpedia, " ", "_"),
    Sdewiki = ifelse(!is.na(Sdewiki), paste0("https://de.wikipedia.org/wiki/", Sdewiki), NA),
    Senwiki = ifelse(!is.na(Senwiki), paste0("https://en.wikipedia.org/wiki/", Senwiki), NA),
    Sfrwiki = ifelse(!is.na(Sfrwiki), paste0("https://fr.wikipedia.org/wiki/", Sfrwiki), NA),
    Seswiki = ifelse(!is.na(Seswiki), paste0("https://es.wikipedia.org/wiki/", Seswiki), NA),
    Swikidatawiki = str_remove_all(wd_item, "<|>")
  ) %>% 
  filter(label != "Anonymous") %>% 
  filter(!is.na(Senwiki))

saveRDS(persons, file = "apps/remove-na-explore-surroundings/data/persons.Rds")

# Check who has connections -----------------------------------------------

# only those are relevant who have at least one connection
# need to run query for all persons and count connections

source("apps/remove-na-explore-surroundings/queries.R")

check_count_of_companions <- persons %>% 
  head() %>% 
  #filter(item == "Q226350") %>% 
  select(item) %>% 
  pmap_df(function(...) {
    current <- tibble(...)
    print(current$item)
    #query <- query_count(fg_item = current$item) 
    res <- kabrutils::sparql_to_tibble(query_count(fg_item = current$item), endpoint = endpoint)
    
    print(res)
    # nrow_res <<- nrow(res)-1
    # # there is always at least 1 result -> if no result, than -1 
    # if (nrow_res > 0) {
    #   
    #   tmp <- tibble(
    #     item = current$item,
    #     has_res = 1,
    #     n = nrow_res
    #   )
    # } else {
    #   tmp <- tibble(
    #     item = current$item,
    #     has_res = 0,
    #     n = 0
    #   )
    # }
    # cli::cli_alert_success("data fetched")

   res <- res %>% mutate(fg_item = current$item)
  })



kabrutils::sparql_to_tibble(query_companions(fg_item = "Q402214"), endpoint = endpoint) %>% View
