library(tidyverse)
library(kabrutils)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

query <- '
SELECT DISTINCT ?item ?itemLabel ?statementcount
WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  ?item wdt:P131 wd:Q400012 .
  ?item wikibase:statements ?statementcount.
}'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint)



# Number of items ---------------------------------------------------------

nr_of_items <- length(unique(query_res$item))

# Number of relations -----------------------------------------------------

nr_of_relations <- sum(query_res$statementcount)

# minus 4098 für "Werke"
redundant_works <- 4098
sum(query_res$statementcount)-redundant_works
# minus 3 meta data per item
netto_relations <- nr_of_relations - (nr_of_items*3) - redundant_works
netto_relations

