library(tidyverse)
library(kabrutils)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

path <- "analysis/network/data/"
file <- "all_neighbors.Rdata"

# Get data ----------------------------------------------------------------

all_items <- '
SELECT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  ?item wdt:P131 wd:Q400012.
}
' %>% sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(across(c(item, starts_with("wd_")), ~extract_id(.)))


# Get 2 next nodes of all statements of an item ---------------------------

get_neighbors <- function(item) {
  paste0('PREFIX fg: <https://database.factgrid.de/entity/>
PREFIX fgt: <https://database.factgrid.de/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>

SELECT ?root ?rootLabel ?property1 ?property1Label ?item1 ?item1Label ?property2 ?property2Label ?item2 ?item2Label WHERE {
  BIND(fg:', item, ' AS ?root)
  ?root ?fgt1 ?item1.
  ?item1 ?fgt2 ?item2.
  ?property1 wikibase:directClaim ?fgt1.
  ?property2 wikibase:directClaim ?fgt2.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}') %>% 
    sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
    mutate(across(c(root, item1, item2), ~extract_id(.))) %>% 
    mutate(across(c(property1, property2), ~extract_id(., "P[0-9]+")))
           
}

all_neighbors <- all_items %>%
  pmap_dfr(function(...) {
    
    current <- tibble(...)
    cli::cli_alert_info("Fetch data for: {current$itemLabel}")
    file_name <- paste0("analysis/network/data/", current$item, ".csv")
    
    if (!file.exists(file_name)) {
      tmp <- get_neighbors(item = current$item)
      write.csv(tmp, file_name)
      tmp
    } else {
      tmp <- tibble()
      cli::cli_alert_info("Already saved, next one")
    }
    tmp
  })




# Read files --------------------------------------------------------------
csv_files <- fs::dir_ls(path, regexp = "\\.csv$")
csv_files %>% head()


all_neighbors_from_csv <- csv_files %>% map_dfr(read_csv)


all_neighbors_from_csv <- all_neighbors_from_csv %>% select(-`...1`)
#all_neighbors_from_csv %>% View

saveRDS(all_neighbors_from_csv, paste0(path, "all_neighbors_data.Rds"))
