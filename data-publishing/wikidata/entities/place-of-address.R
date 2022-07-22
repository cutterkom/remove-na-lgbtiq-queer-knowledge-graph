library(tidyverse)
library(kabrutils)
library(SPARQL)
source("apps/compare-factgrid-wikidata/functions.R")
source("apps/compare-factgrid-wikidata/queries.R")

input_item_filter_property <- "P131"
input_item_filter_value <- "Q400012"

input_properties_vec <- "P83_P551"
all_properties <- get_properties()
input_properties <- get_input_properties(all_properties, input_properties_vec)
comparison_raw <- get_comparison(input_properties_vec, input_item_filter_property, input_item_filter_value)


# Get data ----------------------------------------------------------------

in_both <- comparison_raw %>% 
  filter(is_same == "true") %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, fg_value, fg_valueLabel, wd_value_from_fg, wd_value_from_wd, meta_property_id) %>% 
  # add property info
  left_join(input_properties %>% select(fg_property_id, fg_propertyLabel, meta_property_id, fg_property, wd_property), by = c("meta_property_id", "fg_property_id"))

in_both

only_in_wd <- comparison_raw %>% 
  filter(is_same == "false") %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, wd_property_id, wd_value_from_wd, wd_value_from_wdLabel, fg_value_from_wd, meta_property_id) %>% 
  # remove those that are present in wd
  anti_join(in_both, by = c("fg_item", "wd_value_from_wd")) %>%
  # add property info
  left_join(input_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property, wd_property_id, meta_property_id), by = c("meta_property_id"))

only_in_wd


only_in_fg <- comparison_raw %>% 
  filter(is.na(is_same), !is.na(fg_item)) %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, fg_value, fg_valueLabel, wd_value_from_fg, wd_value_from_wd, meta_property_id) %>% 
  # remove those that are present in wd
  anti_join(in_both, by = c("fg_item", "wd_value_from_fg")) %>% 
  # add property info
  left_join(input_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property, meta_property_id), by =  c("meta_property_id", "fg_property_id"))

only_in_fg %>% glimpse()



# Get instances -----------------------------------------------------------

query_instances <- function(fg_item = NULL) {
  paste0('
   # Prefix standard
    # Factgrid
    PREFIX fg: <https://database.factgrid.de/entity/>
    PREFIX fgt: <https://database.factgrid.de/prop/direct/>
    
    # Wikidata
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    # misc
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX bd: <http://www.bigdata.com/rdf#>
    PREFIX schema: <http://schema.org/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    
    SELECT ?fg_item ?fg_itemLabel ?instance ?instanceLabel WHERE {
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
      BIND(fg:', fg_item, ' AS ?fg_item)
      ?fg_item fgt:P2 ?instance}
  ')
 
}

only_in_fg_with_instances <- only_in_fg %>% 
  mutate(across(c(fg_item), ~ extract_id(.))) %>% 
  select(fg_item) %>% 
  pmap_dfr(function(...) {
    current <- tibble(...)
    query <- query_instances(fg_item = current$fg_item)
    res <- sparql_to_tibble(query, endpoint = endpoint)
  })


only_in_fg_with_instances_per_instance <- only_in_fg_with_instances %>% 
  left_join(only_in_fg, by = c("fg_item", "fg_itemLabel")) %>% #glimpse()
  select(fg_item, wd_item, instance, instanceLabel, wd_property, wd_value_from_fg) %>% 
  mutate(across(c(fg_item, wd_item, wd_value_from_fg), ~ extract_id(.))) %>% 
  mutate(across(c(wd_property), ~ extract_id(., pattern = "P[0-9]+"))) %>% 
  mutate(wd_property = ifelse(str_detect(instanceLabel, "Human"), wd_property, "P131"))
  
import <- only_in_fg_with_instances_per_instance %>% 
  select(wd_item, wd_property, wd_value_from_fg, fg_item) %>% 
  mutate(source = "S8168", 
         source_value = paste0('"', fg_item, '"'),
         time = "S813", 
         timestamp = "+2022-07-18T00:00:00Z/11") %>% 
  select(-fg_item) %>% 
  distinct()


write.table(import, "apps/compare-factgrid-wikidata/data/address.csv", row.names = FALSE, quote = FALSE, sep = "\t")


# add state 
import <- import %>% 
  filter(wd_property == "P131") %>% 
  mutate(wd_property = "P17") %>% 
  mutate(
    wd_value_from_fg = case_when(
    wd_item == "Q56777493" ~ "Q38",
    	
    wd_item == "Q112813203" ~ "Q39562060",
    	
    wd_item == "Q112813293" ~ "Q40",
    	
    wd_item == "Q5274363" ~ "Q41304",
    	
    wd_item == "Q18130381" ~ "Q41304",
    wd_item == "Q108828808" ~ "Q16",
    wd_item == "Q2934320" ~ "Q142",
    TRUE ~ "Q183")) 

write.table(import, "apps/compare-factgrid-wikidata/data/states.csv", row.names = FALSE, quote = FALSE, sep = "\t")
