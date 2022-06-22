library(tidyverse)
library(glue)
library(SPARQL)
library(kabrutils)
library(cli)
library(tidywikidatar)


# Some config -------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

# get all Factgrid properties with corresponding wikidata properti --------

properties <- readr::read_file("data-publishing/factgrid/queries/get_all_properties_person_with_corressponding_prop.rq") %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(
    fg_property_id = str_extract(fg_property, "P[0-9]+"),
    wd_property_id = str_extract(wd_property, "P[0-9]+"))
# 
# # remove instance of property: too much data, will result in timeout
# 
# fg_properties <- properties %>% 
#   filter(!fg_property_id %in% c("P2", "P154", "P165", "P168", "P297")) %>% 
#   distinct(fg_property_id) %>% 
#   arrange(fg_property_id)

# compare factgrid propertyvalues with wikidata ---------------------------

compare <- properties %>% 
  pmap_dfr(function(...) {
    current <- tibble(...)
    cli::cli_alert_info("Fetch data for: {current$fg_propertyLabel}")
    fg_property_id <- current$fg_property_id
    
    query <- paste0( '
      #defaultView:Table
      
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
      
      SELECT DISTINCT ?fg_item ?fg_itemLabel ?wd_item ?fg_property ?fg_propertyLabel ?wd_property ?fg_value ?wd_value_from_fg ?wd_value_from_wd ?is_same where {
      
        # labels from Factgrid
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }  
      
        # starting points
        # which property in focus?
        # todo in one var
        BIND(fgt:', fg_property_id, ' as ?fg_property)
        BIND(fg:', fg_property_id, ' as ?fg_property_as_item)
        # set property of corresponding wikidata property; is constant
        BIND(fgt:P343 as ?fg_corr_wd)
      
        # transform wikidata qid in factgrid to wikidata entity iri
        ?link schema:about ?fg_item .
        ?link schema:isPartOf <https://www.wikidata.org/> . 
        ?link schema:name ?qid.
        BIND(IRI(CONCAT(STR(wd:), ?qid)) AS ?wd_item)
      
        # get corresponding wikidata property
        # and store as IRI
        ?fg_property_as_item ?fg_corr_wd ?fg_corr_wd_value .
        BIND(IRI(CONCAT(STR(wdt:), ?fg_corr_wd_value)) AS ?wd_property)
      
        # get factgrid statement
        ?fg_item ?fg_property ?fg_value .
        # get wikidata qid of fg_value
        # used later for comparison
        ?link_value schema:about ?fg_value .
        ?link_value schema:isPartOf <https://www.wikidata.org/> . 
        ?link_value schema:name ?qid_value.
        BIND(IRI(CONCAT(STR(wd:), ?qid_value)) AS ?wd_value_from_fg)
      
        # now change to wikidata as data source
        # get value from wikidata
        SERVICE <https://query.wikidata.org/sparql> {
            OPTIONAL {
                ?wd_item ?wd_property ?wd_value_from_wd.
         }
        }
        BIND (IF(?wd_value_from_fg = ?wd_value_from_wd, "true", "false") AS ?is_same)
      }
      
      ')
  
    sparql_to_tibble(query, endpoint = config$connection$sparql_endpoint)
  })


compare <- compare %>% rename(fg_propertyLabel = fg_property_as_itemLabel)



# Helper ------------------------------------------------------------------

show_sample <- function(data) {
  tmp <- data %>% 
    sample_n(1)
  
  cli::cli_alert_success(tmp %>% pull(fg_propertyLabel))
  
  tmp %>% 
    pull(fg_item) %>% 
    str_remove_all(., "<|>") %>% 
    browseURL(.)
}

# Get Wikidata Labels -----------------------------------------------------

# takes a while

wd_labels <- bind_rows(
  compare %>% select(qid_long = wd_value_from_fg),
  compare %>% select(qid_long = wd_value_from_wd)) %>% 
  mutate(qid = str_extract(qid_long, "Q[0-9]+")) %>% 
  distinct() %>% 
  mutate(wd_label = tw_get_label(id = qid, language = "de"))


# Interpretation ----------------------------------------------------------

# column is_same: 
# true: same QID
# false: different QID -> Wikidata has more/different info
# empty: Factgrid has more/different info

# ist aber nicht netto!
compare %>% count(is_same)


# Wenn empty, dann kann man davon ausgehen, dass Wikidata Item ja schon besteht und man deshalb die Info einfach übertragen kann


# Get distinct buckets ----------------------------------------------------

in_both <- compare %>% 
  filter(is_same == "true") %>% 
  distinct(fg_item, fg_itemLabel, fg_propertyLabel, wd_value_from_fg, wd_value_from_wd)

more_in_fg <- compare %>% 
  filter(is.na(is_same)) %>% 
  distinct(fg_item, fg_itemLabel, fg_propertyLabel, wd_value_from_fg) %>% 
  # remove those that are present in wd
  anti_join(in_both, by = c("fg_item", "wd_value_from_fg")) 

more_in_wd <- compare %>% 
  filter(is_same == "false") %>% 
  distinct(fg_item, fg_itemLabel, fg_propertyLabel, wd_value_from_wd) %>% 
  # remove if in both sources
  anti_join(in_both, by = c("fg_item", "wd_value_from_wd")) 


nrow(more_in_fg)
nrow(more_in_wd)
nrow(in_both)



more_in_fg %>% show_sample()






# muss entfernt werden in anderen df's
more_in_wd %>% anti_join(is_in_wd_and_fg, by = c("fg_item", "wd_value_from_wd")) %>% View


removena <- 'SELECT ?item ?itemLabel ?Ist_ein_e_ WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],de". }
  ?item wdt:P131 wd:Q400012.
}' %>% sparql_to_tibble(., endpoint = config$connection$sparql_endpoint)


inner_join(compare, removena %>% distinct(item), by = c("fg_item" = "item")) %>% View


compare_distinct <- compare %>% 
  dplyr::mutate(id_1_id_2 = purrr::map2_chr(wd_value_from_fg, wd_value_from_wd, collapse_to_distinct_rows)) %>% 
  filter(str_detect(fg_itemLabel, "Erika Mann"), str_detect(fg_propertyLabel, "Sibl")) %>% 
  arrange(desc(is_same))
  #filter(is_same == "false")# %>% 
  
compare_distinct %>% select(wd_value_from_fg, wd_value_from_wd, is_same)

compare_distinct %>% dplyr::distinct(id_1_id_2, .keep_all = TRUE) %>%
  dplyr::select(-id_1_id_2) %>% 
  select(wd_value_from_fg, wd_value_from_wd, is_same)



more_in_fg



