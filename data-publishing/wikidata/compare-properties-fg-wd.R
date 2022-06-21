library(tidyverse)
library(glue)
library(SPARQL)
library(kabrutils)



# Some config -------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)

# get all Factgrid properties with corresponding wikidata properti --------

properties <- readr::read_file("data-publishing/factgrid/queries/get_all_properties_with_corressponding_prop.rq") %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(
    fg_property_id = str_extract(fg_property, "P[0-9]+"),
    wd_property_id = str_extract(wd_property, "P[0-9]+"))

# remove instance of property: too much data, will result in timeout

fg_properties <- properties %>% 
  filter(fg_property_id != "P2") %>% 
  distinct(fg_property_id) %>% 
  arrange(fg_property_id)

# compare factgrid propertyvalues with wikidata ---------------------------

compare <- fg_properties %>% 
  #head(5) %>% 
  pmap_dfr(function(...) {
    current <- tibble(...)
    fg_property_id <- current$fg_property_id
    print(fg_property_id)
    
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
      
      SELECT DISTINCT ?fg_item ?fg_itemLabel ?wd_item ?fg_property ?fg_property_as_itemLabel ?wd_property ?fg_value ?wd_value_from_fg ?wd_value_from_wd ?is_same where {
      
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
