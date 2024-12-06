
# SPARQL query to get items of property type WikibaseItem -----------------

# Federated query, starting at Factgrid

query_items <- function(input_items_filter = NULL, fg_property_id = NULL) {
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
    
    SELECT DISTINCT ?fg_item ?fg_itemLabel ?wd_item ?fg_property ?fg_propertyLabel ?fg_property_type ?wd_property ?fg_value ?fg_valueLabel ?wd_value_from_fg ?wd_value_from_wd ?wd_value_from_wdLabel ?fg_value_from_wd ?is_same where {
    
      # labels from Factgrid
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    
    ', input_items_filter, "
    
      # which property in focus?
      BIND(fgt:", fg_property_id, " as ?fg_property)
      BIND(fg:", fg_property_id, ' as ?fg_property_as_item)
      # set property of corresponding wikidata property; is constant
      BIND(fgt:P343 as ?fg_corr_wd)
    
      ?fg_property_as_item wikibase:propertyType ?fg_property_type .
    
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
      #OPTIONAL {
        ?link_value schema:about ?fg_value .
        ?link_value schema:isPartOf <https://www.wikidata.org/> .
        ?link_value schema:name ?qid_value.
        BIND(IRI(CONCAT(STR(wd:), ?qid_value)) AS ?wd_value_from_fg)
      #}
      # now change to wikidata as data source
      # get value from wikidata
      SERVICE <https://query.wikidata.org/sparql> {
          OPTIONAL {
              ?wd_item ?wd_property ?wd_value_from_wd.
            OPTIONAL {
              ?wd_value_from_wd wdt:P8168 ?fg_value_from_wd.
            }
          }
    
      }
      BIND (IF(?wd_value_from_fg = ?wd_value_from_wd, "true", "false") AS ?is_same)
    }
  ')
}

query_non_items <- function(input_items_filter = NULL, fg_property_id = NULL) {
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
    
    SELECT DISTINCT ?fg_item ?fg_itemLabel ?wd_item ?fg_property ?fg_propertyLabel ?fg_property_type ?wd_property ?fg_value ?fg_valueLabel ?wd_value_from_fg ?wd_value_from_wd ?wd_value_from_wdLabel ?fg_value_from_wd ?is_same where {
    
      # labels from Factgrid
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    
    ', input_items_filter, "
    
      # which property in focus?
      BIND(fgt:", fg_property_id, " as ?fg_property)
      BIND(fg:", fg_property_id, ' as ?fg_property_as_item)
      # set property of corresponding wikidata property; is constant
      BIND(fgt:P343 as ?fg_corr_wd)
    
      ?fg_property_as_item wikibase:propertyType ?fg_property_type .
    
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
    
      # now change to wikidata as data source
      # get value from wikidata
      SERVICE <https://query.wikidata.org/sparql> {
         SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
          OPTIONAL {
              ?wd_item ?wd_property ?wd_value_from_wd.
              # ?wd_item rdfs:label ?wd_itemLabel.
            OPTIONAL {
              ?wd_value_from_wd wdt:P8168 ?fg_value_from_wd.
              ?wd_value_from_wd rdfs:label ?wd_value_from_wdLabel.
            }
          }
    
      }
      BIND (IF(?fg_value = ?wd_value_from_wd, "true", "false") AS ?is_same)
    }
  ')
}

query_time_items <- function(input_items_filter = NULL, fg_property_id = NULL) {
  paste0('
# Prefix standard
# Factgrid
PREFIX fg: <https://database.factgrid.de/entity/>
PREFIX fgt: <https://database.factgrid.de/prop/direct/>
PREFIX fgp: <https://database.factgrid.de/prop/>
PREFIX fgpsv: <https://database.factgrid.de/prop/statement/value/>

# Wikidata
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wdp: <http://www.wikidata.org/prop/>
PREFIX wdpsv: <http://www.wikidata.org/prop/statement/value/>

# misc
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT 
?fg_item ?fg_itemLabel ?wd_item ?fg_property ?fg_propertyLabel ?fg_property_type ?wd_property ?fg_value ?fg_valueLabel ?is_same ?fg_value_from_wd ?wd_value_from_wd ?wd_value_from_fg ?wd_value_from_wdLabel

where {
#BIND(fg:Q376282 as ?fg_item)
', input_items_filter, '

# which property in focus?
BIND(fgt:', fg_property_id, ' as ?fg_property)
BIND(fg:', fg_property_id, ' as ?fg_property_as_item)
BIND(fgp:', fg_property_id, ' as ?fg_property_as_p)
BIND(fgpsv:', fg_property_id, ' as ?fg_property_as_psv)

# set property of corresponding wikidata property; is constant
BIND(fgt:P343 as ?fg_corr_wd)

SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }

?fg_property_as_item wikibase:propertyType ?fg_property_type .

# transform wikidata qid in factgrid to wikidata entity iri
?link schema:about ?fg_item .
?link schema:isPartOf <https://www.wikidata.org/> .
?link schema:name ?qid.
BIND(IRI(CONCAT(STR(wd:), ?qid)) AS ?wd_item)

# get corresponding wikidata property
?fg_property_as_item ?fg_corr_wd ?fg_corr_wd_value .

# get factgrid statement for time
?fg_item ?fg_property ?fg_value_raw .
?fg_item ?fg_property_as_p [ a wikibase:BestRank ; ?fg_property_as_psv [ wikibase:timePrecision ?fg_value_timeprecision ] ].
# add time precision to fg_value
BIND(CONCAT(STR( ?fg_value_raw ), "/", STR(?fg_value_timeprecision)) AS ?fg_value ) .

# create properties to fetch time info from wikidata
BIND(IRI(CONCAT(STR(wdt:), ?fg_corr_wd_value)) AS ?wd_property)
BIND(IRI(CONCAT(STR(wdp:), ?fg_corr_wd_value)) AS ?wd_property_p)
BIND(IRI(CONCAT(STR(wdpsv:), ?fg_corr_wd_value)) AS ?wd_property_psv)

# now change to wikidata as data source and get value from wikidata
SERVICE <https://query.wikidata.org/sparql> {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  OPTIONAL {
    ?wd_item ?wd_property ?wd_value_raw.
    ?wd_item ?wd_property_p [ a wikibase:BestRank ; ?wd_property_psv [ wikibase:timePrecision ?wd_value_timeprecision ] ].
   BIND(CONCAT(STR( ?wd_value_raw ), "/", STR(?wd_value_timeprecision)) AS ?wd_value_from_wd ) .
  }
  }

  # comparison
  BIND (IF(?fg_value = ?wd_value_from_wd, "true", "false") AS ?is_same) .
}

')
}