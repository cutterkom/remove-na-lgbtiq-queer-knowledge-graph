
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
    
    SELECT DISTINCT ?fg_item ?fg_itemLabel ?wd_item ?fg_property ?fg_propertyLabel ?fg_property_type ?wd_property ?fg_value ?fg_valueLabel ?wd_value_from_fg ?wd_value_from_wd ?fg_value_from_wd ?is_same where {
    
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
      ?link_value schema:about ?fg_value .
      ?link_value schema:isPartOf <https://www.wikidata.org/> .
      ?link_value schema:name ?qid_value.
      BIND(IRI(CONCAT(STR(wd:), ?qid_value)) AS ?wd_value_from_fg)
    
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
