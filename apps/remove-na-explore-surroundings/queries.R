# Federated Query

query_companions <-  function(fg_item = NULL) {
  paste0(
    '
#defaultView:ImageGrid
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
PREFIX wdp: <http://www.wikidata.org/prop/>
PREFIX wps: <http://www.wikidata.org/prop/statement/>
PREFIX wdpsv: <http://www.wikidata.org/prop/statement/value/>
# misc
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX dct:<http://purl.org/dc/terms/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX schema: <http://schema.org/>
prefix foaf:<http://xmlns.com/foaf/0.1/> 
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT 
# all data
?fg_item ?fg_itemLabel ?wd_item ?db_item ?value ?valueLabel ?relation ?relation_stringLabel ?image ?source
# more distinct
# ?fg_item ?valueLabel ?image ?sortname
WHERE {

# labels from Factgrid
SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }

# starting point is a factgrid item
BIND(fg:', fg_item, ' as ?fg_item)
# transform wikidata qid in factgrid to wikidata entity iri
?link schema:about ?fg_item .
?link schema:isPartOf <https://www.wikidata.org/> . 
?link schema:name ?qid.
BIND(IRI(CONCAT(STR(wd:), ?qid)) AS ?wd_item)

# FACTGRID: Get relations with all humans and organisations
{
# get all statments to a PERSON
{
?fg_item ?relation ?value .
?value fgt:P2 fg:Q7 .
?relation_string wikibase:directClaim ?relation.
BIND ("factgrid" AS ?source)
OPTIONAL { ?value fgt:P189 ?image }
}

}

# WIKIDATA: Get relations with all humans and organisations
UNION {
# Wikidata: get all statments to a PERSON
OPTIONAL {
SERVICE <https://query.wikidata.org/sparql> {
SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
?wd_item ?relation ?value .
?wd_item rdfs:label ?wd_itemLabel.
?value wdt:P31 wd:Q5 .
?value rdfs:label ?valueLabel.
FILTER(LANG(?valueLabel) = "[AUTO_LANGUAGE]") .
FILTER(LANG(?wd_itemLabel) = "[AUTO_LANGUAGE]") .
#?relation_string wikibase:directClaim ?relation.
BIND ("wikidata" AS ?source)
OPTIONAL { ?value wdt:P18 ?image }
}

}
}


# WIKIPEDIA: Persons mentioned in Wikipedia article 
UNION {
# fetch data from DBpedia
SERVICE <https://dbpedia.org/sparql> {
# get Wikidata QID from DBPedia resource
?db_item owl:sameAs ?wd_item

OPTIONAL { 
# get all wikipedia links
?db_item dbo:wikiPageWikiLink ?value .
# just those that are persons
?value a dbo:Person .
?value rdfs:label ?valueLabel .
FILTER(LANG(?valueLabel) = "[AUTO_LANGUAGE]") .
# remove Stefan Zweig, because there is some data fuckup within that resource (I informed DBpedia)
MINUS {FILTER(REGEX(STR(?value), "Stefan_Zweig|LGBT_rights_by_country_or_territory|Barbara Hammer|Deutschland|Coming_out"))}
# also get their wikidata qids
#?value owl:sameAs ?db_wikilink_wd_item FILTER(regex(str(?db_wikilink_wd_item), "wikidata" ))
# get image
OPTIONAL { ?value dbo:thumbnail ?image }

BIND ("wikipedia" AS ?source)
BIND ("mentioned_in_wikipedia" AS ?relation_stringLabel)
BIND (dbo:wikiPageWikiLink AS ?relation)


}

}
# WIKIPEDIA: Get Persons from DBPedia
} UNION {
# fetch data from DBpedia
SERVICE <https://dbpedia.org/sparql> {
# get Wikidata QID from DBPedia resource
?db_item owl:sameAs ?wd_item .
?wd_item ?relation ?value .
?value a dbo:Person .
?value rdfs:label ?valueLabel .
FILTER(LANG(?valueLabel) = "[AUTO_LANGUAGE]") .
# remove Stefan Zweig, because there is some data fuckup within that resource (I informed DBpedia)
MINUS {FILTER(REGEX(STR(?value), "Stefan_Zweig|LGBT_rights_by_country_or_territory|Barbara Hammer|Deutschland|Coming_out"))}
BIND ("dbpedia" AS ?source)
OPTIONAL { ?value dbo:thumbnail ?image }

}
}
}
')
}
