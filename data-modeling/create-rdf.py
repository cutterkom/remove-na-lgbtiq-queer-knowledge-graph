import os
import kglab
namespaces = {
    "ex":  "http://example.com/",
    "schema": "https://schema.org/"
    }

kg = kglab.KnowledgeGraph(
    name = "A KG example",
    namespaces = namespaces,
    )

kg.describe_ns()

kg.materialize('configs/kg_v1.ini')

kg_string = kg.save_rdf_text(format="ttl")
#print(kg_string)

kg.save_rdf("output/kg_v1.ttl")
kg.save_jsonld("output/kg_v1.jsonld")

#kg.save_rdf("output/books-rdf-triples.ttl")
#kg.save_jsonld("output/books-rdf-triples.jsonld")