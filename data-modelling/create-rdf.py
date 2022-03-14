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

kg.materialize('configs/posters-config.ini')
#kg.materialize('configs/books-config.ini')

kg_string = kg.save_rdf_text(format="ttl")
print(kg_string)

kg.save_rdf("output/posters-rdf-triples.ttl")
kg.save_jsonld("output/posters-rdf-triples.jsonld")

#kg.save_rdf("output/books-rdf-triples.ttl")
#kg.save_jsonld("output/books-rdf-triples.jsonld")