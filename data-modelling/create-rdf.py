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

kg_string = kg.save_rdf_text(format="ttl")
print(kg_string)

kg.save_rdf("output/rdf-triples-poster.ttl")
kg.save_jsonld("output/rdf-triples-poster.jsonld")