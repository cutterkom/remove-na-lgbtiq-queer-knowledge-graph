import os
from icecream import ic
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

kg.materialize('config.ini');

kg_string = kg.save_rdf_text(format="ttl")
print(kg_string)

kg.save_rdf("output/rdf-triples.ttl")
kg.save_jsonld("output/rdf-triples.jsonld")