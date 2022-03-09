import os
from icecream import ic
import kglab

namespaces = {
    "ex":  "http://example.com/",
    }

kg = kglab.KnowledgeGraph(
    name = "A KG example with students and sports",
    namespaces = namespaces,
    )
print(kg)
kg.materialize('config.ini');

kg_to_write = kg.save_rdf_text(format="ttl")
print(kg_to_write)
kg.save_rdf("rdf-from-yamrrrl.ttl")