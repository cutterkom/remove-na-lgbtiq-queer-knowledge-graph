
# %%
import spacy

nlp = spacy.load("de_core_news_lg")

text = "dsf fdsfsa Korfa e.V. und auch Mi Les e.V. und Forum Homosexualität e.V.  Forum Queeres Archiv e.V.  FrauenWohnen eG und der Fischer Verlag"
# Create the EntityRuler
# before ner, otherwise LOC would sometimes overrule ADR

street_labels = ".*(platz|[Ss]tra[ssß]e|str|anger)$"
org_labels = "e\.V\.|eG|Verlag"

patterns = [
    {"label": "ADR", "pattern": [ {"TEXT": {"REGEX": street_labels}}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]},
    {"label": "ADRddd", "pattern": [{"SHAPE": "Xxxxx", "OP": "?"}, {"TEXT": "Straße"}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]},
    {"label": "ORGA", "pattern": [
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}, "OP": "?"}, 
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}, "OP": "?"}, 
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}}, 
        {"TEXT": {"REGEX": org_labels}}]},
    ]


# check if entity_ruler exists
try:
    ruler
except NameError:
    ruler = nlp.add_pipe("entity_ruler", before="ner")

print(nlp.pipe_names)

ruler.add_patterns(patterns)

# doc needs to be set after ruler and pipeline stuff, otherwise can't be set
doc = nlp(text)
#extract entities
for ent in doc.ents:
    print (ent.text, ent.label_)




#%%
for token in doc:
    print(token.text, token.lemma_, token.pos_, token.tag_, token.dep_,
            token.shape_, token.is_alpha, token.is_stop)

# %%
for token in doc:
    print(token.text, token.shape_)

# %%
from sqlalchemy import create_engine
import pandas as pd
engine = create_engine("mysql+pymysql://root:root@localhost:3306/lgbtiq_kg_clean")

dataset = pd.read_sql_table(
    'entities', 
    con=engine)

print(dataset[["id", "name"]])
#print(dataset)

#%%

# PhraseMatcher
import json
from spacy.lang.de import German

nlp = German()
doc = nlp("Sittenstrolche  Münchner Löwen-Club Tschechien könnte der Slowakei dabei helfen, ihren Luftraum zu schützen")

# Importiere den PhraseMatcher und initialisiere ihn
from spacy.matcher import PhraseMatcher

matcher = PhraseMatcher(nlp.vocab)

# add pattern per org, person, club
# dann adden
patterns = list(nlp.pipe(dataset["name"]))
matcher.add("REMOVENA_ENTS", None, *patterns)

# Wende den Matcher auf das Test-Dokument an und drucke das Resultat
matches = matcher(doc)
print([doc[start:end] for match_id, start, end in matches])

from spacy.tokens import Span
REMOVENA_ENTS = doc.vocab.strings[u'REMOVENA_ENTS']

new_ents = [Span(doc, match[1],match[2],label=REMOVENA_ENTS) for match in matches]

doc.ents = list(doc.ents) + new_ents

def show_ents(doc):
    if doc.ents:
        for ent in doc.ents:
            print(ent.text+' - '+ent.label_+' - '+str(spacy.explain(ent.label_)))
    else:
        print('No named entities found.')
show_ents(doc)

# %%
for match_id, start, end in matches:
    rule_id = nlp.vocab.strings[match_id]  # get the unicode ID, i.e. 'COMPANY'
    span = doc[start : end]  # get the matched slice of the doc
    print(rule_id, span.text)
# %%
#bind together
import spacy

nlp = spacy.load("de_core_news_lg")

text = "dsf fdsfsa Korfa e.V. und auch Mi Les e.V. und Forum Homosexualität e.V.  Forum Queeres Archiv e.V.  FrauenWohnen eG und der Fischer Verlag"
# Create the EntityRuler
# before ner, otherwise LOC would sometimes overrule ADR

street_labels = ".*(platz|[Ss]tra[ssß]e|str|anger)$"
org_labels = "e\.V\.|eG|Verlag"

patterns = [
    {"label": "ADR", "pattern": [ {"TEXT": {"REGEX": street_labels}}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]},
    {"label": "ADRddd", "pattern": [{"SHAPE": "Xxxxx", "OP": "?"}, {"TEXT": "Straße"}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]},
    {"label": "ORGA", "pattern": [
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}, "OP": "?"}, 
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}, "OP": "?"}, 
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}}, 
        {"TEXT": {"REGEX": org_labels}}]},
    ]


# check if entity_ruler exists
try:
    ruler
except NameError:
    ruler = nlp.add_pipe("entity_ruler", before="ner")

print(nlp.pipe_names)

ruler.add_patterns(patterns)

from spacy.matcher import PhraseMatcher

matcher = PhraseMatcher(nlp.vocab)

# add pattern per org, person, club
# dann adden
patterns = list(nlp.pipe(dataset["name"]))
matcher.add("REMOVENA_ENTS", None, *patterns)

# Wende den Matcher auf das Test-Dokument an und drucke das Resultat
matches = matcher(doc)
print([doc[start:end] for match_id, start, end in matches])

from spacy.tokens import Span
REMOVENA_ENTS = doc.vocab.strings[u'REMOVENA_ENTS']

new_ents = [Span(doc, match[1],match[2],label=REMOVENA_ENTS) for match in matches]

doc.ents = list(doc.ents) + new_ents

def show_ents(doc):
    if doc.ents:
        for ent in doc.ents:
            print(ent.text+' - '+ent.label_+' - '+str(spacy.explain(ent.label_)))
    else:
        print('No named entities found.')
show_ents(doc)

# %%
