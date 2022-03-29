
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
