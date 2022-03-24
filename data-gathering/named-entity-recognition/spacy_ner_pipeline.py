
# %%
import spacy

nlp = spacy.load("de_core_news_lg")

text = "LeZ – lesbisch-queeres Zentrum - Oberbürgermeister Dieter Reiter übergibt den Schlüssel zum neuen lesbisch-queeren Zentrum LeZ – lesbisch-queeres Zentrum in der Müllerstraße 26 und am Odeonsplatz. Von-der-Tann-Straße 23, Buschingstr. 19 and Lothringer Straße 23"
#text = "Lothringer Straße 34, Busching Straße 10"
# Create the EntityRuler
# before ner, otherwise LOC would sometimes overrule ADR

print(nlp.pipe_names)

#List of Entities and Patterns
street_labels = ".*(platz|[Ss]tra[ssß]e|str)"

patterns = [
    # Lothringer Straße 23
    {"label": "ADR", "pattern": [{"SHAPE": "Xxxxx"}, {"TEXT": {"REGEX": street_labels}}, {"IS_DIGIT": True}]},
    # Müllerstr. 26
    {"label": "ADR", "pattern": [{"TEXT": {"REGEX": street_labels}}, {"IS_PUNCT": True}, {"IS_DIGIT": True}]},
    # Müllerstraße 26
    {"label": "ADR", "pattern": [{"TEXT": {"REGEX": street_labels}}, {"IS_DIGIT": True}]},
    # Müllerstraße or Odeonsplatz    
    {"label": "ADR", "pattern": [{"TEXT": {"REGEX": street_labels}}]},
    ]

    

# check if entity_ruler exists
try:
    ruler
except NameError:
    ruler = nlp.add_pipe("entity_ruler", before="ner", config={"validate": True})

ruler.add_patterns(patterns)

# doc needs to be set after ruler and pipeline stuff, otherwise can't be set
doc = nlp(text)
#extract entities
for ent in doc.ents:
    print (ent.text, ent.label_)

# %%
for token in doc:
    print(token.text, token.lemma_, token.pos_, token.tag_, token.dep_,
            token.shape_, token.is_alpha, token.is_stop)
