
# %%
import spacy

nlp = spacy.load("de_core_news_lg")

text = "LeZ – lesbisch-queeres Zentrum - Oberbürgermeister Dieter Reiter übergibt den Schlüssel zum neuen lesbisch-queeren Zentrum LeZ – lesbisch-queeres Zentrum in der Müllerstraße 26 und am Odeonsplatz. Von-der-Tann-Straße 23, Buschingstr. 19 and Lothringer Straße 23"
text = "Lothringer Straße 34, Busching Straße 10 Straßenkino Reichenbachstr. 51 Bayerstraße 77a Blumenstraße 11 Richardstraße 22. Müllerstr. 26; Von-der-Tann-Straße 23a Odeonsplatz Lothringer Straße Frauenkneipe Schmlazstraße"
#text = "sdafsa 51 Reichenbachstr. 51."
# Create the EntityRuler
# before ner, otherwise LOC would sometimes overrule ADR

street_labels = ".*(platz|[Ss]tra[ssß]e|str|anger)$"

patterns = [
    {"label": "ADR", "pattern": [ {"TEXT": {"REGEX": street_labels}}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]},
    {"label": "ADRddd", "pattern": [{"SHAPE": "Xxxxx", "OP": "?"}, {"TEXT": "Straße"}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]}
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
