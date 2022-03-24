# Try out Spacy NER

# with 2 models: 
# - big german model de_core_news_lg
# - small german model

# Save results as Rubrix record

#%% 
import spacy
import rubrix as rb

# delete, if exists
rb.delete(name="chronik_ner")

# %%
# get data
# connect to db
from sqlalchemy import create_engine
import pandas as pd
engine = create_engine("mysql+pymysql://root:root@localhost:3306/lgbtiq_kg")

dataset = pd.read_sql_table(
    'text_chronik', 
    con=engine)

dataset["text"] = dataset["title"] + " - " + dataset["text"]
#dataset = dataset[["id", "text"]]
dataset = pd.DataFrame(dataset)
dataset.head()
# %%

# get entities foe earch row and save them as rubrix records

# big German modell
nlp = spacy.load("de_core_news_lg")

records = []

for record in dataset.index:
    # We only need the text of each instance
    text = dataset['text'][record]
    # get id for rubrix record metadata
    id = dataset['id'][record].tolist() # not allowed to be int64
    date = dataset['date'][record]
    year = dataset['year'][record].tolist() # not allowed to be int64
    
    
    # spaCy Doc creation
    doc = nlp(text)
    # Entity annotations
    entities = [
        (ent.label_, ent.start_char, ent.end_char)
        for ent in doc.ents
    ]

    # Pre-tokenized input text
    tokens = [token.text for token in doc]

    # Rubrix TokenClassificationRecord list
    records.append(
        rb.TokenClassificationRecord(
            text=text,
            tokens=tokens,
            metadata={'id': id, 'date': date, 'year': year}, # log the intents for exploration of specific intents
            prediction=entities,
            prediction_agent="de_core_news_lg",
        )
    )

rb.log(records=records, name="chronik_ner")
# %%
# small german modell
nlp = spacy.load("de_core_news_sm")

records = []

for record in dataset.index:
    # We only need the text of each instance
    text = dataset['text'][record]
    # get id for rubrix record metadata
    id = dataset['id'][record].tolist() # not allowed to be int64
    date = dataset['date'][record]
    year = dataset['year'][record].tolist() # not allowed to be int64
    
    
    # spaCy Doc creation
    doc = nlp(text)
    # Entity annotations
    entities = [
        (ent.label_, ent.start_char, ent.end_char)
        for ent in doc.ents
    ]

    # Pre-tokenized input text
    tokens = [token.text for token in doc]

    # Rubrix TokenClassificationRecord list
    records.append(
        rb.TokenClassificationRecord(
            text=text,
            tokens=tokens,
            metadata={'id': id, 'date': date, 'year': year}, # log the intents for exploration of specific intents
            prediction=entities,
            prediction_agent="de_core_news_sm",
        )
    )

rb.log(records=records, name="chronik_ner")

#%% Add addresses as entity, big model
nlp = spacy.load("de_core_news_lg")


street_labels = ".*(platz|[Ss]tra[ssß]e|str|anger)$"

patterns = [
    # Müllerstraße 20
    {"label": "ADR", "pattern": [ {"TEXT": {"REGEX": street_labels}}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]},
    # Müller Straße 20
    {"label": "ADR", "pattern": [{"SHAPE": "Xxxxx", "OP": "?"}, {"TEXT": "Straße"}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]}
    ]

# check if entity_ruler exists
try:
    ruler
except NameError:
    ruler = nlp.add_pipe("entity_ruler", before="ner")

ruler.add_patterns(patterns)

records = []

for record in dataset.index:
    # We only need the text of each instance
    text = dataset['text'][record]
    # get id for rubrix record metadata
    id = dataset['id'][record].tolist() # not allowed to be int64
    date = dataset['date'][record]
    year = dataset['year'][record].tolist() # not allowed to be int64
    
    
    # spaCy Doc creation
    doc = nlp(text)
    # Entity annotations
    entities = [
        (ent.label_, ent.start_char, ent.end_char)
        for ent in doc.ents
    ]

    # Pre-tokenized input text
    tokens = [token.text for token in doc]

    # Rubrix TokenClassificationRecord list
    records.append(
        rb.TokenClassificationRecord(
            text=text,
            tokens=tokens,
            metadata={'id': id, 'date': date, 'year': year}, # log the intents for exploration of specific intents
            prediction=entities,
            prediction_agent="ADR_de_core_news_lg",
        )
    )

rb.log(records=records, name="chronik_ner")

# rb.delete(name="chronik_ner")
# %%
