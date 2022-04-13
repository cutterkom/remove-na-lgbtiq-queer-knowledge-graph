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

#%% Add addresses as entity, big model
nlp = spacy.load("de_core_news_lg")


street_labels = "Sendlinger Tor|Viktualienmarkt|.*(platz|[Ss]tra[ssß]e|str|anger)$"
org_labels = "e\.V\.|eG|Verlag"

date_marker_start =  "[Aa]b$|[Zw]ischen$|[Bb]is$|[Ss]eit$|[Vv]on$"
date_marker_between =  "\\-|und|bis"
date_marker_decade = "er|er Jahre"
day = "0?[1-9]|[12]\d|3[01]"
month = "Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember|0?1\.|0?2\.|0?3\.|0?4\.|0?5\.|0?6\.|0?7\.|0?8\.|0?9\.|10\.|11\.|12\."
year = "[12][0-9]{3}" # years from 1000 to 2999
decade_years = "[12][0-9]{2}0ern?" # 1970er, 1990er
daymonthyear = "^(((0?[1-9]|[12]\d|3[01])\.(0[13578]|[13578]|1[02])\.((1[6-9]|[2-9]\d)\d{2}))|((0?[1-9]|[12]\d|30)\.(0[13456789]|[13456789]|1[012])\.((1[6-9]|[2-9]\d)\d{2}))|((0?[1-9]|1\d|2[0-8])\.0?2\.((1[6-9]|[2-9]\d)\d{2}))|(29\.0?2\.((1[6-9]|[2-9]\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))))$"


patterns = [
    {"label": "ADR", "pattern": [ {"TEXT": {"REGEX": street_labels}}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]},
    {"label": "ADR", "pattern": [{"SHAPE": "Xxxxx", "OP": "?"}, {"TEXT": "Straße"}, {"IS_PUNCT": True, "OP": "?"}, {"SHAPE": {"IN": ["d", "dd", "ddd", "dddx", "ddx", "dx", "d.", "dd.", "ddd."]}, "OP": "?"}]},
    {"label": "ORG", "pattern": [
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}, "OP": "?"}, 
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}, "OP": "?"}, 
        {"SHAPE": {"IN": ["Xx", "Xxxxx"]}}, 
        {"TEXT": {"REGEX": org_labels}}]},


   {"label": "DATE_BETWEEN", "pattern": [
        # zwischen 21. Juni 2000 und 25. Juli 2000
        # von 21. Januar bis 10.10.2000
        {"TEXT": {"REGEX": date_marker_start}},
        {"TEXT": {"REGEX": day}, "OP": "?"},
        {"TEXT": {"REGEX": month}},
        {"TEXT": {"REGEX": year}, "OP": "?"},
        {"TEXT": {"REGEX": date_marker_between}},
        {"TEXT": {"REGEX": day}, "OP": "?"},
        {"TEXT": {"REGEX": month}},
        {"TEXT": {"REGEX": year}, "OP": "?"}
    ]},
    {"label": "DATE_SINGLE", "pattern": [
        # 1. Mai 2022, 
       {"TEXT": {"REGEX": day}},
       {"TEXT": {"REGEX": month}},
       {"TEXT": {"REGEX": year}}
    ]},
    {"label": "DATE_SINGLE_WITHOUT_DAY", "pattern": [
        # Mai 2022, 
       {"TEXT": {"REGEX": month}},
       {"TEXT": {"REGEX": year}}
    ]},
    {"label": "DATE_SINGLE_COMPACT", "pattern": [
        # 1.10.2000
       {"TEXT": {"REGEX": daymonthyear}}
    ]},


    {"label": "DATE_BETWEEN", "pattern": [
        # zwischen 1988 und 2020
        {"TEXT": {"REGEX": date_marker_start}},
        {"SHAPE": "dddd"},
        {"TEXT": {"REGEX": date_marker_between}},
        {"SHAPE": "dddd"}
    ]},
        {"label": "DATE_DECADE", "pattern": [
        # 1970er, 1990ern
        {"TEXT": {"REGEX": decade_years}}
        ]},

    {"label": "DATE_START", "pattern": [
        # Ab 1820, bis 1990
        {"TEXT": {"REGEX": date_marker_start}}, 
        {"TEXT": {"REGEX": year}}
        ]},

    {"label": "DATE_YEAR_FROM_TO", "pattern": [
        # 1999 - 2022, 1999-2022
        {"TEXT": {"REGEX": year}},
        {"TEXT": {"REGEX": "\s?-\s?"}},
        {"TEXT": {"REGEX": year}}
        ]},
    {"label": "DATE_YEAR", "pattern": [
        # 1999
        {"TEXT": {"REGEX": year}}
        ]}
    
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
            prediction_agent="spacy_de_core_news_lg_date",
        )
    )

#%%
rb.log(records=records, name="chronik_ner")


