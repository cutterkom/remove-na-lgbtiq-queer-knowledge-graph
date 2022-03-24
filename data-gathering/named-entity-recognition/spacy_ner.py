# Try out Spacy NER

# with 2 models: 
# - big german model de_core_news_lg
# - small german model

# Save results as Rubrix record

#%% 
import spacy
import rubrix as rb
from tqdm.auto import tqdm



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

for record in df.index:
    # We only need the text of each instance
    text = df['text'][record]
    # get id for rubrix record metadata
    id = df['id'][record].tolist() # not allowed to be int64
    date = df['date'][record]
    year = df['year'][record].tolist() # not allowed to be int64
    
    
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