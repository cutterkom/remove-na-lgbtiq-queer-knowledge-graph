
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
dataset.head()

# load dataset from pandas dataset to hg Dataset
from datasets import Dataset
dataset = Dataset.from_pandas(dataset)

from transformers import pipeline
classifier = pipeline("ner", "Davlan/distilbert-base-multilingual-cased-ner-hrl")

import rubrix as rb
# this works kind of, expect rubrix issue
d = dataset.select(range(1))

records = []

for record in dataset:
    
    text = record['text']
    #print(text)
    id = record['id']
    date = record['date']
    year = record['year']

    # Tokens as subword tokens
    batch_encoding = classifier.tokenizer(text)
   
    # However, we would like word tokens
    words, last_id = [], None
    for word_id in batch_encoding.word_ids():
        if word_id is None or word_id == last_id:
            continue
        char_span = batch_encoding.word_to_chars(word_id)
        words.append(text[char_span.start:char_span.end])
        last_id = word_id
        
    words
    predictions = classifier(text, aggregation_strategy="first")

    record = rb.TokenClassificationRecord(
        text=text,
        tokens=words,
        metadata={'id': id, 'date': date, 'year': year},
        prediction=[(pred["entity_group"], pred["start"], pred["end"]) for pred in predictions],
        prediction_agent="hugging_face_distilbert"

    )
    records.append(record)

rb.log(records=records, name="chronik_ner")
# %%
