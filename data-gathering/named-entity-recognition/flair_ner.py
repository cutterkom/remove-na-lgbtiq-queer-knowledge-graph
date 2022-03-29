#%%
# get data
from sqlalchemy import create_engine
import pandas as pd
engine = create_engine("mysql+pymysql://root:root@localhost:3306/lgbtiq_kg")

dataset = pd.read_sql_table(
    'text_chronik', 
    con=engine)

dataset["text"] = dataset["title"] + " - " + dataset["text"]

# load dataset from pandas dataset to hg Dataset
from datasets import Dataset
dataset = Dataset.from_pandas(dataset)

#%%
import rubrix as rb

from flair.data import Sentence
from flair.models import SequenceTagger

# load tagger
tagger = SequenceTagger.load("flair/ner-german-large")

# %%
records = []
for record in dataset:

    text = record['text']
    print(text)
    id = record['id']
    date = record['date']
    year = record['year']
    
    sentence = Sentence(text)
    tagger.predict(sentence)
    prediction = [
        (entity.get_labels()[0].value, entity.start_pos, entity.end_pos)
        for entity in sentence.get_spans("ner")
    ]
    #print(prediction)

    # building TokenClassificationRecord
    records.append(
        rb.TokenClassificationRecord(
            text=text,
            tokens=[token.text for token in sentence],
            metadata={'id': id, 'date': date, 'year': year},
            prediction=prediction,
            prediction_agent="flair_tars-ner",
        )
    )

#%%
# # log the records to Rubrix
rb.log(records, name='chronik_ner')
# %%
