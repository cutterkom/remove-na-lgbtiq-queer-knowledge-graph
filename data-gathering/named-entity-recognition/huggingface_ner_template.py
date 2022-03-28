
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

# %%
from transformers import pipeline, AutoModelForTokenClassification, AutoTokenizer
tokenizer = AutoTokenizer.from_pretrained("Davlan/distilbert-base-multilingual-cased-ner-hrl")
model = AutoModelForTokenClassification.from_pretrained("Davlan/distilbert-base-multilingual-cased-ner-hrl")

#%%
chronik_ner = pipeline("ner", model=model, framework="pt", tokenizer=tokenizer)
# %%
def predict(dataset):
    return {"predictions": chronik_ner(dataset['text'])}

#%%
result = dataset.select(range(1)).map(predict, batched=True, batch_size=4)
print(result)

#%%
for item in result:
    for prediction in item["predictions"]:
        print(prediction["entity"], prediction['score'])

# %%
import rubrix as rb
# this works kind of, expect rubrix issue
d = dataset.select(range(1))

records = []

for record in d:
    
    text = record['text']
    print(text)
    id = record['id']
    date = record['date']
    year = record['year']

    result = predict(record)
    print(result)

    entities = [
        (prediction["entity"], prediction["start"], prediction["end"], prediction["score"]) 
        for prediction in item["predictions"] for item in result
    ]
    #print(entities)

    tokens = tokenizer(text).tokens()
    #print(tokens)
    #tokens = text.split()

    record = rb.TokenClassificationRecord(
        text=text,
        tokens=tokens,
        metadata={'id': id, 'date': date, 'year': year},
        prediction=entities,
        prediction_agent="distilbert-base-uncased-finetuned-sst-2-english"

    )
    records.append(record)
    
print(records)

#%%
rb.log(records=records, name="hf_chronik_ner")



# %%
tokens = tokenizer(d['text'], return_offsets_mapping = True)
tokens.tokens()
# %%
