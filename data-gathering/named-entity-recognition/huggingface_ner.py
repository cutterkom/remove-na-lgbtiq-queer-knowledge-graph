#%%
from transformers import pipeline

ner_pipe = pipeline("ner")

sequence = """Hugging Face Inc. is a company based in New York City. Its headquarters are in DUMBO,
therefore very close to the Manhattan Bridge which is visible from the window."""

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

#%%
# load dataset from pandas dataset to hg Dataset
from datasets import Dataset
dataset = Dataset.from_pandas(dataset)
print(dataset)

#%%
ner_results = ner_pipe(sequence)
print(ner_results)
#%%
for entity in ner_pipe(sequence):
    print(entity)
# %%
for record in dataset:
    for entity in ner_pipe(record):
        print(entity)
# %%
type(dataset)
# %%
from transformers import pipeline, AutoModelForTokenClassification, AutoTokenizer
model = AutoModelForTokenClassification.from_pretrained("dbmdz/bert-base-german-cased")

#tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
tokenizer = AutoTokenizer.from_pretrained("dbmdz/bert-base-german-cased")

#%%
#chronik_ner = pipeline("ner", model=model, tokenizer=tokenizer)
chronik_ner = pipeline("ner", model=model, framework="pt", tokenizer=tokenizer)

   

# %%
# ner one row
dataset[2]['text'], chronik_ner(dataset[2]['text'])
# %%
def predict(dataset):
    return {"predictions": chronik_ner(dataset['text'])}

# add .select(range(10)) before map if you just want to test this quickly with 10 items
result = dataset.select(range(1)).map(predict, batched=True, batch_size=4)

#%%
print(result)

#%%
for item in result:
    for prediction in item["predictions"]:
        print(prediction["entity"], prediction['score'])

#%%
for item in result:
    print(item)

#%% 
for item in result:
    print(item["predictions"][["end"]])
    

#%%
entities = [
    (prediction["entity"], prediction["start"], prediction["end"]) for item in result for prediction in item["predictions"] 
]
entities

#%%
for item in result for prediction in item["predictions"] 

#%%
encoding = tokenizer("sdatasetsdataset sdatasetasf", return_offsets_mapping = True)
print(type(encoding))
encoding.tokens()
encoding.token_to_chars()

# %%
import rubrix as rb

records = []
for item in result:

    # We only need the text of each instance
    text = item['text']
    # get id for rubrix record metadata
    id = item['id']#.tolist() # not allowed to be int64
    date = item['date']
    year = item['year']#.tolist() # not allowed to be int64

    tokens = tokenizer(text)

    entities = [
        (prediction["entity"], prediction["start"], prediction["end"], prediction["score"]) 
        for prediction in item["predictions"] 
    ]
    #print(entities)

    record = rb.TokenClassificationRecord(
        text=text,
        tokens=tokens.tokens(),
        metadata={'id': id, 'date': date, 'year': year},
        prediction=entities,
        prediction_agent="distilbert-base-uncased-finetuned-sst-2-english"

    )
    records.append(record)


records
#rb.log(records=records, name="chronik_ner_hf")
# %%
type(ner_preds)
# %%
ner_preds.select(range(10))
# %%
for record in dataset:
    print(record)
# %%

# %%
for record in dataset:
    tokens = tokenizer(text).tokens()
    print(tokens)

# %%
# this works kind of, expect rubrix issue
d = dataset.select(range(1))

records = []

for record in d:
    
    text = record['text']
    #print(text)
    id = record['id']
    date = record['date']
    year = record['year']

    result = predict(record)
    # print(result)

    entities = [
        (prediction["entity"], prediction["start"], prediction["end"], prediction["score"]) 
        for item in result
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
q# %%
print(records)


# %%
tokens = tokenizer(input_text, return_offsets_mapping = True)
print(type(encoding))
tokens.tokens()