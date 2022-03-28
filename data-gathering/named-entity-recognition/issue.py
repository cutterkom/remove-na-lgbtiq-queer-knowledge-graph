#%%
import rubrix as rb
from transformers import BertTokenizer, pipeline, AutoModel, AutoTokenizer

#tokenizer = BertTokenizer.from_pretrained("dbmdz/bert-base-german-cased")

#%%
model = AutoModel.from_pretrained("dbmdz/bert-base-german-cased")

#model = "distilbert-base-uncased-finetuned-sst-2-english"
#model = "dbmdz/bert-base-german-cased"
model="elastic/distilbert-base-cased-finetuned-conll03-english"
input_text = "Mein Name ist Sarah und ich lebe in London."
#input_text = "My name is Sarah and I live in London"

#%%
tokenizer = AutoTokenizer.from_pretrained("dbmdz/bert-base-german-cased")


#%%
# We define our HuggingFace Pipeline
classifier = pipeline(
    "ner",
    model=model,
    framework="pt",
    tokenizer=tokenizer
)

#%%

# Making the prediction
predictions = classifier(
    input_text,
)

# Creating the prediction entity as a list of tuples (entity, start_char, end_char)
prediction = [(pred["entity"], pred["start"], pred["end"]) for pred in predictions]
print(prediction)

#%%
transformer_tokens = tokenizer(input_text, return_offsets_mapping = True).tokens()
print(transformer_tokens)
encoding.token_to_chars()
#%%
# Building a TokenClassificationRecord
record = rb.TokenClassificationRecord(
    text=input_text,
    tokens=input_text.split(),
    prediction=prediction,
    prediction_agent=model,
)

# Logging into Rubrix
rb.log(records=record, name="issue-ner")
# %%
