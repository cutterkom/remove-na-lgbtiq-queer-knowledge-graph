"""Convert entity annotation from Rubrix format to spaCy v3 .spacy format."""

import rubrix as rb
import pandas as pd
import spacy
from tqdm import tqdm
from spacy.tokens import DocBin

def convert_rubrix_to_spacy(rubrix_name:str, query:str):
    """
    import annotated data from rubrix and transform it to spacy flavour
    @name name of rubrix dataset to import
    @query query rubrix data, typically "status:Validated"
    """
    # load rubrix dataset
    labeled_data = rb.load(rubrix_name, query=query)

    labeled_data_df = pd.DataFrame({
        "text": labeled_data.text,
        "label": labeled_data.annotation,
    })
    
    training_data = []

    for record in labeled_data_df.index:
        entities = []
        text = labeled_data_df["text"][record]
        labels = labeled_data_df["label"][record]
        
        for label in labels:
            
            start = label[1]
            end = label[2]
            label = label[0]
            # switch position
            entity = text, label, start, end
            entities.append((start, end, label))

        training_data.append([(text, entities)])
    return(training_data)


def create_doc_bin(data: list, lang: str):

    nlp = spacy.blank(lang)

    # the DocBin will store the documents
    doc_bin = DocBin(attrs=["ENT_IOB", "ENT_TYPE"])

    for record in tqdm(data):
        
        # text are class list, need to be transformed to character
        text = " ".join(map(str,[el[0] for el in record]))
        doc = nlp(text)

        annotations = [item[1] for item in record]
        # print("annotations:")
        # print(annotations)
        ents = []
        
        for annotation in annotations[0]:
            # add start, end and label as spans
            start = annotation[0]
            end = annotation[1]
            label = annotation[2]
            span = doc.char_span(start, end, label=label)
            ents.append(span)
        doc.ents = ents
        doc_bin.add(doc)
    return(doc_bin)

training_data = convert_rubrix_to_spacy(rubrix_name="chronik_annotations", query="status:Validated")

# here: splir 20 - 80
create_doc_bin(training_data, "de").to_disk("tesssst.spacy")