# title: Save NER results to db
# desc: 
# input: 2 csv files, exported from rubrix
# output: lgbtiq_kg_clean.chronik_entities


library(tidyverse)
library(kabrutils)

# import manually labeled data --------------------------------------------

# output from data-gathering/named-entity-recognition/train-spacy-ipynb
labeled_data <- read_csv("data-gathering/named-entity-recognition/data/rubrix-export-ner-model.csv") %>% #View
  select(text, tokens, annotation, metadata)

# output from data-gathering/named-entity-recognition/spacy-ner.py
addresses_dates <- read_csv("data-gathering/named-entity-recognition/data/rubrix-export-addresses-dates.csv") %>% 
  # rename prediction col, because not validated yet -> therefore called prediction instead of annotation 
  select(text, tokens, annotation = prediction, metadata) %>% 
  # remove pred w'keit and empty rows
  mutate(annotation = str_remove_all(annotation, ", 1\\.0")) %>% 
  filter(annotation != "[]")

# Extract annotations and ids ---------------------------------------------

annotations <- bind_rows(labeled_data, addresses_dates) %>% 
  select(annotation, metadata) %>% 
  separate_rows(annotation, sep = "\\)\\, \\(") %>% 
  # remove unnnessessary 
  mutate(annotation = str_remove_all(annotation, "\\[\\(|\\)\\]"),
         id = str_extract(metadata, "(?<=id':).*"),
         id = as.numeric(str_remove(id, "\\}"))) %>%  
  distinct(id, annotation, metadata) %>% 
  separate(col = "annotation", into = c("label", "start", "end"), sep = ", ") %>% 
  mutate(label = str_remove_all(label, "'"))

# Clean data: id, label, word ---------------------------------------------

chronik_entities <- bind_rows(labeled_data, addresses_dates) %>% #labeled_data %>% 
  select(text, metadata) %>% 
  left_join(annotations, by = "metadata") %>% 
  select(-metadata) %>% 
  mutate(
    word = trimws(substr(text, start, end)),
    word = str_remove_all(word, "„|“|\\(|\\)|^-|,$|^\\/|\\.$"),
    word = str_replace_all(word, "§§", "§")
  ) %>% 
  select(id, label, name = word) %>% 
  distinct()

# Add to chronik_db data --------------------------------------------------

import <- chronik_entities %>% distinct()

con <- connect_db("db_clean")
DBI::dbWriteTable(con, "chronik_entities", import)
DBI::dbDisconnect(con)
rm(con)
