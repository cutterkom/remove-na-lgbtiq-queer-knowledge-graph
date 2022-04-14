library(tidyverse)
library(kabrutils)
library(jsonlite)


con <- connect_db("db_clean")
entities <- tbl(con, "entities") %>% 
  select(entity_id = id, name) %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  select(id, external_id, external_id_label, external_id_type) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)

con <- connect_db()
chronik_db <- tbl(con, "text_chronik") %>% 
  select(-contains("_at")) %>% 
  collect()
DBI::dbDisconnect(con); rm(con)



# import manually labeled data --------------------------------------------

labeled_data <- read_csv("data-gathering/named-entity-recognition/data/rubrix-export.csv") %>% 
  select(text, tokens, annotation, metadata)


# Extract annotations and ids ---------------------------------------------

annotations <- labeled_data %>% 
  select(annotation, metadata) %>% 
  separate_rows(annotation, sep = "\\)\\, \\(") %>% 
  mutate(annotation = str_remove_all(annotation, "\\[\\(|\\)\\]"),
         id = str_extract(metadata, "(?<=id':).*"),
         id = as.numeric(str_remove(id, "\\}"))) %>% 
  distinct(id, annotation, metadata) %>% 
  separate(col = "annotation", into = c("label", "start", "end"), sep = ", ") %>% 
  mutate(label = str_remove_all(label, "'"))

# Clean data: id, label, word ---------------------------------------------

chronik_entities <- labeled_data %>% 
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

chronik <- chronik_db %>% 
  left_join(chronik_entities, by = "id") %>% 
  left_join(entities, by = c("name")) %>% 
  left_join(el_matches, by = c("entity_id" = "id"))
