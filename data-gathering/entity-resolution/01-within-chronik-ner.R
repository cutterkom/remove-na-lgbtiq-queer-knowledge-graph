library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)
library(testdat)

con <- connect_db()
chronik_entities_raw <- tbl(con, "chronik_entities") %>% 
  collect() %>% 
  select(-chronik_entry_id)
DBI::dbDisconnect(con); rm(con)

# keep only those that might be relevant
chronik_entities <- chronik_entities_raw %>% 
  filter(!str_detect(label, "DATE"), !label %in% c("ADR", "CITY", "COUNTRY", "Slogan", "DISTRICT", "LAW", "MOVEMENT"))

# Distinct Entities -------------------------------------------------------

input <- chronik_entities_raw

# Get string mappings -----------------------------------------------------

# used to remove authors who seem to be organisations
string_mappings <- config::get(file = "static/string-mapping.yml") 

bigrams_input <- input %>% 
  select(id, name) %>% 
  clean_string(col = "name") %>% 
  filter(
    !str_detect(tolower(name), paste0(string_mappings$nonsense, collapse = "|")),
    # remove if there is no space in the name -> just 1 name or abbreviation
    str_detect(name, "\\s")
  )

bigrams_input 

# Prepare text data -------------------------------------------------------

corp <- corpus(bigrams_input, text_field = "name", docid_field = "id")
toks <- tokens(corp)

# create bigrams on word level = shingles
bigrams <- corp %>% 
  tokens(what = "character") %>%
  tokens_keep("[A-Za-z]", valuetype = "regex") %>%
  tokens_ngrams(n = 2, concatenator = "") %>%
  dfm()

# Calculate similarities -> create candidate pairs -------------------------

min_sim <- 0.75
candidates <- calc_similarity(bigrams, method = "cosine", min_sim = min_sim) %>% 
  left_join(input, by = c("id_1" = "id")) %>% 
  left_join(input, by = c("id_2" = "id"), suffix = c("_1", "_2")) %>% 
  # only same entity category needs to be analyzed
  filter(label_1 == label_2)

# Write in DB -------------------------------------------------------------

import <- candidates %>% 
  mutate(entities = "chronik_within", .before = id_1,
         source_1 = "chronik",
         source_2 = "chronik",
         decision = ifelse(value == 1, "positive", "no_judgement")
         ) %>% 
  select(-contains("name"), -contains("label"))

test_that(
  desc = "uniqueness",
  expect_unique(c("id_1", "id_2", "source_1", "source_2"), data = import)
)

con <- connect_db()
DBI::dbAppendTable(con, "er_candidates", import)
DBI::dbDisconnect(con); rm(con)
