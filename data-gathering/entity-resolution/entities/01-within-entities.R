# find possible duplicates within chronik entities

library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)
library(testdat)

con <- connect_db("db_clean")
entities <- tbl(con, "entities") %>% 
  select(id, name) %>% 
  collect()
DBI::dbDisconnect(con); rm(con)
# Distinct Entities -------------------------------------------------------

input <- entities

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

min_sim <- 0.9
candidates <- calc_similarity(bigrams, method = "cosine", min_sim = min_sim) %>% 
  left_join(input, by = c("id_1" = "id")) %>% 
  left_join(input, by = c("id_2" = "id"), suffix = c("_1", "_2"))

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
