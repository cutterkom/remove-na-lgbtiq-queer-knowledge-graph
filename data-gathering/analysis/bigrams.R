# Ngrams based blocks
# 2 grams -> messier names data

library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)
library(DBI)

source("R/deduplication-matching.R")
source("R/utils.R")
source("R/entity-cleaning.R")

con <- connect_db()
authors <- tbl(con, "books_authors") %>%
  collect()
dbDisconnect(con); rm(con)


bigrams_authors <- authors %>% 
  select(id = author_id, name = author) %>% 
  mutate(id = paste0("books_", id)) %>% 
  split_human_name(col = "name") %>% 
  clean_string(col = "name") %>% 
  filter(
    # remove Verlage and Vereine
    !str_detect(tolower(name), "verlag|e.v.|kollektiv"),
    # remove ? 
    !name %in% c("?", "et al."),
    # remove if there is no space in the name -> just 1 name or abbreviation
    str_detect(name, "\\s")
  )

# Prepare text data -------------------------------------------------------

corp <- corpus(bigrams_authors, text_field = "name", docid_field = "id")
toks <- tokens(corp)

# create bigrams on word level = shingles
bigrams <- corp %>% 
  tokens(what = "character") %>%
  tokens_keep("[A-Za-z]", valuetype = "regex") %>%
  tokens_ngrams(n = 2, concatenator = "") %>%
  dfm()

# Calculate similarties -> create candidate pairs -------------------------

bigrams_sims <- calc_similarity(bigrams, method = "cosine", min_sim = 0.5)

bigrams_sims_with_names <- bigrams_sims %>% 
  # add plain strings
  left_join(bigrams_authors, by = c("id_1" = "id")) %>% 
  left_join(bigrams_authors, by = c("id_2" = "id"), suffix = c("_1", "_2"))

bigrams_sims_with_names %>% count(id_1, sort = T) %>% 
  left_join(bigrams_sims_with_names) %>% View

# Write in DB -------------------------------------------------------------

con <- connect_db()
DBI::dbWriteTable()
DBI::dbDisconnect(con); rm(con)


