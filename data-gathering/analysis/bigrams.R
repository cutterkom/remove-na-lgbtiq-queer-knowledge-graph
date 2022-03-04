# Ngrams based blocks
# 2 grams -> messier names data

library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)

source("R/deduplication-matching.R")
source("R/utils.R")
source("R/entity-cleaning.R")

con <- connect_db()
authors <- tbl(con, "books_authors") %>%
  collect()
DBI::dbDisconnect(con); rm(con)


bigrams_authors <- authors %>% 
  select(id = author_id, author) %>% 
  mutate(id = paste0("books_", id)) %>% 
  split_human_name(name_col = "author") %>% 
  filter(
    # remove Verlage and Vereine
    !str_detect(tolower(author), "verlag|e.v.|kollektiv"),
    # remove ? 
    !author %in% c("?", "et al."),
    # remove if there is no space in the name -> just 1 name or abbreviation
    str_detect(author, "\\s")
  )

# Prepare text data -------------------------------------------------------

corp <- corpus(bigrams_authors, text_field = "author", docid_field = "id")
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
