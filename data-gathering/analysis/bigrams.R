# Ngrams based blocks
# 2 grams -> messier names data

library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)
library(DBI)
library(config)

source("R/deduplication-matching.R")
source("R/utils.R")
source("R/entity-cleaning.R")


# Get data ----------------------------------------------------------------

con <- connect_db()
books_authors <- tbl(con, "books_authors") %>%
  collect() %>% 
  mutate(author_id = paste0("books_", author_id))
posters_authors <- tbl(con, "posters_authors") %>%
  collect() %>% 
  mutate(author_id = paste0("poster_", author_id))
dbDisconnect(con); rm(con)

authors <- bind_rows(books_authors, posters_authors)


# Get string mappings -----------------------------------------------------

# used to remove authors who seem to be organisations
string_mappings <- get(file = "static/string-mapping.yml") 

bigrams_authors <- authors %>% 
  select(id = author_id, name = author) %>% 
  #split_human_name(col = "name") %>% 
  clean_string(col = "name") %>% 
  filter(
    # remove Verlage and Vereine
    # !str_detect(tolower(name), paste0(string_mappings$organisation, collapse = "|")),
    # remove ? 
    !str_detect(tolower(name), paste0(string_mappings$nonsense, collapse = "|")),
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

# Calculate similarities -> create candidate pairs -------------------------

bigrams_sims <- calc_similarity(bigrams, method = "cosine", min_sim = 0.65)

bigrams_sims_with_names <- bigrams_sims %>% 
  # add plain strings
  left_join(bigrams_authors, by = c("id_1" = "id")) %>% 
  left_join(bigrams_authors, by = c("id_2" = "id"), suffix = c("_1", "_2"))

# bigrams_sims_with_names %>% count(id_1, sort = T) %>% 
#   left_join(bigrams_sims_with_names) %>% View

# Write in DB -------------------------------------------------------------

create_table <- "
CREATE TABLE IF NOT EXISTS `matching_authors_books_posters` (
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `id_1` varchar(50) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  `id_2` varchar(50) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  `value` float DEFAULT NULL,
  `rank` bigint DEFAULT NULL,
  `decision` varchar(1501) COLLATE utf8mb3_german2_ci NOT NULL DEFAULT 'no_judgement',
  PRIMARY KEY `id_1` (`id_1`,`id_2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci COMMENT='Table to store calculation of similarities between entities. Afterwards there needs to be a human decision made. Decisions are written in this database.';
"

con <- connect_db()
# create db tbl if not exists
dbExecute(con, create_table)
dbAppendTable(con, "matching_authors_books_posters", bigrams_sims)
dbExecute(con, sort_table)
dbDisconnect(con); rm(con)


