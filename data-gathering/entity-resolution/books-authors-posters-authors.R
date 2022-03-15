# title: Calculate Similarities for Human Eyes
# desc: In order to find duplicates, the cosine similarities between bigrams on character level (shingles) is calculated and above 0.75 saved in DB. There they wait for a human judgement.
# input: DB tbl books_authors and posters_authors
# output: DB tbl er_candidates

library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)
library(DBI)
library(config)
library(testdat)

source("R/deduplication-matching.R")
source("R/utils.R")
source("R/entity-cleaning.R")


# Get data ----------------------------------------------------------------

con <- connect_db()
books_authors <- tbl(con, "books_authors") %>%
  collect() %>% 
  mutate(source = "book",
         id = paste(source, author_id, sep = "_"))
posters_authors <- tbl(con, "posters_authors") %>%
  collect() %>% 
  mutate(source = "poster",
         id = paste(source, author_id, sep = "_"))
dbDisconnect(con); rm(con)

authors <- bind_rows(books_authors, posters_authors)


# Get string mappings -----------------------------------------------------

# used to remove authors who seem to be organisations
string_mappings <- get(file = "static/string-mapping.yml") 

bigrams_authors <- authors %>% 
  select(id, name = author) %>% 
  clean_string(col = "name") %>% 
  filter(
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

candidates <- calc_similarity(bigrams, method = "cosine", min_sim = 0.75) %>% 
  left_join(authors %>% select(-author), by = c("id_1" = "id")) %>% 
  left_join(authors %>% select(-author), by = c("id_2" = "id"), suffix = c("_1", "_2")) %>% 
  select(id_1 = author_id_1, id_2 = author_id_2, source_1, source_2, value, rank)

# Write in DB -------------------------------------------------------------

import <- candidates %>% 
  mutate(entities = "authors_books_posters", .before = id_1)

test_that(
  desc = "uniqueness",
  expect_unique(c("id_1", "id_2", "source_1", "source_2"), data = import)
)

create_table <- "
CREATE TABLE IF NOT EXISTS `er_candidates` (
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `entities` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `source_1` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `id_1` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `source_2` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `id_2` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `value` float DEFAULT NULL,
  `rank` bigint(20) DEFAULT NULL,
  `decision` varchar(1501) COLLATE utf8mb3_german2_ci NOT NULL DEFAULT 'no_judgement',
  `keep_label_2` int(1) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  PRIMARY KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci COMMENT='Table to store calculation of similarities between entities. Afterwards there needs to be a human decision made. Decisions are written in this database.';
"

con <- connect_db()
dbExecute(con, create_table)
dbAppendTable(con, "er_candidates", import)
dbDisconnect(con); rm(con)
