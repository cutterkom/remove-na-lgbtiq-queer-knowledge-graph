# title: Calculate Similarities for Human Eyes
# desc: In order to find duplicates, the cosine similarities between bigrams on character level (shingles) is calculated and above 0.75 saved in DB. There they wait for a human judgement.
# input: DB tbl books_publishers and posters_authors
# output: DB tbl er_candidates

library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)
library(DBI)
library(config)
library(testdat)

min_sim <- 0.75


# Get data ----------------------------------------------------------------

con <- connect_db()
books_publishers <- tbl(con, "books_publishers") %>%
  collect() %>% 
  mutate(source = "book_publisher",
         id = paste(source, publisher_id, sep = "_")) %>% 
  rename(name = publisher,
         item_id = publisher_id)
posters_authors <- tbl(con, "posters_authors") %>%
  collect() %>% 
  mutate(source = "poster_author",
         id = paste(source, author_id, sep = "_")) %>% 
  rename(name = author,
         item_id = author_id)
dbDisconnect(con); rm(con)

entities <- bind_rows(books_publishers, posters_authors)


# Get string mappings -----------------------------------------------------

# used to remove authors who seem to be organisations
string_mappings <- get(file = "static/string-mapping.yml") 

bigrams_entities <- entities %>% 
  clean_string(col = "name") %>% 
  filter(
    !str_detect(tolower(name), paste0(string_mappings$nonsense, collapse = "|")),
    # remove if there is no space in the name -> just 1 name or abbreviation
    str_detect(name, "\\s")
  )

# Prepare text data -------------------------------------------------------

corp <- corpus(bigrams_entities, text_field = "name", docid_field = "id")
toks <- tokens(corp)

# create bigrams on word level = shingles
bigrams <- corp %>% 
  tokens(what = "character") %>%
  tokens_keep("[A-Za-z]", valuetype = "regex") %>%
  tokens_ngrams(n = 2, concatenator = "") %>%
  dfm()

# Calculate similarities -> create candidate pairs -------------------------

only_posters <- tibble(source_1 = "poster_author", source_2 = "poster_author")

candidates <- calc_similarity(bigrams, method = "cosine", min_sim = min_sim) %>% 
  left_join(entities %>% select(-name), by = c("id_1" = "id")) %>% 
  left_join(entities %>% select(-name), by = c("id_2" = "id"), suffix = c("_1", "_2")) %>% 
  select(id_1 = item_id_1, id_2 = item_id_2, source_1, source_2, value, rank) %>% 
  anti_join(only_posters, by = c("source_1", "source_2"))


# Get Names ---------------------------------------------------------------

# candidates_with_names <- candidates %>% 
#   mutate(
#     id_1 = 
#       case_when(
#         str_detect(source_1, "book") ~ paste0("book_publisher_", id_1),
#         str_detect(source_1, "poster") ~ paste0("poster_author_", id_1),
#         #source_1 == "poster" ~ paste0("poster_", id_1),
#         TRUE ~ "no_source_sth_wrong"
#       ),
#     id_2 = 
#       case_when(
#         str_detect(source_2, "book") ~ paste0("book_publisher_", id_2),
#         str_detect(source_2, "poster") ~ paste0("poster_author_", id_2),
#         TRUE ~ "no_source_sth_wrong"
#       )
#   ) %>% 
#   left_join(entities %>% select(id, name), by = c("id_1" = "id")) %>% 
#   left_join(entities %>% select(id, name), by = c("id_2" = "id"), suffix = c("_1", "_2"))


# Write in DB -------------------------------------------------------------

import <- candidates %>% 
  mutate(entities = "books_publishers_posters_authors", .before = id_1)

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
  `new_label` varchar(255) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  PRIMARY KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci COMMENT='Table to store calculation of similarities between entities. Afterwards there needs to be a human decision made. Decisions are written in this database.';
"

con <- connect_db()
dbExecute(con, create_table)
dbAppendTable(con, "er_candidates", import)
dbDisconnect(con); rm(con)
