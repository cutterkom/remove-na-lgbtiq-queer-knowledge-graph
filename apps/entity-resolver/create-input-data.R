# title: Create input data for entity resolver
# desc: fetch relevant data from db, incl. additional information about entities
# input: DB tbl matching_candidates_authors_books_posters and posters_authors
# output: Rdata

library(tidyverse)
library(kabrutils)

# get data
con <- connect_db()
candidates <- tbl(con, "er_candidates") %>% 
  collect()

# Get id's in order to fetch addintional infos ----------------------------

poster_author_ids <- candidates %>% 
  filter(source_1 == "poster_author" | source_2 == "poster_author") %>% 
  select(id_1, id_2) %>% 
  pivot_longer(cols = c(id_1, id_2), names_to = "id_seq", values_to = "id") %>% 
  distinct(id) %>% 
  pull()

book_author_ids <- candidates %>% 
  filter(source_1 == "book_author" | source_2 == "book_author") %>% 
  select(id_1, id_2) %>% 
  pivot_longer(cols = c(id_1, id_2), names_to = "id_seq", values_to = "id") %>% 
  distinct(id) %>% 
  pull()

book_publisher_ids <- candidates %>% 
  filter(source_1 == "book_publisher" | source_2 == "book_publisher") %>% 
  select(id_1, id_2) %>% 
  pivot_longer(cols = c(id_1, id_2), names_to = "id_seq", values_to = "id") %>% 
  distinct(id) %>% 
  pull()


# Get additional infos ----------------------------------------------------

posters_wide <- tbl(con, "posters_wide") %>% 
  filter(author_id %in% poster_author_ids) %>% 
  distinct(author_id, author) %>% 
  collect()

books_wide <- tbl(con, "books_wide") %>% 
  filter(author_id %in% book_author_ids) %>% 
  collect()

publisher_names <- tbl(con, "books_wide") %>% 
  filter(publisher_id %in% book_publisher_ids) %>% 
  distinct(publisher_id, publisher) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)

# Export data -------------------------------------------------------------

candidates <- list(
  "similarities" = candidates %>% mutate(across(everything(), as.character)),
  "books_additional_infos" = books_wide %>% mutate(across(everything(), as.character)),
  "poster_additional_infos" = posters_wide %>% mutate(across(everything(), as.character)),
  "books_publisher_additional_infos" = publisher_names %>% mutate(across(everything(), as.character)))

saveRDS(candidates, file = "apps/entity-resolver/data/candidates.Rds")

save(candidates, file = "apps/entity-resolver/data/candidates-authors.Rdata")
