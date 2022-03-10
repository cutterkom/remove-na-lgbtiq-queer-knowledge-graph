# title: Create input data for entity resolver
# desc: fetch relevant data from db, incl. additional information about entities
# input: DB tbl matching_candidates_authors_books_posters and posters_authors
# output: Rdata

library(tidyverse)
library(kabrutils)

# get data
con <- connect_db()
candidates <- tbl(con, "matching_candidates_authors_books_posters") %>% 
  collect()

# Get id's in order to fetch addintional infos ----------------------------

poster_author_ids <- candidates %>% 
  filter(source_1 == "poster" | source_2 == "poster") %>% 
  select(id_1, id_2) %>% 
  pivot_longer(cols = c(id_1, id_2), names_to = "id_seq", values_to = "id") %>% 
  distinct(id) %>% 
  pull()

book_author_ids <- candidates %>% 
  filter(source_1 == "book" | source_2 == "book") %>% 
  select(id_1, id_2) %>% 
  pivot_longer(cols = c(id_1, id_2), names_to = "id_seq", values_to = "id") %>% 
  distinct(id) %>% 
  pull()


# Get additional infos ----------------------------------------------------

posters_wide <- tbl(con, "posters_wide") %>% 
  distinct(author_id, author) %>% 
  filter(author_id %in% poster_author_ids) %>% 
  collect()

books_wide <- tbl(con, "books_wide") %>% 
  filter(author_id %in% book_author_ids) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)

# Export data -------------------------------------------------------------

candidates <- list(
  "similarities" = candidates,
  "books_additional_infos" = books_wide,
  "poster_additional_infos" = posters_wide)

save(candidates, file = "apps/entity-resolver/data/candidates-authors.Rdata")
