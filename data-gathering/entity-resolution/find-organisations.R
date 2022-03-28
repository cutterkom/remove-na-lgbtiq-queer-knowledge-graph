library(tidyverse)
library(kabrutils)

con <- connect_db()

books_authors <- tbl(con, "books_wide") %>% 
  select(contains("author")) %>% 
  filter(!is.na(author_id)) %>% 
  distinct() %>% 
  collect()

books_publishers <- tbl(con, "books_wide") %>% 
  select(contains("publisher")) %>% 
  filter(!is.na(publisher_id)) %>% 
  distinct() %>% 
  collect()

posters_authors <- tbl(con, "posters_wide") %>% 
  select(contains("author")) %>% 
  filter(!is.na(author_id)) %>% 
  distinct() %>% 
  collect()

er_candidates <- tbl(con, "er_candidates") %>% collect()

DBI::dbDisconnect(con); rm(con)

