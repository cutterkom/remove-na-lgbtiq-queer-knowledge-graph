# Goal: Find citavi authors in Wikidata


library(tidyverse)
library(kabrutils)
library(WikidataR)

# citavi book data in local saqlite
con <- connect_db()
authors <- tbl(con, "authors") %>% collect()
DBI::dbDisconnect(con); rm(con)

sample_authors <- sample_n(authors, 20)

# test with one
wd_item_list <- find_item(sample_authors$name[1])

wd_item <- wd_item_list %>% 
  transpose() %>%
  as_tibble() %>%
  unnest(cols = -match)
wd_item

# get all of sample
wd_sample_items <- map2_df(sample_authors$name, sample_authors$id, function(author, author_id) {
  wd_item_list <- find_item(author)
  wd_item <- wd_item_list %>% 
    transpose() %>%
    as_tibble() %>%
    mutate(author = author,
           author_id = as.integer(author_id)) %>% 
    select(author, author_id, everything())
})

# -> funktioniert grundsätzlich, aber zu fuzzy --> erst ISBN und darüber suchen


# ISBN suchen -------------------------------------------------------------
con <- DBI::dbConnect(RSQLite::SQLite(), "../forum-digital-archive/construct-knowledge-graph/data/db.db")
isbn <- tbl(con, "books") %>% collect()
DBI::dbDisconnect(con); rm(con)

sample_isbn <- isbn %>% 
  filter(!is.na(isbn)) %>% 
  sample_n(20)
sample_isbn$isbn

# not working! very slow und hängt isch auf get 1
# qid_from_identifier(property = "ISBN-13", "978-3-406-42101-3")
# hab isbn ohne hypentation --> conecert
# in python https://pypi.org/project/python-stdnum/




