library(tidyverse)
library(kabrutils)


con <- connect_db()
books_authors <- tbl(con, "books_authors") %>%
  collect()
posters_authors <- tbl(con, "posters_authors") %>%
  collect()
DBI::dbDisconnect(con); rm(con)


books_authors
posters_authors %>% clean_string

all_authors <- bind_rows(
  posters_authors %>% mutate(source = "posters"),
  books_authors %>% mutate(source = "books")
)

all_authors %>% count(author, sort = T) %>% View

# nur sehr sehr sehr wenige, die gleich übereinstimmen
