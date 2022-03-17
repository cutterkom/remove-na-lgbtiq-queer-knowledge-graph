library(tidyverse)
library(kabrutils)


con <- connect_db()
er_candidates <- tbl(con, "er_candidates") %>% 
  filter(decision == "positive") %>% 
  collect()

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

books_authors <- tbl(con, "books_authors") %>%
  collect() %>% 
  mutate(source = "book_author",
         id = paste(source, author_id, sep = "_")) %>% 
  rename(name = author,
         item_id = author_id)


DBI::dbDisconnect(con); rm(con)

entities <- bind_rows(books_publishers, posters_authors, books_authors)



# Add names ---------------------------------------------------------------

candidates_with_names <- 
  er_candidates %>%
  mutate(
    id_1 =
      case_when(
        source_1 == "book_publisher" ~ paste0("book_publisher_", id_1),
        source_1 == "poster_author" ~ paste0("poster_author_", id_1),
        source_1 == "book_author" ~ paste0("book_author_", id_1),
        TRUE ~ "no_source_sth_wrong"
      ),
    id_2 =
      case_when(
        source_2 == "book_publisher" ~ paste0("book_publisher_", id_2),
        source_2 == "poster_author" ~ paste0("poster_author_", id_2),
        source_2 == "book_author" ~ paste0("book_author_", id_2),
        TRUE ~ "no_source_sth_wrong"
      )
  ) %>%
  left_join(entities %>% select(id, name), by = c("id_1" = "id")) %>%
  left_join(entities %>% select(id, name), by = c("id_2" = "id"), suffix = c("_1", "_2"))



# Export ------------------------------------------------------------------

candidates_with_names %>% 
  select(entities, source_1, source_2, name_1, name_2, similarity = value, keep_label_2, new_label) %>% 
  write_xlsx(., "linda_duplikate.xlsx", sheet = "duplikate")
