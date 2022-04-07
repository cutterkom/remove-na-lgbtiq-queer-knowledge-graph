library(tidyverse)
library(kabrutils)

con <- connect_db("db_clean")
entities <- tbl(con, "entities")  %>% collect()
DBI::dbDisconnect(con); rm(con)
con <- connect_db()

el_matches <- tbl(con, "el_matches") %>% 
              filter(entity_id_type == "entities") %>% 
              distinct(entity_id, external_id, external_id_label) %>% 
  collect()

el_viaf_books_authors <- tbl(con, "el_viaf_books_authors") %>% collect() %>% select(-contains("_at"))
DBI::dbDisconnect(con); rm(con)



entities_gnd_id <- entities %>% 
  left_join(el_matches %>% filter(!is.na(external_id)), by = c("id" = "entity_id")) %>% 
  left_join(el_viaf_books_authors %>% 
              mutate(id = paste0("book_author_", author_id)), by = "id") %>% 
  mutate(
    has_gnd_id = ifelse(!is.na(external_id), 1, 0),
    has_gnd_id_viaf = ifelse(!is.na(gnd_id), 1, 0),
    has_any_gnd_id = ifelse(has_gnd_id == 1 | has_gnd_id_viaf == 1, 1, 0))


entities_gnd_id %>% count(has_gnd_id)
entities_gnd_id %>% count(has_any_gnd_id)


entities_gnd_id %>% filter(has_any_gnd_id == 0) %>% View
entities_gnd_id %>% filter(has_any_gnd_id == 1) %>% View
