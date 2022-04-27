library(tidyverse)
library(kabrutils)
library(tidywikidatar)


con <- connect_db("db_clean")
entities <- tbl(con, "entities") %>% 
  select(entity_id = id, name) %>% 
  collect()

chronik_entities <- tbl(con, "chronik_entities") %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  select(id, external_id, external_id_label, external_id_type) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)

con <- connect_db()
chronik_db <- tbl(con, "text_chronik") %>% 
  select(-contains("_at")) %>% 
  collect()
DBI::dbDisconnect(con); rm(con)



# Filter entities to search for -------------------------------------------

chronik_entities %>% 
  count(label, sort=T)

chronik_entities_to_find <- chronik_entities %>% 
  filter(!str_detect(label, "DATE"), !label %in% c("ADR", "CITY", "COUNTRY", "Slogan", "DISTRICT"))

chronik_entities_to_find %>% 
  count(label, sort=T)


# Persons -----------------------------------------------------------------

verbose <- F

persons <- chronik_entities_to_find %>% 
  filter(label == "PER") %>% 
  distinct(label, name) %>% 
  rename(cat = label) %>% 
  #head(10) %>% 
  pmap_dfr(function(...) {
    current <- tibble(...)
    
    if (verbose == TRUE) print(current$name)
    
    res <- tw_search(search = current$name, language = "de")
    
    if (res %>% filter(!is.na(id)) %>% nrow() > 0) {
      if (verbose == TRUE) print(res)
      res <- res %>% 
        tw_filter_first(p = "P31", q = "Q5")
    } else {
      res
    }
    
    data <- current %>% bind_cols(res)
    data
  })
    
  


# join  -------------------------------------------------------------------

chronik <- chronik_db %>% 
  left_join(chronik_entities, by = "id") %>% 
  clean_string("name") %>% 
  left_join(entities %>% clean_string("name"), by = c("name")) %>% 
  left_join(el_matches, by = c("entity_id" = "id")) %>% 
  distinct()


chronik_entities_to_find <- chronik_entities_to_find %>% 
  distinct(label, name)

tw_search(search = "Lesbenfrühlingstreffen")





chronik <- chronik_db %>% 
  left_join(chronik_entities, by = "id") %>% 
  clean_string("name") %>% 
  left_join(entities %>% clean_string("name"), by = c("name")) %>% 
  left_join(el_matches, by = c("entity_id" = "id")) %>% 
  distinct()


# Analysis ----------------------------------------------------------------

chronik %>% count(label, sort = T) %>% View

chronik_per_org_entities <- chronik %>% 
  filter(label %in% c("PER", "ORG", "CLUB", "EVENT"))

chronik_per_org_entities %>% 
  filter(!is.na(entity_id)) %>% nrow() / nrow(chronik_per_org_entities) * 100 # 13,08%

chronik_per_org_entities %>% clean_string("name") %>% View
