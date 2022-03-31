# title: Search for queer publishers on Wikidata
# desc: A first try to link data of the internal database to Wikidata. Done by searching for as queer labelled publishers from internal database on wikidata and keep only those that are marked as publisher/Verlag https://www.wikidata.org/wiki/Q1320047
# input: entities_per_type, queere_verlage_elke_labels.xlsx (labeld by expert)
# output: 



library(tidyverse)
library(kabrutils)
library(tidywikidatar)
library(cli)

con <- connect_db(credential_name = "db_clean")
publishers <- tbl(con, "entities_per_type") %>% 
  filter(!is.na(publisher_book_id)) %>% 
  distinct(id, name) %>% 
  left_join(tbl(con, "entities") %>% 
              filter(person == 0), by = c("id", "name")) %>% 
  select(id, name) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)


queer_publishers <- readxl::read_excel("data-gathering/data/input/queere_verlage_elke_labels.xlsx") %>% 
  janitor::clean_names() %>% 
  filter(!is.na(queerer_verlag_ja_oder_nein)) %>% 
  select(publisher, queerer_verlag_ja_oder_nein) %>% 
  mutate(name_small = tolower(publisher))



# Join them ---------------------------------------------------------------

queer_publishers_data <- publishers %>% 
  mutate(name_small = tolower(name)) %>% 
  inner_join(queer_publishers, by = "name_small") %>% 
  select(id, name, queerer_verlag_ja_oder_nein)

anti_join(queer_publishers, queer_publishers_data)



# Search them on Wikidata and keep if it's a publisher --------------------

res <- queer_publishers_data %>%
  pmap_dfr(function(...) {
    current <- tibble(...)
    cli_alert_info(current$name)
    search_res <- tw_search(search = current$name, include_search = TRUE)
    
    if(!is.na(search_res$id)) {
      search_res %>% 
        bind_cols(internal_id = current$id) %>% 
        #tw_filter(p = "P31", q = "Q1320047") %>% 
        tw_filter(p = "P31", q = "Q41298")
        
        
    } else {
      search_res %>% 
        bind_cols(internal_id = current$id)
    }
  })


# Search an keep if publisher or magazine ---------------------------------

# e.g. Emma is a magazine
res <- queer_publishers_data %>%
  pmap_dfr(function(...) {
    current <- tibble(...)
    cli_alert_info(current$name)
    search_res <- tw_search(search = current$name, include_search = TRUE)

    if(!is.na(search_res$id)) {
      qids <- tw_get_property(id = search_res$id, p = c("P31"))
      
      search_res <- search_res %>% 
        left_join(qids, by = "id") %>% 
        rename(publisher_or_magazin = value) %>% 
        filter(publisher_or_magazin %in% c("Q1320047", "Q41298"))

    } else {
      search_res %>% 
        bind_cols(internal_id = current$id)
    }
  }) %>% 
  distinct()
