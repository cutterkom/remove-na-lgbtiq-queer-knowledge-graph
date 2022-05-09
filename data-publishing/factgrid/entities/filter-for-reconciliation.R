

library(tidyverse)
library(kabrutils)
library(testdat)


# Some config ------------------------------------------------------------


# Get data ----------------------------------------------------------------

con <- connect_db("db_clean")

entities_raw <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  select(id, contains("external"), source) %>% 
  distinct() %>% 
  collect()

DBI::dbDisconnect(con); rm(con)


# Hierarchies  ------------------------------------------------------------

# there are different levels of data

el_matches %>% count(external_id_type, source, sort = T)

el_matches %>% 
  count(id, external_id_type, sort = T) %>% 
  left_join(entities_raw %>% select(id, name)) %>% 
  left_join(el_matches) %>% View


el_matches %>% 
  count(id, external_id_type, sort = T) %>% 
  filter(n>1) %>% 
  left_join(entities_raw %>% select(id, name)) %>% 
  left_join(el_matches %>% distinct(id, external_id, external_id_label)) %>% View



# Who has Wikidata? -------------------------------------------------------

wikidata <- el_matches %>% filter(external_id_type == "wikidata", external_id != "no_wikidata_id")
gnd <- el_matches %>% filter(external_id_type == "gnd")
  
testthat::test_that(
  desc = "unique",
  expect_unique(c("id", "external_id"), data = wikidata)
)

entities_raw %>% filter(person == 1) %>% inner_join(wikidata, by = "id") %>% View
entities_raw %>% filter(person == 1) %>% anti_join(wikidata, by = "id") %>% View


# Persons with corresponding IDs ------------------------------------------

entities_raw %>% 
  distinct(id, name, person) %>% 
  filter(person == 1) %>% 
  left_join(gnd %>% distinct(id, external_id) %>% rename(gnd = external_id), by = "id") %>% 
  left_join(wikidata %>% distinct(id, external_id) %>% rename(wikidata = external_id), by = "id") %>% 
  mutate(wikidata = str_remove(wikidata, "http://www.wikidata.org/entity/")) %>% View
  select(-person) %>% 
  clipr::write_clip()

# resulted in Open Refine project "entities_persons_with_ids"

# Get most secure entity links --------------------------------------------
  
entities_raw %>% 
  distinct(id, name, org, club) %>% 
  filter(org == 1, club == 1) 

# hierarchy_id means: very secure, e.g. author search via isbn
