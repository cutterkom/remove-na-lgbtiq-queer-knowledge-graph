
library(rrefine)


# locations ---------------------------------------------------------------

import <- refine_export("entities_locations") %>% select(id, wikidata, factgrid) %>% 
  pivot_longer(cols = c("wikidata", "factgrid"), names_to = "external_id_type", values_to = "external_id") %>% 
  filter(!is.na(external_id)) %>% 
  mutate(
    entity_id_type = "entities",
    source = "open-refine-reconciliation", 
    hierarchy = 1)


import

con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)


# Persons -----------------------------------------------------------------


import <- refine_export("entities_persons_with_ids") %>% select(id, wikidata_new) %>% 
  pivot_longer(cols = c("wikidata_new"), names_to = "external_id_type", values_to = "external_id") %>% 
  filter(!is.na(external_id)) %>% 
  mutate(
    external_id_type = ifelse(external_id_type == "wikidata_new", "wikidata", external_id_type),
    entity_id_type = "entities",
    source = "open-refine-reconciliation", 
    hierarchy = 1)

import

con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)



# Factgrid ----------------------------------------------------------------


import <- refine_export("entities_persons_with_ids") %>% 
  select(id, factgrid) %>% 
  pivot_longer(cols = c("factgrid"), names_to = "external_id_type", values_to = "external_id") %>% 
  filter(!is.na(external_id)) %>% 
  mutate(
    entity_id_type = "entities",
    source = "open-refine-reconciliation", 
    hierarchy = 1)

import

con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)



# lobid -------------------------------------------------------------------

# some more decisions needed
con <- connect_db("db_clean")

entities_raw <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  filter(external_id_type == "factgrid") %>% 
  #distinct(id, external_id) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)

lobid_no_gnd <- refine_export("entities_persons_with_ids") %>% 
  select(id, lobid, gnd) %>% 
  filter(!is.na(lobid), is.na(gnd)) %>% 
  left_join(entities_raw %>% distinct(id, name), by = "id")

lobid_no_gnd %>% filter(str_detect(id, "^er_|chronik")) 
# alle grpüft mit er_, und chronik

# stichprobenartige prüfungen insb. bei auffälligen oder auffällig unauffälligen Namen
# 90 % der Fälle scheinen korrekt zu sein
lobid_no_gnd %>% 
  filter(!str_detect(id, "^er_|chronik")) %>% 
  left_join(el_matches)

import <- lobid_no_gnd %>% 
  select(id, lobid) %>% 
  pivot_longer(cols = c("lobid"), names_to = "external_id_type", values_to = "external_id") %>%
  filter(!is.na(external_id)) %>%
  mutate(
    entity_id_type = "entities",
    source = "open-refine-reconciliation-stichproben-gecheckt",
    hierarchy = 2)

import

con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)
