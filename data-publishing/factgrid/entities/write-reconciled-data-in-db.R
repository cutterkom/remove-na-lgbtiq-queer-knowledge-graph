
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

