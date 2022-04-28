library(tidyverse)
library(kabrutils)
library(config)
library(testdat)
library(DBI)

verbose <- F

con <- connect_db()

entities_raw <- tbl(con, "chronik_entities") %>% 
  collect() %>% 
  mutate(
    name = 
      case_when(
        name == "Münchens" ~ "München",
        name == "Augspurg" ~ "Anita Augspurg",
        TRUE ~ name
      )
  )

er_candiates_raw <- tbl(con, "er_candidates") %>% 
  rename(cand_id = id) %>% 
  collect()

er_candidates <- er_candiates_raw %>% 
  filter(source_1 == "chronik", source_2 == "chronik") %>% 
  filter(decision == "positive") %>% 
  select(contains("source"), contains("id"), decision, contains("label")) %>% 
  group_by(id_1, id_2) %>% 
  filter(row_number() == 1) %>% 
  ungroup()

DBI::dbDisconnect(con); rm(con)

# Resolve manual decisions and create a new ID for those ------------------

new_ids_raw <- er_candidates %>%
  filter(decision == "positive") %>% 
  #head(1) %>% 
  pmap_dfr(function(...) {
    
    current <- tibble(...)
    
    if (verbose == TRUE) {
      message("current data:")
      print(current)
    }
    
    candidate_1 <- entities_raw %>% 
      inner_join(current, by = c("id" = "id_1")) %>% 
      mutate(id_new = paste0("er_", cand_id)) %>% 
      #select(id, id_new, name_1 = name, keep_label_2, new_label)
      select(id, cand_id, name_1 = name, keep_label_2, new_label)
    
    if (verbose == TRUE) {
      message("candidate_1:")
      print(candidate_1)
    }
    
    candidate_2 <- entities_raw %>% 
      inner_join(current, by = c("id" = "id_2")) %>% 
      #mutate(id_new = paste0("er_", cand_id)) %>% 
      #select(id, id_new, name_2 = name, keep_label_2, new_label)
      select(id, cand_id, name_2 = name, keep_label_2, new_label)
    
    if (verbose == TRUE) {
      message("candidate_2:")
      print(candidate_2)
    }
    
    er_pair <- candidate_1 %>% 
      #left_join(candidate_2, by = c("id", "keep_label_2", "new_label"), suffix = c("_1", "_2")) %>% 
      left_join(candidate_2, by = c("cand_id", "keep_label_2", "new_label"), suffix = c("_1", "_2")) %>% 
      mutate(
        name = 
          case_when(
            keep_label_2 == 1 ~ name_2,
            !is.na(new_label) ~ new_label,
            TRUE ~ name_1
          ),
        id = paste0("er_", cand_id)
      ) %>% 
      filter(!is.na(name)) %>% 
      select(-cand_id) %>% 
      select(id, everything())
    
    if (verbose == TRUE) {
      message("er_pair:")
      print(er_pair)
    }
    
    er_pair
    
  })


# Save mapping of old to new ID -------------------------------------------

id_mappings <- new_ids_raw %>% select(id_new = id, id_1, id_2)


# minor tweaks in new_ids df ----------------------------------------------

new_ids <- new_ids_raw %>% 
  select(id, name) %>% 
  mutate(
    name = case_when(
      str_detect(name, "forum") ~ "Forum Queeres Archiv München e.V.",
      name == "Münchens" ~ "München",
      TRUE ~ name)
  )


# Unfortunately there are still duplicates in the ER'ed data ---------------

# e.g. when appears multiple times in multiple comparisons
# counting when names appear more than once, then keep first row

duplicate_new_ids <- new_ids %>% 
  inner_join(new_ids %>% 
               count(name, sort = T) %>% 
               filter(n>1), by = "name") %>% 
  group_by(name) %>% 
  mutate(
    id_new = first(id)
  ) %>% 
  ungroup() %>%
  select(id_new, id_old = id, name)

duplicate_new_ids_for_mapping <- duplicate_new_ids %>% 
  filter(id_new != id_old) %>% 
  select(id_new, er_old = id_old)
  

duplicate_new_ids_for_mapping
# Add to ID mapping -------------------------------------------------------

id_mappings_step_2 <- bind_rows(
  id_mappings, 
  
  # map id (incl. "old" er_id's) and add to mappings df
  new_ids %>% 
    left_join(duplicate_new_ids_for_mapping, by = c("id" = "id_new")) %>%
    select(id_new = id, er_old) %>% 
    distinct() %>% 
    filter(!is.na(id_new))
) %>% 
  filter(!is.na(id_1) | !is.na(id_2) | !is.na(er_old))

# add de-deduplicated IDs to df with all new entities ---------------------

new_ids_final <- bind_rows(
  new_ids %>% anti_join(new_ids %>% 
                          count(name, sort = T) %>% 
                          filter(n>1), by = "name"),
  duplicate_new_ids %>% select(id = id_new, name)
) %>% 
  distinct()

# Add to other entities ---------------------------------------------------

# in order to create proper id's I first have to remove the new ER'ed id from entities df.

remove_from_entities <- new_ids_final %>% 
  mutate(er_id_to_join = as.numeric(str_remove(id, "er_"))) %>% 
  left_join(er_candidates, by = c("er_id_to_join" = "cand_id")) %>% 
  select(id, source_1, id_1, source_2, id_2)

remove_from_entities <- bind_rows(
  remove_from_entities %>% select(source = source_1, id = id_1),
  remove_from_entities %>% select(source = source_2, id = id_2)) 


entities <- entities_raw %>% 
  anti_join(remove_from_entities, by = c("id")) %>% 
  select(id, name) %>% 
  bind_rows(new_ids_final) %>% 
  distinct()


# There are still some duplicates, removed here --------------------------

# beware: there are cases, when there are more than 2 duplicate values
# pivot wider adds as many cols as there are duplicates. 

duplicate_entities <- entities %>%
  inner_join(entities %>%
               count(name, sort = T) %>%
               filter(n > 1) %>%
               filter(!name %in% c("Forum")), by = "name") %>%
  select(-n) %>%
  group_by(name) %>%
  # arrange so that er_ is at first and will be kept
  arrange(desc(id)) %>%  
  mutate(rowid = row_number()) %>% 
  ungroup() %>%
  pivot_wider(id_col = name, names_from = rowid, values_from = id) %>% 
  rename(id_old = `1`, id_new = `2`) %>% 
  mutate(
    id_new =
      case_when(
        !is.na(`4`) ~ `4`,
        !is.na(`3`) ~ `3`,
        TRUE ~ id_new
      )
  ) %>%
  select(name, id_new = id_old, id_old = id_new)


# Same removed ones in mapping df -----------------------------------------

id_mappings_step_3 <- bind_rows(
  id_mappings_step_2, 
  duplicate_entities %>% select(id_new, id_1 = id_old)
  )

# Add de-duduplicated to entities df --------------------------------------


entities_final <- bind_rows(
  entities %>% anti_join(entities %>% 
                           count(name, sort = T) %>% 
                           filter(n>1), by = "name"),
  duplicate_entities %>% select(id = id_new, name)
)


# Extract orgs etc --------------------------------------------------------

import <- entities_final %>% 
  left_join(entities_raw, by = "name") %>% 
  mutate(
    org = 
      case_when(
        label == "ORG" ~ 1,
        TRUE ~ 0
      ),
    club = 
      case_when(
        label == "CLUB" ~ 1,
        TRUE ~ 0
      ),
    person = 
      case_when(
        label == "PER" ~ 1,
        TRUE ~ 0
      ),
    location = 
      case_when(
        label == "LOC" ~ 1,
        TRUE ~ 0
      ),
    city = 
      case_when(
        label == "CITY" ~ 1,
        TRUE ~ 0
      ),
    country = 
      case_when(
        label == "COUNTRY" ~ 1,
        TRUE ~ 0
      ),
    address = 
      case_when(
        label == "ADR" ~ 1,
        TRUE ~ 0
      ),
    event = 
      case_when(
        label == "EVENT" ~ 1,
        TRUE ~ 0
      ),
    slogan = 
      case_when(
        label == "Slogan" ~ 1,
        TRUE ~ 0
      ),
    publication = 
      case_when(
        label == "PUBLICATION" ~ 1,
        TRUE ~ 0
      ),
    party = 
      case_when(
        label == "PARTY" ~ 1,
        TRUE ~ 0
      ),
    law = 
      case_when(
        label == "LAW" ~ 1,
        TRUE ~ 0
      ),
    movement = 
      case_when(
        label == "MOVEMENT" ~ 1,
        TRUE ~ 0
      ),
    district = 
      case_when(
        label == "DISTRICT" ~ 1,
        TRUE ~ 0
      ),
    award = 
      case_when(
        label == "AWARD" ~ 1,
        TRUE ~ 0
      ),
  ) %>% 
  filter(!str_detect(label, "DATE")) %>% 
  distinct() %>% 
  select(-id.y, -chronik_entry_id, -label) %>% 
  rename(id =  id.x) %>% 
  mutate(
    # München
    location = 
      case_when(
        id == "chronik_63" ~ 0, 
        id == "chronik_672" ~ 0,
        id == "chronik_662" ~ 1, 
        TRUE ~ location),
    city = case_when(
      id == "chronik_63" ~ 1, 
      id == "chronik_672" ~ 1,
      TRUE ~ city),
    
    club = case_when(
      id == "chronik_662" ~ 1, 
      TRUE ~ club),
    
    org = case_when(
      id == "chronik_662" ~ 1, 
      club == 1 ~ 1,
      name == "VSG" ~ 1,
      TRUE ~ org
    )
  ) %>% 
  distinct() %>% 
  # can happen that one entity has multiple labels, no keep all 1 (max)
  group_by(name, id) %>% 
  summarise_all(max)

testthat::test_that(
  desc = "all unique",
  expect_unique(c("id", "name"), data = import)
)

import %>% View()

con <- connect_db(credential_name = "db_clean")
DBI::dbAppendTable(con, "entities", import)
DBI::dbDisconnect(con); rm(con)


# Save mappings table -----------------------------------------------------

import <- id_mappings_step_3

con <- connect_db(credential_name = "db_clean")
DBI::dbAppendTable(con, "id_mapping", import)
DBI::dbDisconnect(con); rm(con)

