# title: Save deduplicated entities to new database lgbtiq_kg_clean
# desc: In order to keep different thing separatly, I create a new database that contains only cleaned data.
# input: lgbtiq_kg_clean.entities, lgbtiq_kg.lgbtiq_kg.chronik_entities
# output: lgbtiq_kg_clean.chronik_entities

library(tidyverse)
library(kabrutils)
library(testdat)
library(DBI)


# Get deduplicated entities -----------------------------------------------

con <- connect_db(credential_name = "db_clean")
entities <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

# clean forum


forum_ids <- entities %>% 
  filter(str_detect(tolower(name), "forum homo|forum queer"), id != "chronik_986") %>% 
  pull(id) %>% 
  unique()

entities <- entities %>% 
  mutate(
    id = case_when(
      id %in% forum_ids ~ "er_390",
      TRUE ~ id
    ),
    name = case_when(
      id %in% forum_ids ~ "Forum Queeres Archiv München e.V.",
      TRUE ~ name
    )
  ) %>% 
  distinct()


id_mapping <- tbl(con, "id_mapping") %>% 
  select(-contains("_at")) %>% 
  collect()
DBI::dbDisconnect(con); rm(con)

# Get authors and publishers ----------------------------------------------

con <- connect_db()

chronik_entities_raw <- tbl(con, "chronik_entities") %>% 
  collect() %>% 
  mutate(
    name = 
      case_when(
        name == "Münchens" ~ "München",
        name == "Augspurg" ~ "Anita Augspurg",
        name == "Hitlers" ~ "Adolf Hitler",
        name == "Röhm" ~ "Ernst Röhm",
        name == "Magnus Hirschfelds" ~ "Magnus Hirschfeld",
        TRUE ~ name
      ),
    
  )

dbDisconnect(con); rm(con)



# Update IDs --------------------------------------------------------------

id_mapping_long <- id_mapping %>% 
  pivot_longer(cols = c(id_1, id_2, er_old), names_to = "id_type", values_to = "id_old")

#' update IDs depending on id_mapping
#' 
#' @param data entity df with id col
#' @param id_mapping longform df with cols `c(id_new, id_old)`
#' @return dataframe with updated IDs
update_ids <- function(data, mapping) {
  data %>% 
    left_join(mapping %>% 
                select(id_new, id_old), by = c("id" = "id_old")) %>% 
    mutate(
      id = case_when(
        !is.na(id_new) ~ id_new,
        TRUE ~ id)
    )
}


chronik_entities <- update_ids(chronik_entities_raw, id_mapping_long) %>% 
  select(-id_new) %>% 
  distinct() %>% 
  mutate(
    id = ifelse(id == "chronik_550", "er_1816", id),
    id = ifelse(id == "chronik_546", "er_1840", id),
    id = ifelse(id == "chronik_695", "chronik_99", id),
    id = ifelse(id == "chronik_456", "chronik_452", id),
    id = ifelse(id == "chronik_695", "chronik_99", id),
    id = ifelse(id == "chronik_695", "chronik_99", id),
    id = ifelse(id == "chronik_695", "chronik_99", id),
    id = ifelse(id == "chronik_414", "er_390", id),
    id = ifelse(id == "chronik_959", "er_390", id),
    id = ifelse(id == "chronik_984", "er_390", id),
    id = ifelse(id == "chronik_718", "er_390", id),
    id = ifelse(id == "chronik_739", "book_author_726", id),
    id = ifelse(id == "er_1881", "er_390", id),
    
    )
  


# # Update IDs 2nd round ----------------------------------------------------
# 
# # This process needs to be done again, because some er_ids needed to be de-duplicated in a two-step-process
# 
# 
id_mapping_long_2nd_round <- id_mapping_long %>%
  filter(id_type == "er_old") %>%
  filter(!is.na(id_old)) %>%
  distinct(id_new, id_old)

chronik_entities_import <- update_ids(chronik_entities, id_mapping_long_2nd_round) %>% 
  select(-name, -id_new) %>%
  distinct() %>%
  left_join(entities %>% select(id, name), by = "id") %>%
  select(-name)

# Get Chronik Text --------------------------------------------------------

con <- connect_db()
chronik_text <- tbl(con, "text_chronik") %>% 
  select(-contains("_at")) %>% 
  collect()
DBI::dbDisconnect(con); rm(con)

import <- chronik_text %>% 
  rename(chronik_entry_id = id) %>% 
  left_join(chronik_entities_import, by = c("chronik_entry_id")) %>% 
  left_join(entities %>% distinct(name, id), by = "id") %>% 
  left_join(chronik_entities_raw %>% distinct(name, id), by = "id") %>% 
  mutate(name = 
           case_when(
             !is.na(name.x) ~ name.x,
             !is.na(name.y) ~ name.y,
             TRUE ~ NA_character_
           )) %>% 
  select(-name.x, -name.y)

fill_na_gaps <- import %>% 
  filter(is.na(name)) %>% 
  select(chronik_entry_id, id, label) %>% 
  left_join(chronik_entities_raw, by = c("chronik_entry_id", "label")) %>% 
  mutate(id = id.y) %>% 
  select(-id.x, -id.y) %>% 
  distinct()


import <- import %>% 
  left_join(fill_na_gaps, by = c("chronik_entry_id", "label"), suffix = c("", "_na")) %>% 
  mutate(
    name = 
      case_when(
        is.na(name) ~ name_na,
        TRUE ~ name
      ),
    id = 
      case_when(
        is.na(name) ~ id_na,
        TRUE ~ id
      )
  ) %>% 
  select(-name_na, -id_na) %>% 
  distinct()

test_that(
  desc = "uniqueness of id-name-combination",
  expect_unique(c("id", "name"), data = import %>% distinct(id, name))
)

# Write in DB -------------------------------------------------------------
con <- connect_db(credential_name = "db_clean")
DBI::dbWriteTable(con, "chronik", import)
DBI::dbDisconnect(con); rm(con)

