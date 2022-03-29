# title: Create distinct entities from manual entity resolution
# desc: Takes authors and publishers and check, if they were in the manual entitiy resolution process. If so, then entitiy gets a new ID. Also, some first rough guesses are made if an entity is an organisation, club or person. 
# input: books_authors, book_publishers, posters_authors, er_candidates
# output: lgbtiq_kg_clean.entities


library(tidyverse)
library(kabrutils)
library(config)
library(testdat)
library(DBI)


# Get data ----------------------------------------------------------------

con <- connect_db()

books_authors <- tbl(con, "books_authors") %>%
  collect() %>% 
  mutate(source = "book_author",
         id = paste(source, author_id, sep = "_")) %>% 
  rename(name = author,
         item_id = author_id)

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

er_candidates <- tbl(con, "er_candidates") %>% 
  rename(er_id = id) %>% 
  collect()

dbDisconnect(con); rm(con)

entities_raw <- bind_rows(books_publishers, posters_authors, books_authors) %>% 
  mutate(item_id = as.character(item_id)) %>% 
  filter(!name %in% c("?"))

# Resolve manual decisions and create a new ID for those ------------------

new_ids_raw <- er_candidates %>%
  filter(decision == "positive") %>%
  pmap_dfr(function(...) {
    
    current <- tibble(...) %>% 
      mutate(id_1 = paste0(source_1, "_", id_1),
             id_2 = paste0(source_2, "_", id_2))
    
    candidate_1 <- entities_raw %>% 
      inner_join(current, by = c("id" = "id_1")) %>% 
      mutate(id_new = paste0("er_", er_id)) %>% 
      select(id, id_new, name_1 = name, keep_label_2, new_label)
    
    
    candidate_2 <- entities_raw %>% 
      inner_join(current, by = c("id" = "id_2")) %>% 
      mutate(id_new = paste0("er_", er_id)) %>% 
      select(id, id_new, name_2 = name, keep_label_2, new_label)
    
    er_pair <- candidate_1 %>% 
      left_join(candidate_2, by = c("id_new", "keep_label_2", "new_label"), suffix = c("_1", "_2")) %>% 
      mutate(
        name = 
          case_when(
            keep_label_2 == 1 ~ name_2,
            !is.na(new_label) ~ new_label,
            TRUE ~ name_1
          )
      ) %>% 
      filter(!is.na(name))
    
    er_pair
    
  }) 


# Save mapping of old to new ID -------------------------------------------

id_mappings <- new_ids_raw %>% select(id_new, id_1, id_2)


# minor tweaks in new_ids df ----------------------------------------------

new_ids <- new_ids_raw %>% 
  select(id = id_new, name) %>% 
  mutate(
    name = case_when(
      str_detect(name, "Koordinierungsstelle für") ~ "Koordinierungsstelle für gleichgeschlechtliche Lebensweisen Stadt München",
      name == "AIDS-Hilfe Nürnberg-Erlangen-Fürth" ~ "AIDS-Hilfe Nürnberg-Erlangen-Fürth e.V.",
      name == "D. A. F. Sade" ~ "Donatien Alphonse François de Sade",
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
  filter(id_new != id_old)


# Add to ID mapping -------------------------------------------------------

id_mappings <- bind_rows(
  id_mappings, 
  
  # map id (incl. "old" er_id's) and add to mappings df
  er_candidates %>% 
    filter(decision == "positive") %>% 
    mutate(id_1 = paste0(source_1, "_", id_1),
           id_2 = paste0(source_2, "_", id_2),
           er_new = paste0("er_", er_id)) %>% 
    left_join(duplicate_new_ids_for_mapping, by = c("er_new" = "id_new")) %>%
    mutate(er_id = paste0("er_", er_id)) %>% 
    select(id_new = er_id, id_1, id_2, er_old = id_old)) %>% 
  distinct() %>% 
  filter(!is.na(id_new))


# add de-deduplicated IDs to df with all new entities ---------------------

new_ids_final <- bind_rows(
  new_ids %>% anti_join(new_ids %>% 
                          count(name, sort = T) %>% 
                          filter(n>1), by = "name"),
  duplicate_new_ids %>% select(id = id_new, name)
)



# Add to other entities ---------------------------------------------------

# in order to create proper id's I first have to remove the new ER'ed id from entities df.

remove_from_entities <- new_ids_final %>% 
  mutate(er_id_to_join = as.numeric(str_remove(id, "er_"))) %>% 
  left_join(er_candidates, by = c("er_id_to_join" = "er_id")) %>% 
  select(id, source_1, id_1, source_2, id_2)

remove_from_entities <- bind_rows(
  remove_from_entities %>% select(source = source_1, item_id = id_1),
  remove_from_entities %>% select(source = source_2, item_id = id_2)) 


entities <- entities_raw %>% 
  anti_join(remove_from_entities, by = c("item_id", "source")) %>% 
  select(id, name) %>% 
  bind_rows(new_ids_final) %>% 
  distinct()
  
# There are still some duplicates,  removed here --------------------------

duplicate_entities <- entities %>% 
  inner_join(entities %>% 
               count(name, sort = T) %>% 
               filter(n>1) %>% 
               filter(!name %in% c("Forum", "Bruno Gmünder")), by = "name") %>% 
  select(-n) %>% 
  group_by(name) %>% 
  mutate(rowid = row_number()) %>% 
  ungroup() %>% 
  pivot_wider(id_col = name, names_from  = rowid, values_from = id) %>% 
  rename(id_old = `1`, id_new = `2`)


# Same removed ones in mapping df -----------------------------------------

id_mappings <- bind_rows(id_mappings, duplicate_entities %>% select(id_new, er_old = id_old))
  
# Add de-duduplicated to entities df --------------------------------------


entities_final <- bind_rows(
  entities %>% anti_join(entities %>% 
                          count(name, sort = T) %>% 
                          filter(n>1), by = "name"),
  duplicate_entities %>% select(id = id_new, name)
)

# Get mappings: What's probably an Org or a Club (e.V.)? -----------------

string_mappings <- config::get(file = "static/string-mapping.yml")
orgs_indicator <- paste0(string_mappings$organisation, collapse = "|")


entities_final <- entities_final %>% 
  mutate(
    org = 
      case_when(
        tolower(name) %>% str_detect(orgs_indicator) ~ 1,
        str_detect(id, "book_publisher") ~ 1,
        TRUE ~ 0
      ),
    club = 
      case_when(
        tolower(name) %>% str_detect("e\\.v") ~ 1,
        TRUE ~ 0
      ),
    # if not an org or club, but a book_author, then person
    person = 
      case_when(
        str_detect(id, "book_author") & org == 0 & club == 0 ~ 1,
        # some special cases
        id == "er_242" & name == "Bruno Gmünder" ~ 1,
        name == "Sands Murray-Wassink" ~ 1,
        TRUE ~ 0
      ),
    org = as.factor(org),
    club = as.factor(club)
  )



# Write in DB -------------------------------------------------------------

import <- entities_final

test_that(
  desc = "uniqueness",
  expect_unique(c("id"), data = import)
)


# This table will be saved in a new database ------------------------------

# CREATE DATABASE lgbtiq_kg_clean;

create_table <- "
CREATE TABLE IF NOT EXISTS `entities` (
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `id` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `name` varchar(255) COLLATE utf8mb3_german2_ci NOT NULL,
  `org` bigint(5) DEFAULT NULL,
  `club` bigint(5) DEFAULT NULL,
  `person` bigint(5) DEFAULT NULL,
  PRIMARY KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci COMMENT='Table distinct entities from books_authors, book_publishers and posters_authors.';
"

con <- connect_db(credential_name = "db_clean")
dbExecute(con, create_table)
dbAppendTable(con, "entities", import)
dbDisconnect(con); rm(con)

