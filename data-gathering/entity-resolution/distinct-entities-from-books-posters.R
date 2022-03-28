# title: Create distinct entities from manual entity resolution
# desc: Takes authors and publishers and check, if they were in the manual entitiy resolution process. If so, then entitiy gets a new ID. Also, some first rough guesses are made if an entity is an organisation, club or person. 
# input: books_authors, book_publishers, posters_authors, er_candidates
# output: 


library(tidyverse)
library(kabrutils)
library(config)


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
  mutate(item_id = as.character(item_id))


# Entity resolution: Define new id's --------------------------------------

new_ids <- er_candidates %>%
  filter(decision == "positive") %>%
  pmap_dfr(function(...) {
    
    current <- tibble(...) %>% 
      mutate(id_1 = paste0(source_1, "_", id_1),
             id_2 = paste0(source_2, "_", id_2))

    candidate_1 <- entities_raw %>% 
      inner_join(current, by = c("id" = "id_1")) %>% 
      mutate(new_id = paste0("er_", er_id)) %>% 
      select(id, new_id, name_1 = name, keep_label_2, new_label)

    candidate_2 <- entities_raw %>% 
      inner_join(current, by = c("id" = "id_2")) %>% 
      mutate(new_id = paste0("er_", er_id)) %>% 
      select(id, new_id, name_2 = name, keep_label_2, new_label)
    
    er_pair <- candidate_1 %>% 
      full_join(candidate_2, by = c("id", "new_id", "keep_label_2", "new_label")) %>% 
      mutate(
        name = 
          case_when(
            keep_label_2 == 1 ~ name_2,
            !is.na(new_label) ~ new_label,
            TRUE ~ name_1
          )
      ) %>% 
      select(id = new_id, name) %>% 
      filter(!is.na(name))
    
    er_pair
  }) %>% 
  distinct() %>% 
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
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  select(-n)

new_ids_final <- bind_rows(
  new_ids %>% anti_join(new_ids %>% 
              count(name, sort = T) %>% 
              filter(n>1), by = "name"),
  duplicate_new_ids
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
  bind_rows(new_ids_final)
  

# Get mappings: What's probably an Org or a Club (e.V.)? -----------------

string_mappings <- config::get(file = "static/string-mapping.yml")
orgs_indicator <- paste0(string_mappings$organisation, collapse = "|")


entities <- entities %>% 
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
  ) %>% 
  filter(!name %in% c("München", "?"))


# There are still some duplicates,  removed here --------------------------

duplicate_entities <- entities %>% 
  inner_join(entities %>% 
               count(name, sort = T) %>% 
               filter(n>1) %>% 
               filter(!name %in% c("Forum", "Bruno Gmünder")), by = "name") %>% 
  group_by(name) %>% 
  filter(row_number() == 2) %>% 
  ungroup() %>% 
  select(-n)


entities_final <- bind_rows(
  entities %>% anti_join(entities %>% 
                          count(name, sort = T) %>% 
                          filter(n>1), by = "name"),
  duplicate_entities
)


# Write in DB -------------------------------------------------------------

import <- entities_final

test_that(
  desc = "uniqueness",
  expect_unique(c("id"), data = import)
)

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

con <- connect_db()
dbExecute(con, create_table)
dbAppendTable(con, "entities", import)
dbDisconnect(con); rm(con)

