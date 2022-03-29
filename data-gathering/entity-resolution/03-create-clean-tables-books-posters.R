# title: Save deduplicated entities to new database lgbtiq_kg_clean
# desc: In order to keep different thing separatly, I create a new database that contains only cleaned data. In the first step this is are authors and publishers from books and posters. 
# input: lgbtiq_kg_clean.entities, lgbtiq_kg.books_author*, lgbtiq_kg.books_publisher*, lgbtiq_kg.posters_author*
# output: lgbtiq_kg_clean.books_author*, lgbtiq_kg_clean.books_publisher*, lgbtiq_kg_clean.posters_author*


library(tidyverse)
library(kabrutils)
library(testdat)
library(DBI)


# Get deduplicated entities -----------------------------------------------

con <- connect_db(credential_name = "db_clean")
entities <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()
id_mapping <- tbl(con, "id_mapping") %>% 
  select(-contains("_at")) %>% 
  collect()
DBI::dbDisconnect(con); rm(con)


# Get authors and publishers ----------------------------------------------

con <- connect_db()

books_authors_raw <- tbl(con, "books_authors") %>%
  left_join(tbl(con, "book_author"), by = "author_id") %>% 
  collect() %>% 
  mutate(source = "book_author",
         id = paste(source, author_id, sep = "_")) %>% 
  rename(
    name = author,
    item_id = author_id)
  

books_publishers_raw <- tbl(con, "books_publishers") %>%
  left_join(tbl(con, "book_publisher"), by = "publisher_id") %>% 
  collect() %>% 
  mutate(source = "book_publisher",
         id = paste(source, publisher_id, sep = "_")) %>% 
  rename(name = publisher,
         item_id = publisher_id)

posters_authors_raw <- tbl(con, "posters_authors") %>%
  left_join(tbl(con, "poster_author"), by = "author_id") %>% 
  collect() %>% 
  mutate(source = "poster_author",
         id = paste(source, author_id, sep = "_")) %>% 
  rename(name = author,
         item_id = author_id)

dbDisconnect(con); rm(con)



# Update IDs --------------------------------------------------------------

id_mapping_long <- id_mapping %>% 
  pivot_longer(cols = c(id_1, id_2), names_to = "id_type", values_to = "id_old")

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


books_authors <- update_ids(books_authors_raw, id_mapping_long) %>% 
  select(id, name, book_id) %>% 
  distinct()

books_publishers <- update_ids(books_publishers_raw, id_mapping_long) %>% 
  select(id, name, book_id) %>% 
  distinct()

posters_authors <- update_ids(posters_authors_raw, id_mapping_long) %>% 
  select(id, name, poster_id) %>% 
  distinct()

# Update IDs 2nd round ----------------------------------------------------

# This process needs to be done again, because some er_ids needed to be de-duplicated in a two-step-process


id_mapping_long_2nd_round <- id_mapping_long %>% 
  filter(!is.na(er_old)) %>% 
  select(id_new, id_old = er_old)

books_authors_import <- update_ids(books_authors, id_mapping_long_2nd_round) %>% 
  select(-name, -id_new) %>% 
  distinct() %>% 
  left_join(entities %>% select(id, name), by = "id") %>% 
  select(-name)

test_that(
  desc = "uniqueness",
  expect_unique(c("id", "book_id"), data = books_authors_import)
)

books_publishers_import <- update_ids(books_publishers, id_mapping_long_2nd_round) %>% 
  select(-name, -id_new) %>% 
  distinct() %>% 
  left_join(entities %>% select(id, name), by = "id") %>% 
  distinct(id, book_id)

test_that(
  desc = "uniqueness",
  expect_unique(c("id", "book_id"), data = books_publishers_import)
)

posters_authors_import <- update_ids(posters_authors, id_mapping_long_2nd_round) %>% 
  select(-name, -id_new) %>% 
  distinct() %>% 
  left_join(entities %>% select(id, name), by = "id") %>% 
  select(-name)

test_that(
  desc = "uniqueness",
  expect_unique(c("id", "poster_id"), data = posters_authors_import)
)

# Write in DB -------------------------------------------------------------


import <- posters_authors_import

create_table <- "
CREATE TABLE IF NOT EXISTS `posters_authors` (
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `id` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `poster_id` int(11) NOT NULL,
  PRIMARY KEY `id` (`id`, `poster_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci;
"

con <- connect_db(credential_name = "db_clean")
dbExecute(con, create_table)
dbAppendTable(con, "posters_authors", import)
dbDisconnect(con); rm(con)



# Books authors -----------------------------------------------------------

import <- books_authors_import

create_table <- "
CREATE TABLE IF NOT EXISTS `books_authors` (
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `id` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `book_id` int(11) NOT NULL,
  PRIMARY KEY `id` (`id`, `book_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci;
"

con <- connect_db(credential_name = "db_clean")
dbExecute(con, create_table)
dbAppendTable(con, "books_authors", import)
dbDisconnect(con); rm(con)

# Books publishers ------------------------------------------------------

import <- books_publishers_import

create_table <- "
CREATE TABLE IF NOT EXISTS `books_publishers` (
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `id` varchar(50) COLLATE utf8mb3_german2_ci NOT NULL,
  `book_id` int(11) NOT NULL,
  PRIMARY KEY `id` (`id`, `book_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci;
"

con <- connect_db(credential_name = "db_clean")
dbExecute(con, create_table)
dbAppendTable(con, "books_publishers", import)
dbDisconnect(con); rm(con)
