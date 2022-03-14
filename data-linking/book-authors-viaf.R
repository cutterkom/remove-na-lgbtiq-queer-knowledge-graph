# title: Connect books_authors with VIAF
# there is an API to the Viaf.org authority file that suggest an entity based on an input string. R-package viafr is used for it.
# input: books_authors
# output: el_viaf_books_authors

library(tidyverse)
library(viafr)
library(DBI)
library(cli)
library(dbx)
library(kabrutils)
library(testdat)

# import from db ----------------------------------------------------------

con <- connect_db()
authors <- tbl(con, "books_authors") %>%
  collect() %>% 
  rename(id = author_id) %>% 
  # remove `é` etc, but change Ö to OE instead of O
  mutate(author = stringi::stri_trans_general(author, "de-ASCII; Latin-ASCII"))
DBI::dbDisconnect(con); rm(con)


# Get suggestions from VIAF API -------------------------------------------

#' Get suggestion of viaf_id
#'
#' @param name name as string
get_viaf_suggest <- function(name) {
  cli_alert_info(name)
  safer_viaf_suggest <- possibly(viaf_suggest, otherwise = "no item found")
  data <- safer_viaf_suggest(name)
  if (data == "no item found") {
    data <- NULL
  } else {
    data <- data %>%
      enframe() %>%
      unnest(col = "value") %>%
      unnest(col = "source_ids", names_sep = "_") %>%
      mutate(across(everything(), as.character))
  }
}

# Get the DNB-ID with highest score per author_id
viaf_data_raw <- authors %>%
  mutate(viaf = map(.x = author, .f = get_viaf_suggest)) %>%
  unnest(cols = c(viaf))


viaf_data <- viaf_data_raw %>%
  filter(source_ids_scheme == "DNB") %>%
  #rename(id = author_id) %>% 
  group_by(id) %>%
  filter(score == max(score)) %>%
  ungroup() %>%
  distinct(id, author, viaf_id, gnd_id = source_ids_id, source_ids_scheme, score) %>%
  mutate(score = as.numeric(score)) %>%
  right_join(authors, by = c("id", "author"))

# cleaning ----------------------------------------------------------------

# problematic, when single names like tony, stephan etc -> remove them

viaf_data <- viaf_data %>%
  filter(str_detect(author, " ")) %>%
  filter(!is.na(viaf_id))

# Write in DB -------------------------------------------------------------

import <- viaf_data %>%
  select(author_id = id, viaf_id, gnd_id, score)

test_that(
  desc = "unique values",
  expect_unique(c(author_id, viaf_id, gnd_id), data = import)
)

create_table <- "
CREATE TABLE IF NOT EXISTS `el_viaf_books_authors` (
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `author_id` int(11) NOT NULL,
  `viaf_id` VARCHAR(55) NOT NULL,
  `gnd_id` VARCHAR(55),
  `score` int(11),
  PRIMARY KEY `id` (`author_id`, `viaf_id`, `gnd_id`),
  CONSTRAINT `author_id` FOREIGN KEY (`author_id`) REFERENCES `books_authors` (`author_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci COMMENT='Table to store links of books_authors to VIAF and GND.';
"

con <- connect_db()
dbExecute(con, create_table)
dbAppendTable(con, "el_viaf_books_authors", import)
dbDisconnect(con); rm(con)