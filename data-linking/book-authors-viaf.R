# Connect forum author_id with VIAF
# https://viaf.org/

library(tidyverse)
library(viafr)
library(DBI)
library(cli)
library(dbx)
library(kabrutils)

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
  group_by(author_id) %>%
  filter(score == max(score)) %>%
  ungroup() %>%
  distinct(author_id, author, viaf_id, gnd_id = source_ids_id, source_ids_scheme, score) %>%
  mutate(score = as.numeric(score)) %>%
  right_join(authors, by = c("author_id", "author"))

# cleaning ----------------------------------------------------------------

# problematic, when single names like tony, stephan etc -> remove them

viaf_data <- viaf_data %>%
  filter(str_detect(author, " ")) %>%
  filter(!is.na(viaf_id))

# Write in DB -------------------------------------------------------------

import <- viaf_data %>%
  select(id = author_id, viaf_id, gnd_id, score)

con <- connect_db()
dbxUpsert(con, "books_authors_viaf_gnd", import, where_cols = c("id", "viaf_id", "gnd_id"))
dbDisconnect(con)
rm(con)
