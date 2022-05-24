# Data model Publications https://database.factgrid.de/wiki/FactGrid:Print_publications_data_model


library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "statements")

tw_enable_cache()
tw_set_cache_folder(path = fs::path(fs::path_home_r(), "R", "tw_data"))
tw_set_language(language = "de")
tw_create_cache_folder(ask = FALSE)

# Get input data ----------------------------------------------------------

con <- connect_db("db_clean")

entities_raw <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  select(-contains("_at"), -entity_id_type, -entity_id_combination, -entity_id_combination_type, external_id_label) %>% 
  collect()

posters_raw_clean <- tbl(con, "posters_wide") %>% collect()

DBI::dbDisconnect(con); rm(con)

con <- connect_db()
posters_meta <- tbl(con, "posters_wide") %>% collect() %>% 
  distinct(poster_id, year, size)
DBI::dbDisconnect(con); rm(con)


# Build URL ---------------------------------------------------------------

posters <- posters_raw_clean %>% 
  mutate(url = stringi::stri_trans_general(short_title, "de-ASCII; Latin-ASCII"),
         url = str_remove_all(url, "\\?|\\(|\\)|!|\\[|'|\\]"),
         url = str_remove_all(url, '"'),
         url = tolower(url),
         url = str_replace_all(url, "\\s\\s|\\s\\s\\s", " "),
         url = str_replace_all(url, ",", "-"),
         url = str_replace_all(url, "--", "-"),
         url = str_replace_all(url, "\\s|\\.", "-"),
         url = str_replace_all(url, "--|---|----", "-"),
         url = str_replace_all(url, "--", "-"),
         url = paste0("https://archiv.forummuenchen.org/objekt/", url),
         url = ifelse(is.na(short_title), NA_character_, url)) %>% 
  distinct(id, poster_id, name, title, short_title, url) %>% 
  left_join(posters_meta, by = "poster_id") %>% 
  mutate(year_clean = str_extract(year, "[0-9]{4}"),
         year_qual_property = ifelse(str_detect(year, "\\?|ca."), "P155", NA_character_),
         year_qual_value = ifelse(str_detect(year, "\\?|ca."), "Q10", NA_character_))


#posters %>% filter(str_detect(short_title, "Kulturladen Westend, Die Geierwallis")) %>% pull(url) %>% unique() %>% browseURL(.)

input_statements <- posters %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, author_item = external_id), by = "id") %>% 
  # add poster ID as Forum ID
  mutate(P728 = as.character(paste0("poster_", poster_id))) %>% 
  mutate(item = paste0("CREATE_", P728)) %>% 
  add_statement(statements, "instance_of_print_publication") %>% 
  rename(P2_2 = P2) %>% 
  add_statement(statements, "instance_of_poster") %>% 
  #add author statement
  add_statement(statements, "is_author", qid_from_row = TRUE, col_for_row_content = "author_item") %>% 
  add_statement(statements, "link_to_website", qid_from_row = TRUE, col_for_row_content = "url") %>% 
  mutate(
    Lde = ifelse(!is.na(short_title), short_title, substr(title, 1, 200)),
    Len = ifelse(!is.na(short_title), short_title, substr(title, 1, 200)),
    Les = ifelse(!is.na(short_title), short_title, substr(title, 1, 200)),
    Lfr = ifelse(!is.na(short_title), short_title, substr(title, 1, 200)),
    Dde = "Poster im Zusammenhang mit der LGBTIQ*-Geschichte",
    Den = "Poster related to LGBTIQ* history",
    Dfr = "Affiches en rapport avec l'histoire LGBTIQ",
    Des = "Carteles relacionados con la historia de LGBTIQ*"
  ) %>% 
  add_statement(statements, "note", qid_from_row = TRUE, col_for_row_content = "title") %>% 
  #mutate(P73 = str_replace_all(P73, '"', "'")) %>% 
  mutate(P73 = str_remove_all(P73, "\r|\n")) %>% 
  add_statement(statements, "research_project") %>% 
  add_statement(statements, "research_area") %>% 
  # add year
  mutate(P222 = ifelse(!is.na(year_clean), paste0("+", year_clean, "-00-00T00:00:00Z/9"), NA_character_),
         P222 = str_remove_all(P222, " |\\s")) %>% 
  distinct()

# Prepare import ----------------------------------------------------------

sort_helper <- input_statements %>% 
  select(starts_with("L"), starts_with("D"), P2_2, P2, P21, P222, P156, P73, P728, P97, P131) %>% 
  names() %>% 
  enframe() %>% 
  rename(sort = name, property = value)

year_qual <- input_statements %>% select(item, matches("^year_qual", ignore.case = FALSE)) %>% 
  mutate(property = "P222") %>% 
  replace(is.na(.), "")

import <- input_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  arrange(item) %>% 
  select(item, starts_with("L"), starts_with("D"), P2_2, P2, P21, P222, P156, P73, P728, P97, P131) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value", values_drop_na = TRUE) %>%
  distinct() %>% 
  left_join(sort_helper, by = "property") %>%
  arrange(item, sort) %>% 
  select(-sort) %>% 
  left_join(year_qual, by = c("item", "property")) %>% 
  distinct() %>% 
  replace(is.na(.), "")

import <- import %>% filter(!item %in% c("CREATE_poster_102", "CREATE_poster_1006"))

# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  qual.properties = import$year_qual_property,
  qual.values = import$year_qual_value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/posters.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)



# Fetch from Factgrid -----------------------------------------------------

query <- 
  '
SELECT ?item ?itemLabel ?id ?idLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P131 wd:Q400012.
  OPTIONAL { ?item wdt:P728 ?id. }
  ?item wdt:P2 wd:Q408517.
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))


factgrid_qids <- input_statements %>% 
  select(poster_id = P728, Lde) %>% 
  left_join(query_res, by = c("poster_id" = "idLabel"))


to_do <- factgrid_qids %>% filter(is.na(external_id))



# Remove duplicates -------------------------------------------------------

# there are some duplicates
dups <- input_statements %>% 
  distinct(poster_id, Dde, Lde) %>%  
  count(Lde, Dde, sort = T) %>% 
  filter(n>1)

dups

final_labels_dups <- input_statements %>% 
  distinct(poster_id = P728, Lde, Dde) %>% 
  inner_join(dups, by = c("Lde", "Dde")) %>% 
  group_by(Lde, Dde) %>% 
  mutate(rowid = row_number(),
         Lde = paste0(Lde, " (", rowid, ")"))
  
to_do %>% left_join(final_labels_dups, by = "poster_id") %>% View

# new labels to state number of poster
input_statements2 <- to_do %>%
  distinct(poster_id, Lde) %>% 
  inner_join(dups %>% select(-Dde), by = c("Lde")) %>% 
  group_by(Lde) %>%
  mutate(rowid = row_number() + 1,
         Lde = paste0(Lde, " (", rowid, ")"),
         Len = Lde,
         Lfr = Lde,
         Les = Lde) %>%
  left_join(input_statements %>% 
              select(item, 
                     matches("^D", ignore.case = FALSE), 
                     matches("^S", ignore.case = FALSE), 
                     matches("^P", ignore.case = FALSE)),
            by = c("poster_id" = "P728")) %>% 
  mutate(P156 = paste0(P156, "-", rowid)) %>% 
  rename(P728 = poster_id)
  
 
import <- input_statements2 %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  arrange(item) %>% 
  select(item, starts_with("L"), starts_with("D"), P2_2, P2, P21, P222, P156, P73, P728, P97, P131) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value", values_drop_na = TRUE) %>%
  distinct() %>% 
  left_join(sort_helper, by = "property") %>%
  arrange(item, sort) %>% 
  select(-sort) %>% 
  left_join(year_qual, by = c("item", "property")) %>% 
  distinct() %>% 
  replace(is.na(.), "")


# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  qual.properties = import$year_qual_property,
  qual.values = import$year_qual_value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/posters2.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# Fetch from Factgrid -----------------------------------------------------

query <- 
  '
SELECT ?item ?itemLabel ?id ?idLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P131 wd:Q400012.
  OPTIONAL { ?item wdt:P728 ?id. }
  ?item wdt:P2 wd:Q408517.
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))


factgrid_qids <- input_statements %>% 
  select(poster_id = P728, Lde) %>% 
  left_join(query_res, by = c("poster_id" = "idLabel"))


# Fix: URL had some more [ and ] -> remove and reimport
to_do <- factgrid_qids %>% filter(is.na(external_id))

input_statements3 <- to_do %>% 
  distinct(poster_id) %>% 
  inner_join(input_statements %>% mutate(poster_id = P728), by = "poster_id")

import <- input_statements3 %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  arrange(item) %>% 
  select(item, starts_with("L"), starts_with("D"), P2_2, P2, P21, P222, P156, P73, P728, P97, P131) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value", values_drop_na = TRUE) %>%
  distinct() %>% 
  left_join(sort_helper, by = "property") %>%
  arrange(item, sort) %>% 
  select(-sort) %>% 
  left_join(year_qual, by = c("item", "property")) %>% 
  distinct() %>% 
  replace(is.na(.), "")


# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  qual.properties = import$year_qual_property,
  qual.values = import$year_qual_value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/posters3.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)

# Fetch from Factgrid -----------------------------------------------------

query <- 
  '
SELECT ?item ?itemLabel ?id ?idLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P131 wd:Q400012.
  OPTIONAL { ?item wdt:P728 ?id. }
  ?item wdt:P2 wd:Q408517.
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))


factgrid_qids <- input_statements %>% 
  select(poster_id = P728, Lde) %>% 
  left_join(query_res, by = c("poster_id" = "idLabel"))



# Write Factgrid IDs in DB ------------------------------------------------

testthat::test_that(
  desc = "all posters in factgrid",
  expect_equal(factgrid_qids %>% filter(is.na(external_id)) %>% nrow(), 0)
)

import <- factgrid_qids %>% 
  distinct(id, external_id) %>% 
  filter(!is.na(external_id)) %>% 
  mutate(
    entity_id_type = "entities",
    external_id_type = "factgrid", 
    source = "forum", 
    hierarchy = 1)

con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)
