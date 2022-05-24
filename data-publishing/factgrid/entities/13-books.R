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

publishers <- tbl(con, "books_publishers") %>% collect() %>% distinct(publisher_id = id, book_id)

books_raw <- tbl(con, "books_wide") %>% collect() %>% 
  mutate(
    isbn = str_remove_all(isbn, " "),
    nchar_isbn = nchar(isbn)) %>% 
  select(-name) %>% 
  distinct() %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, author_item = external_id), by = "id") %>% 
  left_join(publishers, by = c("book_id")) %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, publisher_item = external_id), by = c("publisher_id" = "id")) %>% 
  left_join(entities_raw %>% select(author_id = id, name), by = c("id" = "author_id"))
  
DBI::dbDisconnect(con); rm(con)

con <- connect_db()
books_meta <- tbl(con, "books_wide") %>% distinct(book_id, location, year) %>% collect() %>% 
  mutate(year = trimws(year),
         year = str_remove_all(year, " |\\s")) #%>% 
  # group_by(book_id) %>% 
  # arrange(year) %>% 
  # filter(row_number() == 1) %>% 
  # ungroup()
DBI::dbDisconnect(con); rm(con)


# Create Statements -------------------------------------------------------

final_labels <- books_raw %>% 
  select(book_id, title, subtitle, author = name) %>% 
  distinct() %>% 
  group_by(book_id) %>% 
  mutate(author = as.character(glue::glue_collapse(author, sep = ", ", last = ""))) %>% 
  ungroup() %>% 
  distinct() %>% 
  left_join(books_meta, by = "book_id") %>% 
  mutate(title = ifelse(is.na(title), "", title),
         subtitle = ifelse(is.na(subtitle), "", subtitle),
         author = ifelse(is.na(author), "", author)) %>% 
  group_by(book_id) %>% 
  mutate(year_label = as.character(glue::glue_collapse(year, sep = ", ", last = ""))) %>% 
  distinct() %>% 
  mutate(
         Lde = paste0(author, ", ", title, ", ", subtitle, " (", year_label, ")"),
    #Lde = paste0(author, ", ", title, ", ", subtitle),
         Lde = str_replace_all(Lde, ", , ", ", "),
         Lde = str_replace_all(Lde, ",  \\(", " \\("),
         Len = Lde,
         Lfr = Lde,
         Les = Lde) %>%
  select(book_id, starts_with("L"))


# Add ISBN
input_statements <- bind_rows(
  books_raw %>% 
    filter(nchar_isbn == 13) %>% 
    add_statement(statements, "is_isbn13", qid_from_row = TRUE, col_for_row_content = "isbn"),
  books_raw %>% 
    filter(nchar_isbn == 10) %>% 
    add_statement(statements, "is_isbn10", qid_from_row = TRUE, col_for_row_content = "isbn"),
  books_raw %>% 
    filter(!nchar_isbn %in% c(13, 10))
) %>% 
  mutate(item = paste0("CREATE_", book_id)) %>% 
  left_join(final_labels, by = "book_id") %>% 
  add_statement(statements, "external_id_forum", qid_from_row = TRUE, col_for_row_content = "book_id") %>% 
  mutate(P728 = as.character(paste0("book_", P728))) %>% 
  left_join(books_meta %>% select(book_id, P222 = year), by = "book_id") %>% 
  #filter(is.na(P222)) %>% 
  mutate(P222 = paste0("+", P222, "-00-00T00:00:00Z/9"),
         P222 = str_remove_all(P222, " |\\s")) %>% 
  # author
  add_statement(statements, "is_author", qid_from_row = TRUE, col_for_row_content = "author_item") %>% 
  add_statement(statements, "instance_of_print_publication") %>% 
  add_statement(statements, "is_publisher", qid_from_row = TRUE, col_for_row_content = "publisher_item") %>% 
  add_statement(statements, "research_project") %>% 
  add_statement(statements, "research_area") %>% 
  mutate(
    Dde = "Buch im Zusammenhang mit LGBTIQ*-Geschichte",
    Den = "Book related to LGBTIQ* history",
    Dfr = "Livre en rapport avec l'histoire LGBTIQ",
    Des = "El libro en el contexto de la historia LGBTIQ*"
  ) %>% 
  distinct()



# Prepare import ----------------------------------------------------------

sort_helper <- input_statements %>% 
  select(starts_with("L"), starts_with("D"), P2, P21, P206, P222, P605, P606, P728, P97, P131) %>% 
  select(-location) %>% 
  names() %>% 
  enframe() %>% 
  rename(sort = name, property = value)

import <- input_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>%
  arrange(item) %>% 
  select(item, starts_with("L"), starts_with("D"), P2, P21, P206, P222, P605, P606, P728, P97, P131) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value", values_drop_na = TRUE) %>%
  distinct() %>% 
  left_join(sort_helper, by = "property") %>%
  arrange(item, sort)
  

#import <- import %>% filter(item != "CREATE_1421")

# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/books-wo-year.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# Import IDs in local DB --------------------------------------------------

query <- 
  '
SELECT ?item ?itemLabel ?id ?idLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P131 wd:Q400012.
  OPTIONAL { ?item wdt:P728 ?id. }
  ?item wdt:P2 wd:Q20.
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))

factgrid_qids <- input_statements %>% 
  distinct(id = P728, Lde) %>% 
  left_join(query_res, by = c("id" = "idLabel"))

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



books_raw %>% 
  left_join(el_matches)