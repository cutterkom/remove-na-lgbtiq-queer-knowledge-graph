# Somehow Christa Reinig Books have an NA as id in lgbtiq_kg_clean.book_authors
# need to be imported 


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

# Get input data ----------------------------------------------------------

con <- connect_db("db_clean")

el_matches <- tbl(con, "el_matches") %>% 
  select(-contains("_at"), -entity_id_type, -entity_id_combination, -entity_id_combination_type, external_id_label) %>% 
  filter(external_id_type == "factgrid") %>% 
  select(id, external_id) %>% 
  collect()

books_raw <- tbl(con, "books_authors") %>% 
  filter(id == "NA") %>% 
  select(book_id) %>%
  inner_join(tbl(con, "books"), by = c("book_id")) %>% 
  left_join(tbl(con, "books_publishers") , by = "book_id") %>% 
  left_join(tbl(con, "entities") %>% select(id, name), by = "id") %>% 
  collect() %>% 
  left_join(el_matches, by = c("id")) %>% 
  mutate(nchar_isbn = nchar(isbn))

DBI::dbDisconnect(con); rm(con)

con <- connect_db()
books_meta <- tbl(con, "books_wide") %>% distinct(book_id, location, year) %>% 
  collect() %>% 
  filter(book_id %in% books_raw$book_id) %>% 
  mutate(year = trimws(year),
         year = str_remove_all(year, " |\\s")) #%>% 
DBI::dbDisconnect(con); rm(con)

# Create Labels -----------------------------------------------------------

final_labels <- books_raw %>% 
  mutate(author = "Christa Reinig") %>% 
  select(book_id, name = name.x, subtitle, author) %>% 
  distinct() %>% 
  group_by(book_id) %>% 
  mutate(author = as.character(glue::glue_collapse(author, sep = ", ", last = ""))) %>% 
  ungroup() %>% 
  distinct() %>% 
  left_join(books_meta, by = "book_id") %>% 
  mutate(title = ifelse(is.na(name), "", name),
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



# Create Statements -------------------------------------------------------

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
  mutate(item = paste0("CREATE_", book_id),
         author_item = "Q403941",
         publisher_item = external_id) %>% 
  left_join(final_labels, by = "book_id") %>% 
  add_statement(statements, "external_id_forum", qid_from_row = TRUE, col_for_row_content = "book_id") %>% 
  mutate(P728 = as.character(paste0("book_", P728))) %>% 
  left_join(books_meta %>% select(book_id, P222 = year), by = "book_id") %>% 
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
    Des = "El libro en el contexto de la historia LGBTIQ*",
    P2 = "Q394362"
  ) %>% 
  distinct()

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

# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/christa-reinig.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = T,
  quickstatements.url = config$connection$quickstatements_url
)
