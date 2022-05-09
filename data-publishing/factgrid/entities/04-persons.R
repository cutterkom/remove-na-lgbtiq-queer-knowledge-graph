# title: Write person entities in Factgrid
# desc: Data is reconciled against 
# input: 
# output: 


# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- read_sheet(config$statements$gs_table, sheet = "statements")

# Get input data ----------------------------------------------------------

con <- connect_db("db_clean")

entities_raw <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  select(-contains("_at"), -entity_id_type, -entity_id_combination, -entity_id_combination_type, external_id_label) %>% 
  collect()

entities_per_type <- tbl(con, "entities_per_type") %>% 
  select(contains("id")) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)


# Add info if book_author,  poster_author or publisher --------------------

persons <- entities_raw %>% 
  filter(person == 1) %>% 
  left_join(entities_per_type, by = "id") %>% 
  distinct() %>% 
  mutate(
    book_author = ifelse(!is.na(book_id), 1, 0),
    poster_author = ifelse(!is.na(poster_id), 1, 0),
    publisher = ifelse(!is.na(publisher_book_id), 1, 0)
  ) %>% 
  distinct(name, id, book_author, poster_author, publisher)




# Create Item ID ----------------------------------------------------------

# if already in Factgrid -> Factgrid QID, otherwise CREATE_id

items_id <- persons %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid"), by = "id") %>% 
  mutate(
    item = ifelse(!is.na(external_id), external_id, paste0("CREATE_", id))
  ) %>% 
  distinct(id, item)


# Get Wikidata ID ---------------------------------------------------------


wikidata <- persons %>% 
  inner_join(
    el_matches %>% 
      filter(external_id_type == "wikidata", external_id != "no_wikidata_id") %>% 
      mutate(external_id = str_remove(external_id, "http://www.wikidata.org/entity/")), 
    by = "id") %>% 
  distinct(id, name, qid = external_id)


wikipedia_sitelinks <- wikidata %>% 
  pmap_df(function(...) {
    current <- tibble(...)
    tmp <- tibble(
      id = current$id,
      Sdewiki = tw_get_wikipedia(id = current$qid, language = tidywikidatar::tw_get_language()),
      Senwiki = tw_get_wikipedia(id = current$qid, language = "en")
      )
  })


# Statements --------------------------------------------------------------

person_statements <- 
  bind_rows(
    persons %>% filter(book_author == 1) %>% add_statement(statements, "career_is_author"),
    persons %>% filter(publisher == 1) %>% add_statement(statements, "career_is_publisher")
    # add poster authors
    # persons %>% filter(poster_author == 1) %>% add_statement(statements, "POSTER")
  ) %>% 
  add_statement(statements, "research_area") %>% 
  add_statement(statements, "research_project") %>% 
  right_join(items_id, by = "id") %>% 
  left_join(wikipedia_sitelinks, by = "id") %>% 
  left_join(wikidata %>% distinct(id, Swikidatawiki = qid), by = "id")

  

saveRDS(wikipedia_sitelinks, file = "data-publishing/factgrid/data/wikipedia_sitelinks.Rds")
