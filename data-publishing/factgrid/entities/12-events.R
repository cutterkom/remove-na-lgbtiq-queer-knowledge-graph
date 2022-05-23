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

chronik <- tbl(con, "chronik") %>% collect()

DBI::dbDisconnect(con); rm(con)

# entities_raw %>% 
#   anti_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, external_id), by = "id") %>% View

# Get items not yet in Factgrid ------------------------------------

input <- entities_raw %>% 
  filter(event == 1) %>% 
  distinct(id, name) %>% 
  left_join(el_matches %>% filter(!str_detect(external_id, "no_")), by = "id") %>% 
  #filter(external_id != "no_wikidata_id") %>% 
  #distinct(id, external_id_type, external_id) %>% 
  mutate(external_id = ifelse(external_id_type == "wikidata", str_extract(external_id, "Q[0-9]+"), external_id)) %>% 
  distinct(id, name, external_id, external_id_type) %>% 
  pivot_wider(id_cols = c("id", "name"), names_from = "external_id_type", values_from = "external_id") %>% 
  select(-`NA`)


# Reconiliation -----------------------------------------------------------

input %>% distinct(id, name) %>% clipr::write_clip()

or_rec_data <- rrefine::refine_export("entities_events") %>% distinct(id, wikidata)

input <- input %>% 
  distinct(id, name, factgrid) %>% 
  left_join(or_rec_data, by = "id")

# Create Item ID ----------------------------------------------------------

input <- input %>% 
  mutate(item = ifelse(is.na(factgrid), paste0("CREATE_", id), factgrid), .after = "name")


# Fetch Wikidata data -----------------------------------------------------

wikidata <- input %>% filter(!is.na(wikidata)) %>% distinct(id, qid = wikidata)


# |- Labels ---------------------------------------------------------------

wikidata_labels <- wikidata %>% 
  pmap_df(function(...) {
    current <- tibble(...)
    tmp <- tibble(
      id = current$id,
      Lde = tw_get_label(id = current$qid, language = "de", cache = TRUE),
      Len = tw_get_label(id = current$qid, language = "en", cache = TRUE),
      Les = tw_get_label(id = current$qid, language = "es", cache = TRUE),
      Lfr = tw_get_label(id = current$qid, language = "fr", cache = TRUE)
    )
  }) 

final_labels <- input %>% 
  left_join(wikidata_labels, by = "id") %>% 
  # if some labels missing, take the other one
  mutate(
    Lde = case_when(
      is.na(Lde) & !is.na(name) ~ name,
      is.na(Lde) & !is.na(Len) ~ Len,
      TRUE ~ Lde
    ),
    Len = case_when(
      is.na(Len) & !is.na(Lde) ~ Lde,
      TRUE ~ Len
    ),
    Lfr = case_when(
      is.na(Lfr) & !is.na(Lde) ~ Lde,
      TRUE ~ Lfr
    ),
    Les = case_when(
      is.na(Les) & !is.na(Lde) ~ Lde,
      TRUE ~ Les
    )
  ) %>% 
  select(id, starts_with("L"))


testthat::test_that(
  desc = "all rows/cols filled",
  expect_equal(final_labels %>% filter_at(vars(all_of(names(.))), any_vars(is.na(.))) %>% nrow(), 0)
)


# |- Descriptions ---------------------------------------------------------

wikidata_descriptions <- wikidata %>%
  pmap_df(function(...) {
    current <- tibble(...)
    tmp <- tibble(
      id = current$id,
      Dde = tw_get_description(id = current$qid, language = "de", cache = TRUE),
      Den = tw_get_description(id = current$qid, language = "en", cache = TRUE),
      Des = tw_get_description(id = current$qid, language = "es", cache = TRUE),
      Dfr = tw_get_description(id = current$qid, language = "fr", cache = TRUE)
    )
  })


# |-- Translate descriptions ---------------------------------------------

wikidata_descriptions_long <- wikidata_descriptions %>% 
  pivot_longer(cols = starts_with("D")) %>% 
  mutate(nchar = nchar(value)) %>% 
  group_by(id) %>% 
  # find the longest string -> will be input for translation
  mutate(max_nchar = ifelse(nchar == max(nchar, na.rm = TRUE), 1, 0)) %>% 
  select(id, name, contains("nchar"))

wikidata_descriptions <- wikidata_descriptions %>% 
  mutate(needs_translation = ifelse(rowSums(is.na(wikidata_descriptions)) > 0, TRUE, FALSE),
         no_trans_possible = ifelse(rowSums(is.na(wikidata_descriptions)) == 4, TRUE, FALSE))

# make buckets if translation is possible or not
all_desc_filled <- wikidata_descriptions %>% filter(needs_translation == FALSE)
no_desc_from_wikidata <- wikidata_descriptions %>% filter(no_trans_possible == TRUE)
translation_possible <-  wikidata_descriptions %>% 
  filter(needs_translation == TRUE, no_trans_possible == FALSE)

translations <- translation_possible %>% 
  select(-contains("trans")) %>% 
  pmap_df(function(...) {
    current <- tibble(...)
    current_meta <- wikidata_descriptions_long %>% filter(id == current$id)
    
    # get the one with the longest string, will be input for translation;
    # include also a slice if there there are multiple string with same length
    translate_from <- current_meta %>% filter(max_nchar == 1) %>% slice(1) %>% pull(name)
    
    # get the languages/cols that need a translation
    # can be found in meta df: dont have an nchar
    translate_to <- current_meta %>% filter(is.na(nchar)) %>% pull(name)
    
    map2_df(translate_from, translate_to, function(from, to) {
      
      # get text to translate
      text <- pull(current, {{from}})
      
      # translate target_lang to deepl abbr
      to_deepl <- replace(to, to == "Dde", "DE")
      to_deepl <- replace(to_deepl, to_deepl == "Den", "EN")
      to_deepl <- replace(to_deepl, to_deepl == "Dfr", "FR")
      to_deepl <- replace(to_deepl, to_deepl == "Des", "ES")
      
      # call Deepl API
      current %>%
        mutate({{to}}:= deeplr::translate2(text = text, target_lang = to_deepl, auth_key = Sys.getenv("DEEPL_API_KEY")))
    })
  }) %>% 
  pivot_longer(cols = 2:last_col()) %>% 
  filter(!is.na(value)) %>% distinct() %>% 
  pivot_wider(id_col = "id", names_from = "name", values_from = "value") %>% 
  mutate(across(starts_with("D"), ~ as.character(.)))

wikidata_descriptions_final <-
  bind_rows(all_desc_filled, no_desc_from_wikidata, translations) %>% 
  distinct(id, Dde, Den, Dfr, Des)

final_descriptions <- input %>% 
  left_join(wikidata_descriptions_final, by = "id") %>% 
  mutate(
    Dde = ifelse(is.na(Dde), "Organisation", Dde),
    Den = ifelse(is.na(Den), "organisation", Den),
    Dfr = ifelse(is.na(Dfr), "organisation", Dfr),
    Des = ifelse(is.na(Des), "organización", Des)
  ) %>% 
  select(id, starts_with("D"))

testthat::test_that(
  desc = "all rows/cols filled",
  expect_equal(final_descriptions %>% filter_at(vars(all_of(names(.))), any_vars(is.na(.))) %>% nrow(), 0)
)


# |- Wikipedia Sitelinks --------------------------------------------------

wikipedia_sitelinks <- wikidata %>% 
  pmap_df(function(...) {
    current <- tibble(...)
    tmp <- tibble(
      id = current$id,
      Sdewiki = tw_get_wikipedia(id = current$qid, language = "de", cache = TRUE),
      Senwiki = tw_get_wikipedia(id = current$qid, language = "en", cache = TRUE),
      Sfrwiki = tw_get_wikipedia(id = current$qid, language = "fr", cache = TRUE),
      Seswiki = tw_get_wikipedia(id = current$qid, language = "es", cache = TRUE)
    )
  }) %>% 
  mutate(
    Sdewiki = str_extract(Sdewiki, "(?<=\\/wiki\\/).*"),
    Senwiki = str_extract(Senwiki, "(?<=\\/wiki\\/).*"),
    Sfrwiki = str_extract(Sfrwiki, "(?<=\\/wiki\\/).*"),
    Seswiki = str_extract(Seswiki, "(?<=\\/wiki\\/).*"),
    Sdewiki = ifelse(!is.na(Sdewiki), paste0(config$import_helper$single_quote, Sdewiki, config$import_helper$single_quote), Sdewiki),
    Senwiki = ifelse(!is.na(Senwiki), paste0(config$import_helper$single_quote, Senwiki, config$import_helper$single_quote), Senwiki),
    Sfrwiki = ifelse(!is.na(Sfrwiki), paste0(config$import_helper$single_quote, Sfrwiki, config$import_helper$single_quote), Sfrwiki),
    Seswiki = ifelse(!is.na(Seswiki), paste0(config$import_helper$single_quote, Seswiki, config$import_helper$single_quote), Seswiki)
  )


# |- Instance of ----------------------------------------------------------

wikidata_instances <- wikidata %>%
  
  pmap_df(function(...) {
    current <- tibble(...)
    #print(current)
    tmp <- tw_get_property(id = current$qid, p = "P31", language = "de", cache = TRUE)
    if (nrow(tmp) > 0) {
      tmp <- tmp %>% mutate(label = tw_get_label(value))
    }
    tmp
  })

#wikidata_instances %>% count(label, sort = T) %>% View

# Create Statements -------------------------------------------------------

input_statements <- input %>% 
  distinct(id, item) %>% 
  left_join(final_labels, by = "id") %>% 
  left_join(final_descriptions, by = "id") %>% 
  left_join(wikipedia_sitelinks, by = "id") %>% 
  add_statement(statements, "instance_of_event") %>% 
  add_statement(statements, "research_project") %>% 
  add_statement(statements, "research_area") %>% 
  # add wikidata qid
  left_join(input %>% select(id, Swikidatawiki = wikidata) %>% 
              mutate(Swikidatawiki = ifelse(!is.na(Swikidatawiki), paste0(config$import_helper$single_quote, Swikidatawiki, config$import_helper$single_quote), Swikidatawiki)), by = "id") %>% 
  mutate(P728 = id)

# Prepare import ----------------------------------------------------------

import <- input_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*")) %>% 
  filter(!is.na(value)) %>% 
  distinct()

import


# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/events.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_clubs",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# Fetch Factgrid IDs ------------------------------------------------------

query <- 
  '
SELECT ?item ?itemLabel ?id ?idLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P131 wd:Q400012.
  OPTIONAL { ?item wdt:P728 ?id. }
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))


factgrid_qids <- input_statements %>% distinct(id, Lde) %>% 
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



# Changes -----------------------------------------------------------------

long_for_quickstatements <- function(data, start_at = 2) {
  data %>%
    tidyr::pivot_longer(cols = start_at:last_col(), names_to = "property", values_to = "value") %>%
    # fix helper with two instances_of:
    dplyr::mutate(property = stringr::str_remove(property, "_.*")) %>%
    dplyr::filter(!is.na(value)) %>%
    dplyr::distinct()
}

input_fresh <- entities_raw %>% 
  filter(event == 1) %>% 
  distinct(id, name)
import <- factgrid_qids %>% 
  distinct(id, name = Lde, item = external_id) %>% 
  left_join(input_fresh, by = c("id")) %>% 
  mutate(same_name = name.x == name.y,
         name = ifelse(same_name == FALSE, name.y, name.x),
         Lde = name,
         Len = name,
         Lfr = name, 
         Les = name,
         Dde = "Ereignis im Zusammenhang mit LGBTIQ*-Geschichte",
         Den = "Event related to LGBTIQ* history",
         Dfr = "Événement en rapport avec l'histoire LGBTIQ",
         Des = "Evento relacionado con la historia de LGBTIQ*") %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  long_for_quickstatements()
  

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/update_events.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_update_events",
  api.submit = T,
  quickstatements.url = config$connection$quickstatements_url
)
