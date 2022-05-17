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

entities_per_type <- tbl(con, "entities_per_type") %>% 
  select(contains("id")) %>% 
  collect()

chronik <- tbl(con, "chronik") %>% collect()

DBI::dbDisconnect(con); rm(con)

input <- entities_raw %>% 
  filter(club == 1) %>% 
  distinct(id, name) %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid"), by = "id") %>% 
  mutate(
    item = ifelse(!is.na(external_id), external_id, paste0("CREATE_", id))
  ) %>% 
  distinct(id, name, item)


# Open Refine Input -------------------------------------------------------

# reconciliation in Open refine
input %>% 
  distinct(id, name) %>% clipr::write_clip()
# get results
openrefine_results <- rrefine::refine_export(project.id = "2215454787807")

input <- input %>% 
  left_join(openrefine_results, by = c("id", "name"))

wikidata <- input %>% distinct(id, wikidata) %>% filter(!is.na(wikidata)) %>% rename(qid = wikidata) %>% 
  filter(str_detect(qid, "Q"))


# Labels and Descriptions -------------------------------------------------

wikidata_descriptions <- wikidata %>% 
  pmap_df(function(...) {
    current <- tibble(...)
    tmp <- tibble(
      id = current$id,
      Dde = tw_get_description(id = current$qid, language = "de"),
      Den = tw_get_description(id = current$qid, language = "en"),
      Des = tw_get_description(id = current$qid, language = "es"),
      Dfr = tw_get_description(id = current$qid, language = "fr")
    )
  })

meta_desc <- wikidata_descriptions %>% 
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
    current_meta <- meta_desc %>% filter(id == current$id)
    
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



# Create Descriptions -----------------------------------------------------

labels_descriptions <- input %>% 
  left_join(wikidata_descriptions_final, by = "id") %>% 
  mutate(Lde = name, Len = name, Lfr = name, Les = name,
         Dde = ifelse(!is.na(Dde), Dde, "Verein"),
         Den = ifelse(!is.na(Den), Den, "Club"),
         Dfr = ifelse(!is.na(Dfr), Dfr, "Association"),
         Des = ifelse(!is.na(Des), Des, "Asociación")) %>% 
  select(id, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE))
  

# Wikipedia: Sitelinks ---------------------------------------------------

wikipedia_sitelinks <- wikidata %>% 
    pmap_df(function(...) {
      current <- tibble(...)
      
      tmp <- tibble(
        id = current$id,
        Sdewiki = tw_get_wikipedia(id = current$qid, language = "de"),
        Senwiki = tw_get_wikipedia(id = current$qid, language = "en"),
        Sfrwiki = tw_get_wikipedia(id = current$qid, language = "fr"),
        Seswiki = tw_get_wikipedia(id = current$qid, language = "es")
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
  

# City --------------------------------------------------------------------

location <- input %>% 
  inner_join(chronik, by = "id") %>%
  select(id, location) %>% 
  filter(!is.na(location)) %>% 
  add_statement(statements, "location_in_munich") %>% 
  distinct(id, P47)


# Target Group ------------------------------------------------------------

target_group <- 
  bind_rows(
    input %>% inner_join(chronik, by = "id") %>% select(id, group) %>% filter(group == "lesbian") %>% 
      add_statement(statements, "target_group_lesbian"),
    input %>% inner_join(chronik, by = "id") %>% select(id, group) %>% filter(group == "gay") %>% 
      add_statement(statements, "target_group_gay"),
    input %>% inner_join(chronik, by = "id") %>% select(id, group) %>% filter(group == "misc") %>% 
      add_statement(statements, "target_group_lgbt"),
    input %>% inner_join(chronik, by = "id") %>% select(id, group) %>% filter(group == "misc") %>% 
      add_statement(statements, "target_group_lesbian"),
    input %>% inner_join(chronik, by = "id") %>% select(id, group) %>% filter(group == "misc") %>% 
      add_statement(statements, "target_group_gay"),
    input %>% filter(str_detect(name, "Lesbenwoche")) %>% select(id) %>% add_statement(statements, "target_group_lesbian")
  )
  

# Statements --------------------------------------------------------------

input_statements <- input %>% 
  left_join(labels_descriptions, by = "id") %>% 
  add_statement(statements, "instance_of_organisation") %>% 
  rename(P2_1 = P2) %>% 
  add_statement(statements, "instance_of_club") %>% 
  left_join(target_group, "id") %>% 
  left_join(wikipedia_sitelinks, by = "id") %>% 
  left_join(location, by = "id") %>% 
  left_join(openrefine_results %>% distinct(id, P76 = lobid), by = "id") %>% 
  add_statement(statements, "research_area") %>% 
  add_statement(statements, "research_project")
  

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
  

# Write Wikibase ----------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/clubs.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_clubs",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# Fetch new Factgrid IDs --------------------------------------------------

query <- '
SELECT ?item ?itemLabel ?id WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  
  OPTIONAL {  }
  ?item wdt:P131 wd:Q400012.
  OPTIONAL { ?item wdt:P728 ?id }
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))


factgrid_qids <- input_statements %>%
  distinct(id, name) %>% 
  left_join(query_res %>% select(-id), by = c("name" = "itemLabel"))


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



# Add Forum ID to Factgrid ------------------------------------------------

import <- factgrid_qids %>% 
  distinct(external_id, id) %>% 
  add_statement(statements, "external_id_forum", qid_from_row = TRUE, col_for_row_content = "id") %>% 
  select(-id) %>% 
  rename(item = external_id) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*")) %>% 
  filter(!is.na(value)) %>% 
  distinct()
  
write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/clubs_forum_id.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_clubs",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# Arbeitsgebiet -----------------------------------------------------------
# Arbeitsgebiet P452

import <- input_statements %>% 
  # location zu Regional coverage
  # Field of work P452 = Target group 
  select(id, name, group, P429 = P47) %>% 
  distinct() %>% 
  left_join(factgrid_qids %>% select(id, item = external_id)) %>% 
  mutate(
    P452_2_aids = ifelse(str_detect(tolower(name), "aids"), "Q404295", NA),
    #P452_3_lesben = ifelse(str_detect(tolower(name), "frauen|lesbe|feminis|kofra|amazonen|intervention|rad und tat|lesbisch"), "Q399990", NA),
    # Wenn Lesbe, dann Frauenbewegung
    # Q245592
    P452_344_lgbt_move = ifelse(!is.na(group), "Q404296", NA),
    P452_6_lgbt_move = ifelse(str_detect(tolower(name), "fluss|lesmamas|queer"), "Q404296", NA),
    P452_6_lgbt_move = ifelse(str_detect(tolower(name), "fluss|lesmamas|queer"), "Q404296", NA),
    P452_4_feminism = ifelse(group == "lesbian", "Q245592", NA),
    # frauenbewegung
    P452_5_feminism = ifelse(str_detect(tolower(name), "frauen|lesbe|feminis|kofra|amazonen|intervention|rad und tat|lesbisch|lesben|tribunal"), "Q245592", NA),
    # schwulen- und lesbenbewegung
    P452_6 = ifelse(str_detect(tolower(name), "fluss|lesmamas|queer"), "Q404296", NA)
    ) %>% 
  select(item, contains("P")) %>% 
  select(-group) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*")) %>% 
  filter(!is.na(value)) %>% 
  distinct() %>% 
    filter(!is.na(item))


write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/clubs_movements.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_clubs",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


