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


# Get organisation not yet in Factgrid ------------------------------------

input <- entities_raw %>% 
  filter(org == 1, club == 0) %>% 
  anti_join(entities_per_type %>% filter(!is.na(publisher_book_id)) %>% distinct(id), by = "id") %>% 
  filter(is.na(party) | party == 0) %>% 
  distinct(id, name) %>% 
  left_join(el_matches %>% filter(!str_detect(external_id, "no_")), by = "id") %>% 
  #filter(external_id != "no_wikidata_id") %>% 
  #distinct(id, external_id_type, external_id) %>% 
  mutate(external_id = ifelse(external_id_type == "wikidata", str_extract(external_id, "Q[0-9]+"), external_id)) %>% 
  distinct(id, name, external_id, external_id_type) %>% 
  pivot_wider(id_cols = c("id", "name"), names_from = "external_id_type", values_from = "external_id") %>% 
  select(-`NA`)

input$gnd <- purrr::modify_if(input$gnd, ~ length(.) == 0, ~ NA_character_)
input$gnd <- as.character(input$gnd)
input$wikidata <- purrr::modify_if(input$wikidata, ~ length(.) == 0, ~ NA_character_)
input$wikidata <- as.character(input$wikidata)
input$factgrid <- purrr::modify_if(input$factgrid, ~ length(.) == 0, ~ NA_character_)
input$factgrid <- as.character(input$factgrid)

input

# Reconciliation ----------------------------------------------------------

input %>% clipr::write_clip("table")
input %>% write_csv("data-publishing/factgrid/data/df.csv")


or_rec_data <- rrefine::refine_export("entities_org") %>% 
  mutate(wd_is_same = wikidata == wikidata_new,
         factgrid = ifelse(!is.na(factgrid_new), factgrid_new, factgrid)) %>% 
  select(id, factgrid, wikidata = wikidata_new, gnd)

input <- input %>% 
  distinct(id, name) %>% 
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
      Dde = tw_get_description(id = current$qid, language = "de"),
      Den = tw_get_description(id = current$qid, language = "en"),
      Des = tw_get_description(id = current$qid, language = "es"),
      Dfr = tw_get_description(id = current$qid, language = "fr")
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


# |- Instance of ----------------------------------------------------------

wikidata_instances <- wikidata %>%
  pmap_df(function(...) {
    current <- tibble(...)
    tw_get_property(id = current$qid, p = "P31", language = "de") %>% 
      mutate(label = tw_get_label(value))
  })
wikidata_instances


# Wenn Frauenzentrum, dann diese OrgForm frauenze
# Was hat https://www.wikidata.org/wiki/Q28942424 für Klasse?

# Wenn Universität

# Wenn AG/Gruppe, Arbeitskreis

# Inititative Initiativgruppe Homosexualität

# Ministerium

# Stiftung

# AIDS-Hilfe -> Arbeitsgebiet AIDS

# Act up

# HAW-Frauen zu HAW dazu

# Bayerischer Rundfunk (BR), ARD, ZDF, SWR, WDR

# Bristol-Myers Squibb, GlaxoSmith--- Unternehmen, Arbeitsgebiet Pharma

# GlaxoWellcome -> Nachfolger davon GlaxosmithLine

# FHG Homosexualität und Geschichte Dachverband

# Forum der schwul-lesbischen Chöre Europas -> Arbeitsgebiet Chor/Musik
# Philhomoniker
# Rainbow Sound Orchestra Munich
# poster_author_235 Die Schrillmänner


# "Frauen" -> Zielgruppe Frauen

# bei homosexuellen Initiativen als Abkürzung nur Österreeich offenbar https://de.wikipedia.org/wiki/HOSI


# Kölner Lesben- und Schwulentag c/o Schulz -> c/o weg

# Kommunistische Partei als Partei

# poster_author_393 Medium Verlag als Verlag

# Als mit München, Münchner -> P47 München



# Stefanie Seibold Mensch keine Org




# Vorbereitungsgruppe Lesbenwoche hängt zusammen mit?

# chronik_182 HuK München zu HuK Allegemein verbinden

# chronik_466 Sozialreferat -> zu Stadt München hängen
# poster_author_114 Kulturreferat


# poster_author_214 LFC Leder und Fetisch - wie das hinterlegen? -> Schwul Zielgruppe


# poster_author_474

# BiNe – Bisexuelles Netzwerk  -> zielgruppe

# poster_author_71


# Geest-Verlag -> verlag