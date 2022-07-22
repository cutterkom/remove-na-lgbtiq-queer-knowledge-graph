
library(tidyverse)
library(tidywikidatar)
library(kabrutils)
library(WikidataR)


# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "wikidata")

# Caching for tidywikidatar
tw_enable_cache()
tw_set_cache_folder(path = fs::path(fs::path_home_r(), "R", "tw_data"))
tw_set_language(language = "de")
tw_create_cache_folder(ask = FALSE)


# Get data ----------------------------------------------------------------

url <- "https://de.wikipedia.org/wiki/Kategorie:LGBT-Zeitschrift"
permalink <- "https://de.wikipedia.org/w/index.php?title=Kategorie:LGBT-Zeitschrift&oldid=171321850"

input_raw <- tw_get_wikipedia_category_members(url = url) %>% 
  filter(!str_detect(wikipedia_title, "Liste von")) %>% 
  mutate(name = 
           case_when(
             str_detect(title_url, "\\(") ~ str_extract(title_url, ".*(?=\\s\\()"),
             TRUE ~ title_url))

instance_df <- input_raw %>% 
  pull(qid) %>% 
  tw_get_property(p = "P31")

labels_df <- input_raw %>% 
  pull(qid) %>% 
  map_df(., function(qid) {
    tibble(
      qid = qid,
      Len = tw_get_label(id = qid, language = "en"),
      Lde = tw_get_label(id = qid, language = "de"))
  }) %>% 
  mutate(
    Len = ifelse(is.na(Len), Lde, Len),
    Lde = ifelse(is.na(Lde), Len, Lde)
  ) %>% 
  mutate(Len = 
           case_when(
             str_detect(Len, "\\(") ~ str_extract(Len, ".*(?=\\s\\()"),
             TRUE ~ Len),
         Lde = 
           case_when(
             str_detect(Lde, "\\(") ~ str_extract(Lde, ".*(?=\\s\\()"),
             TRUE ~ Lde))

descriptions_df <- input_raw %>% 
  pull(qid) %>% 
  map_df(., function(qid) {
    tibble(
      qid = qid,
      Den = tw_get_description(id = qid, language = "en", cache = F),
      Dde = tw_get_description(id = qid, language = "de", cache = F))
  }) %>% 
  mutate(Den = ifelse(Den %in% c("periodical literature", "magazine"), NA, Den),
         Dde = ifelse(Dde == "Zeitschrift", NA, Dde)) %>% 
  # translation with Deepl API
  # quite complicated, because deeplr has some problems within case_when, 
  # but whatever, then the stupid complicated way
  pmap_df(function(...) {
    current <- tibble(...)
    text_en <- current$Den
    text_de <- current$Dde
    
    tmp <- tibble(
      qid = current$qid
    ) %>% 
      mutate(
        Den = 
          case_when(
            is.na(text_de) & is.na(text_en) ~ "LGBT magazine",
            is.na(text_en) & !is.na(text_de) ~ "translation",
            TRUE ~ text_en),
        Dde = 
          case_when(
            is.na(text_de) & is.na(text_en) ~ "LGBT-Zeitschrift",
            is.na(text_de) & !is.na(text_en) ~ "translation",
            TRUE ~ text_de)
      )
    
    if(tmp$Den == "translation") {
      translation_to_en <- deeplr::translate2(text = text_de, target_lang = "en", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY"))
      tmp <- tmp %>% mutate(Den = translation_to_en)
      tmp
    } else {
      tmp
    }
    
    if(tmp$Dde == "translation") {
      translation_to_de <- deeplr::translate2(text = text_en, target_lang = "de", source_lang = "en", auth_key = Sys.getenv("DEEPL_API_KEY"))
      tmp <- tmp %>% mutate(Dde = translation_to_de)
      tmp
    } else {
      tmp
    }
    tmp
  })


# Create statements -------------------------------------------------------

import <- input_raw %>% 
  select(qid) %>% 
  left_join(labels_df, by = "qid") %>% 
  left_join(descriptions_df, by = "qid") %>% 
  add_statement(statements, "genre_is_lgbt_magazine") %>% 
  add_statement(statements, "instance_is_magazine") %>% 
  select(item = qid, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  mutate(
    Len = paste0('"', Len, '"'),
    Lde = paste0('"', Lde, '"'),
    Dde = paste0('"', Dde, '"'),
    Den = paste0('"', Den, '"')) %>% 
  arrange(item) %>% 
  long_for_quickstatements()

import

import <- import %>% 
  # create a source for genre lgbt media/lgbt magazine
  bind_cols(tibble(source_statement_1 = "S887",
                   source_statement_1_value = "Q113172346",
                   source_statement_2 = "source_statement_143",
                   source_statement_2_value = "Q48183",
                   source_statement_3 = "S4656",
                   source_statement_3_value = paste0('"', permalink, '"')) ) %>% 
  mutate(
    source_statement_1 = ifelse(property == "P136", source_statement_1, ""),
    source_statement_2 = ifelse(property == "P136", source_statement_2, ""),
    source_statement_3 = ifelse(property == "P136", source_statement_3, ""),
    source_statement_1_value = ifelse(property == "P136", source_statement_1_value, ""),
    source_statement_2_value = ifelse(property == "P136", source_statement_2_value, ""),
    source_statement_3_value = ifelse(property == "P136", source_statement_3_value, "")
  )
  

add <- import %>% 
  filter(property == "P136")

rm <- import %>% 
  filter(property == "P136") %>% 
  mutate(item = paste0("-", item))
  
csv_file <- tempfile(fileext = "csv")
write.table(add, csv_file, row.names = FALSE, quote = FALSE, sep = "\t")
fs::file_show(csv_file)  
