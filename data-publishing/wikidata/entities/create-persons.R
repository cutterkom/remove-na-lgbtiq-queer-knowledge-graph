library(tidyverse)
library(kabrutils)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "wikidata")


# Get data ----------------------------------------------------------------

query_file <- "data-publishing/factgrid/queries/persons_factgrid_wikidata.rq"
query <- readr::read_file(query_file)

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint)

query_res <- query_res %>% 
  mutate(across(c(fg_item, starts_with("wd_")), ~extract_id(.))) %>% 
  mutate(across(c(fg_itemLabel, fg_itemDescription, fg_itemAltLabel), ~remove_lang(.)))

not_in_wd <- query_res %>% 
  filter(is.na(wd_item))

# Create Statements -------------------------------------------------------

input_raw <- not_in_wd %>% 
  # get all clubs
  filter(str_detect(instance, "Q7")) %>% 
  # extract IDs
  mutate(across(c(fg_item, starts_with("wd_")), ~extract_id(.))) %>% 
  mutate(across(c(fg_itemLabel, fg_itemDescription, fg_itemAltLabel), ~remove_lang(.)))

remove <- c("Josef", "Max")

input_raw <- input_raw %>% 
  filter(!fg_itemLabel %in% remove) %>% 
  mutate(fg_itemDescription = ifelse(str_detect(fg_itemDescription, "omosexuelles NS-Opfe"), "Homosexuelles NS-Opfer", fg_itemDescription)) %>% 
  filter(fg_itemDescription != "Autor*in", fg_itemDescription != "Plakatgestalter*in") 

# Labels ------------------------------------------------------------------


input_labels <- input_raw %>% 
  mutate(
    Lde = fg_itemLabel,
    Les = fg_itemLabel,
    Len = fg_itemLabel,
    Lfr = fg_itemLabel,
    Lbar = fg_itemLabel
  ) %>% 
  distinct(fg_item, Lde, Len, Lfr, Les, Lbar)


# Alias -------------------------------------------------------------------

alias <- input_raw %>% 
  select(fg_item, fg_itemAltLabel) %>% 
  separate_rows(fg_itemAltLabel, sep = ", ") %>% 
  mutate(fg_itemAltLabel = trimws(fg_itemAltLabel)) %>% 
  mutate(Ade = fg_itemAltLabel,
         Aen = Ade, 
         Afr = Ade,
         Aes = Ade,
         Abar = Ade) %>% 
  distinct()


# Descriptions ------------------------------------------------------------

desc_translations <- input_raw %>% 
  select(fg_item, Dde = fg_itemDescription) %>%
  mutate(
    #Dde = ifelse(Dde == "Verein", "LGBT-Verein", Dde),
    Den = deeplr::translate2(text = Dde, target_lang = "en", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY")),
    Dfr = deeplr::translate2(text = Dde, target_lang = "fr", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY")),
    Des = deeplr::translate2(text = Dde, target_lang = "es", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY"))
  )

# |- Import translated labels back in Factgrid ----------------------------

import_fg <- desc_translations %>% rename(item = fg_item) %>% long_for_quickstatements()

csv_file <- tempfile(fileext = "csv")

write_wikibase(
  items = import_fg$item,
  properties = import_fg$property,
  values = import_fg$value,
  format = "csv",
  format.csv.file = csv_file
)

fs::file_show(csv_file)


# Create statements -------------------------------------------------------

input_statements <- input_raw %>% 
  distinct(fg_item) %>% 
  mutate(item = paste0("CREATE_", fg_item)) %>% 
  left_join(input_labels, by = "fg_item") %>% 
  left_join(desc_translations, by = "fg_item") %>% 
  left_join(alias, by = "fg_item") %>% 
  distinct() %>% 
  add_statement(statements, "instance_of_human") %>% 
  add_statement(statements, "factgrid_id", qid_from_row = T, col_for_row_content = "fg_item") %>% 
  mutate(
    across(P8168, ~paste0(config$import_helper$single_quote, .x , config$import_helper$single_quote))
  )


import <- input_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^A", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  arrange(item) %>% 
  long_for_quickstatements()

csv_file <- tempfile(fileext = "csv")

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = csv_file
)

fs::file_show(csv_file)
