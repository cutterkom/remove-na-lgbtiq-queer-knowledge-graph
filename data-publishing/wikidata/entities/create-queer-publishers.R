
library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "wikidata")


# get data ----------------------------------------------------------------

verlage <- readxl::read_excel("data-gathering/data/input/queere_verlage_elke_labels.xlsx") %>% 
  janitor::clean_names() %>% 
  filter(!is.na(queerer_verlag_ja_oder_nein)) %>% 
  filter(queerer_verlag_ja_oder_nein != "?") %>% 
  filter(queerer_verlag_ja_oder_nein != "klingt interessant - recherchieren")


verlage %>% clipr::write_clip()

refined <- rrefine::refine_export("queere-verlage") %>% 
  filter(!is.na(factgrid), !is.na(wikidata_first)) %>% 
  select(-anzahl_der_bucher)

new_in_wd <- refined %>% 
  filter(is.na(wikidata_first))


# Import new Publishers ---------------------------------------------------
input_labels <- new_in_wd %>% 
  mutate(
    Lde = publisher,
    Les = publisher,
    Len = publisher,
    Lfr = publisher,
    Lbar = publisher
  ) %>% 
  select(publisher, Lde, Len, Lfr, Les, Lbar)

input_descriptions <- new_in_wd %>% 
  mutate(
    Dde = 
      case_when(
        queerer_verlag_ja_oder_nein == "Frauen / Lesben" ~ "Feministischer Verlag aus Deutschland",
        TRUE ~ "LGBT-Verlag aus Deutschland"
      ),
    Des = case_when(
      queerer_verlag_ja_oder_nein == "Frauen / Lesben" ~ "Editorial feminista de Alemania",
      TRUE ~ "Editorial LGBT de Alemania"
    ),
    Den = case_when(
      queerer_verlag_ja_oder_nein == "Frauen / Lesben" ~ "Feminist publishing house from Germany",
      TRUE ~ "LGBT publisher from Germany"
    ),
    Dfr =   case_when(
      queerer_verlag_ja_oder_nein == "Frauen / Lesben" ~ "Maison d'édition féministe allemande",
      TRUE ~ "Éditeur LGBT allemand"
    )
  ) %>% 
  select(publisher, Dde, Den, Dfr, Des)
  



import <- new_in_wd %>% 
  left_join(input_labels, by = "publisher") %>% 
  left_join(input_descriptions, by = "publisher") %>% 
  rowid_to_column() %>% 
  mutate(item = paste0("CREATE_", rowid)) %>% 
  add_statement(statements, "factgrid_id", qid_from_row = T, col_for_row_content = "factgrid") %>% 
  add_statement(statements, "instance_is_publisher") %>% 
  add_statement(statements, "genre_is_lgbt_media") %>% 
  mutate(
    P921 = 
      case_when(
        queerer_verlag_ja_oder_nein == "Frauen / Lesben" ~ "Q6649",
        TRUE ~ "Q104969456"
      ),
    P921_2 = ifelse(P921 == "Q6649", "Q7252", NA_character_),
    P8168 = paste0('"', P8168, '"')
  ) %>% 
  add_statement(statements, "state_is_germany") %>% 
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


import <- refined %>% 
  select(item = wikidata_first, queerer_verlag_ja_oder_nein) %>% 
  add_statement(statements, "instance_is_publisher") %>% 
  add_statement(statements, "genre_is_lgbt_media") %>% 
  mutate(
    P921 = 
      case_when(
        queerer_verlag_ja_oder_nein == "Frauen / Lesben" ~ "Q6649",
        TRUE ~ "Q104969456"
      ),
    P921_2 = ifelse(P921 == "Q6649", "Q7252", NA_character_)
  ) %>% 
  add_statement(statements, "state_is_germany") %>% 
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
