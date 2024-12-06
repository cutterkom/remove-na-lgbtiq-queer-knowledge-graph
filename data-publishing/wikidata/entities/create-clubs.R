library(tidyverse)
library(kabrutils)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "wikidata")


# Get data ----------------------------------------------------------------

query_file <- "data-publishing/factgrid/queries/orgs_factgrid_wikidata.rq"
query <- readr::read_file(query_file)

fs::file_show(query_file)

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint)

query_res

not_in_wd <- query_res %>% 
  filter(is.na(wd_item))

not_in_wd %>% count(instanceLabel, sort = T)

# Create Statements -------------------------------------------------------

clubs <- not_in_wd %>% 
  # get all clubs
  filter(str_detect(instance, "Q266834")) %>% 
  # extract IDs
  mutate(across(c(fg_item, starts_with("wd_")), ~extract_id(.))) %>% 
  mutate(across(c(fg_itemLabel, fg_itemDescription, fg_itemAltLabel), ~remove_lang(.)))


# Labels ------------------------------------------------------------------


input_labels <- clubs %>% 
  mutate(
    Lde = fg_itemLabel,
    Les = fg_itemLabel,
    Len = fg_itemLabel,
    Lfr = fg_itemLabel,
    Lbar = fg_itemLabel
  ) %>% 
  select(fg_item, Lde, Len, Lfr, Les, Lbar)
  


# Descriptions ------------------------------------------------------------

# automated translation via deepl API
# meta_desc <- clubs %>% 
#   select(fg_item, fg_itemDescription) %>%
#   pivot_longer(cols = starts_with("D")) %>% 
#   mutate(nchar = nchar(value)) %>% 
#   group_by(id) %>% 
#   # find the longest string -> will be input for translation
#   mutate(max_nchar = ifelse(nchar == max(nchar, na.rm = TRUE), 1, 0)) %>% 
#   select(id, name, contains("nchar"))
# 
# wikidata_descriptions <- wikidata_descriptions %>% 
#   mutate(needs_translation = ifelse(rowSums(is.na(wikidata_descriptions)) > 0, TRUE, FALSE),
#          no_trans_possible = ifelse(rowSums(is.na(wikidata_descriptions)) == 4, TRUE, FALSE))
# 
# # make buckets if translation is possible or not
# all_desc_filled <- wikidata_descriptions %>% filter(needs_translation == FALSE)
# no_desc_from_wikidata <- wikidata_descriptions %>% filter(no_trans_possible == TRUE)
# translation_possible <-  wikidata_descriptions %>% 
#   filter(needs_translation == TRUE, no_trans_possible == FALSE)

desc_translations <- clubs %>% 
  select(fg_item, Dde = fg_itemDescription) %>%
  mutate(
    #Dde = ifelse(Dde == "Verein", "LGBT-Verein", Dde),
    Den = deeplr::translate2(text = Dde, target_lang = "en", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY")),
    Dfr = deeplr::translate2(text = Dde, target_lang = "fr", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY")),
    Des = deeplr::translate2(text = Dde, target_lang = "es", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY"))
  )
 
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

input_statements <- clubs %>% 
  mutate(item = paste0("CREATE_", fg_item)) %>% 
  left_join(input_labels, by = "fg_item") %>% 
  left_join(desc_translations, by = "fg_item") %>% 
  add_statement(statements, "instance_of_club") %>% 
  add_statement(statements, "legal_form_ev") %>% 
  add_statement(statements, "factgrid_id", qid_from_row = T, col_for_row_content = "fg_item") %>% 
  mutate(
    across(P8168, ~paste0(config$import_helper$single_quote, .x , config$import_helper$single_quote))
  )
  

import <- input_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
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

# ist ein Freiwilligen-Verein WD Q190740


# AIDS HILFEN 
# besteht aus https://www.wikidata.org/wiki/Property:P527




query <- 'SELECT ?Verein ?VereinLabel ?FactGrid_Objekt_ID WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de, [AUTO_LANGUAGE],en". }
  ?Verein wdt:P31 wd:Q988108.
  ?Verein wdt:P8168 ?FactGrid_Objekt_ID.
}
' 
query_res_wd <- query %>% 
  sparql_to_tibble(endpoint = config$connection_wikidata$sparql_endpoint)
  
update_add <- query_res_wd %>% 
  select(item = Verein, VereinLabel) %>% 
  mutate(across(c(item, starts_with("wd_")), ~extract_id(.))) %>% 
  mutate(
    P31 = 
      case_when(
        str_detect(VereinLabel, "Queer|queer|Homo|homo|rungsprojekt|lesb|Les|Leather|LSVD|rosa|wind|Schwu") ~ "Q64606659",
        TRUE ~ NA_character_
      )
  ) %>%
  select(item, P31) %>% 
  long_for_quickstatements()

update_add

csv_file <- tempfile(fileext = "csv")

write_wikibase(
  items = update_add$item,
  properties = update_add$property,
  values = update_add$value,
  format = "csv",
  format.csv.file = csv_file
)

fs::file_show(csv_file)



input_labels_new <- input_labels %>% 
  inner_join(query_res_wd %>% select(item = Verein, fg_item = FactGrid_Objekt_ID), by = c("fg_item")) %>% 
  mutate(item = extract_id(item), .before = "Lde") %>% 
  mutate(Aen = Len,
         Ade = Lde,
         Afr = Lfr,
         Aes = Les,
         Abar = Lbar) %>% 
  mutate(Lde = ifelse(str_detect(Lde, "e.V$"), paste0(Lde, "."), Lde),
         Len = ifelse(str_detect(Len, "e.V$"), paste0(Len, "."), Len),
         Les = ifelse(str_detect(Les, "e.V$"), paste0(Les, "."), Les),
         Lfr = ifelse(str_detect(Lfr, "e.V$"), paste0(Lfr, "."), Lfr),
         Lbar = ifelse(str_detect(Lbar, "e.V$"), paste0(Lbar, "."), Lbar)) %>% 
  mutate(Aen = str_remove(Aen, " e.V."),
         Ade = str_remove(Ade, " e.V."),
         Afr = str_remove(Afr, " e.V."),
         Aes = str_remove(Aes, " e.V."),
         Abar = str_remove(Abar, " e.V."))

import <- input_labels_new %>% 
  select(item, everything()) %>% 
  select(-fg_item) %>% 
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

