library(tidyverse)
library(kabrutils)
library(WikidataR)
library(SPARQL)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "wikidata")

# Get data ----------------------------------------------------------------

query_file <- "data-publishing/factgrid/queries/lokale-from-factgrid.rq"
query <- readr::read_file(query_file)

fs::file_show(query_file)

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint)

query_res

not_in_wd <- query_res %>% 
  filter(is.na(wd_item))

#not_in_wd %>% count(instanceLabel, sort = T)

in_wd <- query_res %>% 
  filter(!is.na(wd_item))

# Create Statements -------------------------------------------------------

input <-not_in_wd %>%  #query_res %>% 
  filter(!is.na(location))  %>% 
  filter(is.na(instanceLabel)) %>% 
  filter(!str_detect(fg_itemLabel, "N.N")) %>% 
  # extract IDs
  mutate(across(c(fg_item, starts_with("wd_")), ~extract_id(.))) %>% 
  mutate(across(c(fg_itemLabel, fg_itemDescription, fg_itemAltLabel), ~remove_lang(.))) %>% 
  mutate(fg_itemLabel = str_remove(fg_itemLabel, ", München"))

# Labels ------------------------------------------------------------------

input_labels <- input %>% 
  mutate(
    Lde = fg_itemLabel,
    Les = fg_itemLabel,
    Len = fg_itemLabel,
    Lfr = fg_itemLabel,
    Lbar = fg_itemLabel
  ) %>% 
  distinct(fg_item, Lde, Len, Lfr, Les, Lbar)

input_labels


# Description ------------------------------------------------------------------


input_descriptions <- input %>% 
  mutate(
    Dde = "LGBT-Bar in München",
    Des = "Bar LGBT en Múnich",
    Den = "LGBT bar in Munich",
    Dfr = "Bar LGBT à Munich",
    Dbar = "LGBT-Wirtschaft in Minga"
  ) %>% 
  distinct(fg_item, Dde, Den, Dfr, Des, Dbar)

input_descriptions


# Alias -------------------------------------------------------------------


input_alias <- input %>% 
  mutate(
    Ade = fg_itemAltLabel,
    Aes = fg_itemAltLabel,
    Aen = fg_itemAltLabel,
    Afr = fg_itemAltLabel,
    Abar = fg_itemAltLabel
  ) %>% 
  distinct(fg_item, Ade, Aen, Afr, Aes, Abar)

input_alias





# Create Statements -------------------------------------------------------

import <- input %>% 
  distinct(fg_item, wd_item) %>% 
  rowid_to_column() %>% 
  mutate(item = ifelse(is.na(wd_item), paste0("CREATE_", rowid), wd_item)) %>% 
  select(item, P8168 = fg_item) %>% 
  filter(item == "CREATE_4", !str_detect(item, "Q")) %>% 
  left_join(input_labels, by = c("P8168" = "fg_item")) %>% 
  left_join(input_descriptions, by = c("P8168" = "fg_item")) %>% 
  left_join(input_alias, by = c("P8168" = "fg_item")) %>% 
  add_statement(statements, "admin_location_munich") %>% 
  add_statement(statements, "state_is_germany") %>% 
  mutate(
    P8168 = paste0('"', P8168, '"'),
    P31 = 
           case_when(
            # str_detect(TreffpunktLabel, "Schwul") ~ "Q1043639",
             item == "Q405758" ~ "Q30324198",
             TRUE ~ "Q105321449"
           )
         ) %>% 
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
