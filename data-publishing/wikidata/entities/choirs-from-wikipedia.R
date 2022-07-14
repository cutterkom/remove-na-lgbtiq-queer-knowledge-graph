library(tidyverse)
library(kabrutils)
library(WikidataR)
library(SPARQL)
library(tidywikidatar)
library(rvest)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "wikidata")

url <- "https://de.wikipedia.org/wiki/Liste_homosexueller_Ch%C3%B6re_im_deutschen_Sprachraum"
page <- read_html(url)


input <- page %>% html_table()
input <- input[[1]]  
input <- input %>% 
  janitor::clean_names() %>% 
  mutate(name = str_remove(name, "\\[.*"),
         jahr = str_extract(grundungsjahr, "[0-9]{4}"))

input %>% clipr::write_clip()

input <- input %>% 
  left_join(rrefine::refine_export("wikipedia-choires") %>% 
              select(name, contains("wikidata")), by = "name") %>% 
  rowid_to_column() %>% 
  mutate(name = str_remove(name, ",.*"),
         item = paste0("CREATE_", rowid))


# Create statements -------------------------------------------------------

# |- Labels

input_labels <- input %>% 
  mutate(
    Lde = name,
    Len = name,
    Lfr = name,
    Les = name
  ) %>% 
  select(item, Lde, Len, Lfr, Les)


# |- Descriptions

desc_translations <- input %>% 
  mutate(
    Dde = 
      case_when(
        besetzung == "Frauen" ~ paste0("Lesbenchor in ", ort),
        besetzung == "Männer" ~ paste0("Schwulenchor in ", ort),
        TRUE ~  paste0("LGBT-Chor in ", ort)
      ),
    Den = 
      case_when(
        besetzung == "Frauen" ~ paste0("Lesbian choir in ", ort),
        besetzung == "Männer" ~ paste0("Gay choir in ", ort),
        TRUE ~  paste0("LGBT choir in ", ort)
      ),
    Dfr = 
      case_when(
        besetzung == "Frauen" ~ paste0("Chœur de lesbiennes à ", ort),
        besetzung == "Männer" ~ paste0("Chœur de gay à ", ort),
        TRUE ~  paste0("LGBT Chœur à ", ort)
      ),
    Des = 
      case_when(
        besetzung == "Frauen" ~ paste0("Coro de lesbianas en  ", ort),
        besetzung == "Männer" ~ paste0("Coro gay en ", ort),
        TRUE ~  paste0("LGBT Coro en ", ort)
      )
  ) %>% 
  select(item, Dde, Den, Dfr, Des)



# final statements --------------------------------------------------------

input_statements <- input %>% 
  select(item, wikidata_item, P131 = wikidata_ort, jahr) %>% 
  mutate(item = 
           case_when(
             !is.na(wikidata_item) ~ wikidata_item,
             TRUE ~ item
           )) %>% 
  select(-wikidata_item) %>% 
  left_join(input_labels, by = "item") %>% 
  left_join(desc_translations, by = "item") %>% 
  add_statement(statements, "instance_of_lgbt_choir") %>% 
  add_statement(statements, "state_is_germany") %>% 
  # start
  mutate(P571 =  paste0("+", jahr, "-00-00T00:00:00Z/", 9))

import <- input_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  arrange(item) %>% 
  long_for_quickstatements()

import



csv_file <- tempfile(fileext = "csv")

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = csv_file
)

fs::file_show(csv_file)



# schweiz ändern bern zürich
