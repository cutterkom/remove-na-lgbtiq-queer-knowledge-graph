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
url <- "https://de.wikipedia.org/wiki/Liste_der_Denkm%C3%A4ler_f%C3%BCr_homosexuelle_Opfer_des_Nationalsozialismus"

list_of_memorials_tidy <- tw_get_wikipedia_page_links(url = url)
  

# start <- tibble()
# 
# read_html(url) %>% html_element(".wikitable") %>%
#   {
#   data.frame(bgcolor = html_nodes(., '.image') %>% html_attr('href', default = ""), 
#              html_table(.))
#   } %>% View
# 
# tableee <- read_html(url) %>% html_node(".wikitable")
# tableee %>%
#   map_dfr(tableee,
#           ~ tibble(#img = html_nodes(., '.image') %>% html_attr('href') %>% html_text()
#               dsf = paste0("",html_text(html_nodes(.,"image")))
#             )
#   )
# 
# read_html(url) %>% html_element(".wikitable") %>% 
#   map_df(
#     item = html_elements("a") %>% html_attr("href") 
#   )
#   html_elements("a") %>% html_attr("href") %>% 
#   as_tibble() %>% 
#   filter(str_detect(value, "Datei")) %>% 
#   distinct()


list_of_memorials_raw <- read_html(url) %>% html_table(convert = F)
list_of_memorials_raw <- list_of_memorials_raw[[1]]
list_of_memorials <- list_of_memorials_raw %>% 
  separate_rows(Ort, sep = ",") %>% 
  mutate(Ort = trimws(Ort)) %>% 
  janitor::clean_names() %>% 
  rowid_to_column() %>% 
  group_by(name, jahr) %>% 
  filter(rowid == min(rowid)) %>% 
  ungroup()

list_of_memorials %>% 
  select(name, jahr, ort, inschrift_bemerkung, initiative_realisierung) %>% 
  clipr::write_clip()

list_of_memorials %>% clipr::write_clip()

openrefine_ex <- rrefine::refine_export("memorials") %>% 
  rowid_to_column() %>% 
  select(rowid, contains("wikidata"))


import_raw <- list_of_memorials %>% 
  bind_cols(openrefine_ex) %>% 
  mutate(item = case_when(
    !is.na(wikidata_item) ~ wikidata_item,
    TRUE ~ paste0("CREATE_", rowid...1)
  )) %>% 
  
  select(item, 
         name, 
         jahr, 
         wikidata_ort,
         wikidata_item, 
         wikidata_initative, 
         inschrift_bemerkung) %>% 
  ungroup()

import_raw


# Create statements -------------------------------------------------------

# |- Labels

input_labels <- import_raw %>% 
  mutate(
    Lde = name,
    Len = deeplr::translate2(text = Lde, target_lang = "en", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY")),
    Lfr = deeplr::translate2(text = Lde, target_lang = "fr", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY")),
    Les = deeplr::translate2(text = Lde, target_lang = "es", source_lang = "de", auth_key = Sys.getenv("DEEPL_API_KEY"))
  ) %>% 
  select(item, Lde, Len, Lfr, Les)


# |- Descriptions

desc_translations <- import_raw %>% 
  select(item) %>%
  mutate(
    Dde = "LGBT-Gedenkstätte",
    Den = "LGBT memorial",
    Dfr = "Mémorial pour les LGBT",
    Des = "Memorial LGBT"
  )

  
input_statements <- import_raw %>% 
  select(item, jahr, wikidata_ort, wikidata_initative, inschrift_bemerkung) %>% 
  left_join(input_labels, by = "item") %>% 
  left_join(desc_translations, by = "item") %>% 
  mutate(P31_1 = "Q29469577",
         P31_2 = "Q5003624",
         P31_3 = "Q1885014",
         P571 =  paste0("+", jahr, "-00-00T00:00:00Z/", 9),
         P131 = wikidata_ort,
         P180 = case_when(
           str_detect(Lde, "Rosa Winkel") ~ "Q165371",
           TRUE ~ NA_character_
         )#,
         #P1684 = inschrift_bemerkung
         )


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



# get back ----------------------------------------------------------------

# reminds of

#erinnert an
P547 = "Q718858"


# urheber P170 # Künsterl

# more --------------------------------------------------------------------

# dachau: 2. org gruppe dazu

