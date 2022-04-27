# title: Data Import and Data Modeling of Bars in Munich to Factgrid
# desc: In order to import data to factgrid I use the Wikibase Quickstatement CSV format. 
# input: google sheet with locations in Munich relevant especially for gay history
# output: Factgrid Items


library(tidyverse)
library(kabrutils)
library(googlesheets4)
library(tidygeocoder)
library(testdat)


# config ------------------------------------------------------------------

output_dir <- "data-publishing/factgrid/data/"


# Import raw data ---------------------------------------------------------

orte_raw <- read_sheet("https://docs.google.com/spreadsheets/d/14wtFqTTJ9n2hpuZIj6I3lVXH5xG4jlr3tr1vkQsJuSU/edit#gid=0", 
                       sheet = "Lokale - explizit homosexuell") %>% 
  janitor::clean_names() %>% 
  filter(!is.na(name)) %>% 
  select(-original)

orte_raw$notiz <- purrr::modify_if(
  orte_raw$notiz, 
  ~ length(.) == 0, 
  ~ NA_character_
)

orte <- orte_raw %>% 
  mutate(
    notiz = as.character(notiz),
    adresse = str_replace(adresse, "tr.", "traße"),
    city = "München") %>% 
  geocode(street = adresse, city = city, method = "osm", lat = latitude, long = longitude) %>% 
  #, full_results = TRUE)#, lat = latitude, long = longitude) %>% 
  mutate(
    latitude = ifelse(is.na(adresse), NA_character_, latitude),
    longitude = ifelse(is.na(adresse), NA_character_, longitude),
    quelle = ifelse(quelle == "#N/A", NA_character_, quelle))


# Create Quickstatements --------------------------------------------------

munich_de <- "München"
munich_en <- "Munich"
munich_fr <- "Munich"
munich_es <- "Múnich"

Dde <- "Gaststätte in München, LGBT-Szene-Lokal"
Den <- "Restaurant in Munich, LGBT scene local"
Dfr <- "Restaurant à Munich, scène LGBT locale"
Des <- "Restaurante en Munich, local de escena LGBT"


# Documentation: https://www.wikidata.org/wiki/Help:QuickStatements#Command_sequence_syntax

orte_as_statements <- orte %>% 
  mutate(
    # Labels in multiple languages
    # Lde: German
    Lde = paste('"', name, munich_de, '"', sep = '', ''),
    # Len: English
    Len =  paste('"', name, munich_en, '"', sep = '', ''),
    # Lfr: French
    Lfr =  paste('"', name, munich_fr, '"', sep = '', ''),
    # Les: Spanish
    Les =  paste('"', name, munich_es, '"', sep = '', ''),
    
    Dde = Dde,
    Den = Den,
    Dfr = Dfr,
    Des = Des,
    
    # format coordinates: @LAT/LON
    P48 = 
      case_when(
        !is.na(latitude) ~ paste0("@", latitude, "/", longitude),
        TRUE ~ NA_character_
      ),
    
    # Ort: in München
    P47 = "Q10427",
    
    # ist ein Gasthof
    P2 = "Q40454",
    
    # Treffpunkt für
    
    P726 = 
      case_when(
        type == "gay" ~ "Q399989",
        type == "lesbian" ~ "Q399990",
        TRUE ~ "Q399988"
      ),
    
    S726 = quelle,
    
    # Forschungsprojekt
    P131 = "Q400012",
    
    # Forschungsfeld
    P97 = "Q400013"
  
    ) %>% 
  rename(
    # Adresse in Worten
    P111 = adresse) %>% 
  select(-latitude, -longitude)

# Write entities in DB ----------------------------------------------------

# Every locations should also be an entity in the database and get assigned an internal entity_id

entities_db <- orte %>% 
  distinct(name) %>% 
  #bind_rows(tibble(name = "N.N.")) %>% 
  arrange(name) %>% 
  rowid_to_column() %>% 
  mutate(id = paste0("location_", rowid)) %>% 
  select(-rowid) %>% 
  left_join(orte, by = "name") %>% 
  # fix double N.N.
  mutate(id = ifelse(name == "N.N." & is.na(adresse), "location_86", id))

entities_db_import <- entities_db %>% 
  distinct(id, name) %>% 
  mutate(org = 0, person = 0, club = 0, location = 1)

test_that(
  desc = "uniqueness",
  expect_unique(c("id", "name"), data = entities_db_import)
)

# con <- connect_db("db_clean")
# DBI::dbAppendTable(con, "entities", entities_db_import)
# DBI::dbDisconnect(con); rm(con)

# Import der Adressen -----------------------------------------------------

# Quickstatement CSV formatting
# https://www.wikidata.org/wiki/Help:QuickStatements#CSV_file_syntax

# I replicated this item: https://database.factgrid.de/wiki/Item:Q14578

address_statements <- orte %>% 
  distinct(adresse) %>% 
  filter(!is.na(adresse)) %>% 
  # no valide address
  filter(adresse != "Ecke Herrn-/Hildegardstraße") %>% 
  left_join(orte_as_statements %>% distinct(name, P48, P111), by = c("adresse" = "P111")) %>% 
  mutate(
    # Labels in multiple languages
    # Lde: German
    Lde = paste0('"', munich_de, ', ', adresse, '"'),
    # Len: English
    Len = paste0('"', munich_en, ', ', adresse, '"'),
    # Lfr: adresse
    Lfr = paste0('"', munich_fr, ', ', adresse, '"'),
    # Les: Spanish
    Les = paste0('"', munich_es, ', ', adresse, '"'),
    
    Dde = paste0('"Straße in ' , munich_de, '"'),
    Den = paste0('"Street in ', munich_en, '"'),
    Dfr = paste0('"Rue en ', munich_fr, '"'),
    Des = paste0('"Calle en ', munich_es, ''),
    
    # Class/ Is an 
    P2 = "Q16200",
    
    # Ort: in München
    P47 = "Q10427",
    
    # Adressierung
    P153 = paste0('"', adresse, '"')
    )

address_statements_import <- address_statements %>% 
  select(starts_with("L"), starts_with("D"), starts_with("P")) %>% 
  distinct() %>% 
  mutate(qid = "", .before = "Lde")

address_statements_import %>% write_csv(paste0(output_dir, "location_addresses.csv"), quote = "needed")

#write_sheet(address_statements_import, ss = "Factgrid-Import", sheet = "location_addresses)

# Import der Lokale -------------------------------------------------------

import <- orte_as_statements %>% 
  select(starts_with("L"), starts_with("D"), starts_with("P"), starts_with("S"))

import
