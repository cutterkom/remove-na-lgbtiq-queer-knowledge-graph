
library(tidyverse)
library(kabrutils)
library(googlesheets4)
library(tidygeocoder)
library(testdat)



# config ------------------------------------------------------------------

output_dir <- "data-publishing/factgrid/data/"

orte_raw <- read_sheet("https://docs.google.com/spreadsheets/d/14wtFqTTJ9n2hpuZIj6I3lVXH5xG4jlr3tr1vkQsJuSU/edit#gid=0", 
                       sheet = "Lokale - explizit homosexuell") %>% 
  janitor::clean_names() %>% 
  filter(!is.na(name)) %>% 
  select(-original) %>% 
  mutate(name = trimws(name))

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
    Lde = paste(name, munich_de, sep = ", "),
    # Len: English
    Len = paste(name, munich_en, sep = ", "),
    # Lfr: French
    Lfr = paste(name, munich_fr, sep = ", "),
    # Les: Spanish
    Les = paste(name, munich_es, sep = ", "),
    
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


entities_db %>% count(id, name, sort = T)

# Create CSV for Quickstatements Import -----------------------------------

import <- orte_as_statements %>% 
  select(starts_with("L"), starts_with("D"), starts_with("P"), starts_with("S"))

import



# Import der Adressen -----------------------------------------------------

# Vorlage https://database.factgrid.de/wiki/Item:Q14578

address_statements <- orte %>% 
  distinct(adresse) %>% 
  filter(!is.na(adresse)) %>% 
  # keine valide Adresse
  filter(adresse != "Ecke Herrn-/Hildegardstraße") %>% 
  left_join(orte_as_statements %>% distinct(name, P48, P111), by = c("adresse" = "P111")) %>% 
  mutate(
    # Labels in multiple languages
    # Lde: German
    Lde = paste(munich_de, adresse, sep = ", "),
    # Len: English
    Len = paste(munich_en, adresse, sep = ", "),
    # Lfr: adresse
    Lfr = paste(munich_fr, adresse, sep = ", "),
    # Les: Spanish
    Les = paste(munich_es, adresse, sep = ", "),
    
    Dde = paste("Straße in", munich_de),
    Den = paste("Street in", munich_en),
    Dfr = paste("Rue en", munich_fr),
    Des = paste("Calle en", munich_es),
    # Ort: in München
    P47 = "Q10427",
    
    # Straße oder Platz
    P522 = str_remove_all(adresse, "[:digit:]")
    ) %>% 
    rename(    
      # Adressierung
      P153 = adresse)

address_statements_import <- address_statements %>% 
  select(starts_with("L"), starts_with("D"), starts_with("P")) %>% 
  distinct() %>% 
  mutate(qid = "", .before = "Lde")


address_statements_import %>% write_csv(paste0(output_dir, "location-addresses.csv"))
