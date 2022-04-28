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

# How to reach Factgrid SPARQL endpoint
endpoint <- "https://database.factgrid.de/sparql"
useragent <- paste("WDQS-Example", R.version.string)

# adding single and triple quotes
labels_desc <- c("Lde", "Len", "Lfr", "Les", "Dde", "Den", "Dfr", "Des")
single_quote <- '"'
double_quote <- '""'
triple_quote <- '"""'

# google spreadsheet
spreadsheet <- "1Pw_kBvrASJyJUjTTIciPlcPlBG-K52hjWMaoq_t39_c"

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
  left_join(entities_db %>% select(name, adresse, id), by = c("name", "adresse")) %>% 
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

# write_sheet(address_statements_import, ss = "Factgrid-Import", sheet = "location_addresses)

# next step: import that via Quickstatements on https://database.factgrid.de/quickstatements/#/batch

# Fetch new created Factgrid QIDs  ----------------------------------------

# Now, QIDs are produced. I need them locally in order to add them as to locations. 
# So I fetch them via SPARQL in German and join them to my import data

query <- '
SELECT ?item ?itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P2 wd:Q16200.
  ?item wdt:P47 wd:Q10427.
}
'


address_qid <- SPARQL(endpoint, query, curl_args = list(useragent = useragent))
address_qid <- tibble(address_qid$results) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|@de')) %>% 
  # entity_ids
  left_join(
    address_statements %>% select(Lde, id) %>% mutate(Lde = str_remove_all(Lde, '\"')),
    by = c("itemLabel" = "Lde")
  ) %>% 
  filter(!is.na(id))
  


# Import of locations  ----------------------------------------------------

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
  left_join(entities_db %>% select(id, name, adresse), by = c("name", "adresse")) %>% 
  left_join(address_qid %>% select(id, address_qid = item), by = "id") %>% 
  mutate(
    # Labels in multiple languages
    # Lde: German
    Lde = trimws(paste0( name, ', ', munich_de,  '')),
    # Len: English
    Len =  trimws(paste0( name, ', ', munich_en,  '')),
    # Lfr: French
    Lfr =  trimws(paste0( name, ', ', munich_fr,  '')),
    # Les: Spanish
    Les =  trimws(paste0( name, ', ', munich_es,  '')),
    
    Dde = Dde,
    Den = Den,
    Dfr = Dfr,
    Des = Des,
    
    # format coordinates: @LAT/LON
    # P48 = 
    #   case_when(
    #     !is.na(latitude) ~ paste0("@", latitude, "/", longitude),
    #     TRUE ~ NA_character_
    #   ),
    # 
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
    
    # Zielgruppe 
    
    P573 = 
      case_when(
        type == "gay" ~ "Q399989",
        type == "lesbian" ~ "Q399990",
        TRUE ~ "Q399988"
      ),
    
    S726 = quelle,
    
    # Forschungsprojekt
    P131 = "Q400012",
    
    # Forschungsfeld
    P97 = "Q400013",
    
    # Adresse as Item - if possible
    P208 = 
      case_when(
        !is.na(address_qid) ~ str_extract(address_qid, "Q[0-9]+"),
        TRUE ~ NA_character_
      ),
    
    # Forum External ID 
    P728 = id
    
  ) %>% 
  select(-latitude, -longitude, -address_qid) %>% 
  # entspricht Cafe Zehner
  filter(name != "Ze(c)hner-Diele") %>% 
  # add quotes
  mutate(
    across(labels_desc, ~paste0(single_quote, .x , single_quote)),
    across(P728, ~paste0(triple_quote, .x , triple_quote))
    )




import <- orte_as_statements %>% 
  mutate(
    # empty item -> create new
    qid = "", .before = "name") %>% 
  select(qid, starts_with("L"), starts_with("D"), starts_with("P")) %>% 
  distinct() %>% 
  
  # i guess i cant upload two items per property, at least not when making a new item 
  # have to add second address for those few items by hand
  group_by(Lde) %>% 
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  arrange(P208)
  
import[is.na(import)] <- ""


# Tests -------------------------------------------------------------------

test_that(
  desc = "Check that there is a P2 (is class of) property",
  expect_equal(import %>% select(P2) %>% ncol(), 1)
)

test_that(
  desc = "Check that there is a P2 (is class of) property",
  expect_unique(c(Lde), data = import)
)


import


# Write in Sheet ----------------------------------------------------------

write_sheet(import, ss = spreadsheet, sheet = "locations")
write.csv(import, paste0(output_dir, "locations.csv"), quote = F, row.names = F)

# Check those with multiple addresses -------------------------------------

orte_as_statements %>% count(id, sort = T) %>% 
  left_join(orte_as_statements) %>% View

# added notes manually


# Get QIDs and save in DB -------------------------------------------------

query <- 
'
#defaultView:Map
SELECT ?item ?itemLabel ?Address ?AddressLabel ?Geo WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P2 wd:Q40454.
  { ?item wdt:P726 wd:Q399989. }
  UNION
  {
    ?item wdt:P2 wd:Q40454;
      wdt:P726 wd:Q399990.
  }
  UNION
  {
    ?item wdt:P2 wd:Q40454;
      wdt:P726 wd:Q399988.
  }
  UNION
  {
    ?item wdt:P2 wd:Q40454;
      wdt:P726 wd:Q400014.
  }
  UNION
  {
    ?item wdt:P2 wd:Q40454;
      wdt:P726 wd:Q137530.
  }
  OPTIONAL {
    ?item wdt:P208 ?Address.
    ?Address wdt:P48 ?Geo.
  }
}
'

# get data and do some cleaning to get unique values
remove <- tibble(id = c("location_59"), item = c("<https://database.factgrid.de/entity/Q400387>"))
remove2 <- tibble(id = c("location_86"), Address = c("<https://database.factgrid.de/entity/Q400139>"))

location_qid <- SPARQL(endpoint, query, curl_args = list(useragent = useragent))
location_qid <- tibble(location_qid$results) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|')) %>%
  group_by(item) %>% 
  mutate(
    Address = glue::glue_collapse(Address, sep = ", "),
    AddressLabel = glue::glue_collapse(AddressLabel, sep = ", ")) %>% 
  ungroup() %>% 
  select(-Geo) %>% 
  distinct() %>% 
  right_join(entities_db, by = c("itemLabel" = "name")) %>% 
  mutate(item = ifelse(id == "location_86", "<https://database.factgrid.de/entity/Q400387>", item)) %>% 
  anti_join(remove, by = c("item", "id")) %>% 
  anti_join(remove2, by = c("Address", "id")) %>% 
  select(id, name = itemLabel, factgrid = item, factgrid_address = Address, address_string = adresse, note = notiz, quelle, type, city)



# Write data  -------------------------------------------------------------

import <- location_qid

create_table <- "
CREATE TABLE IF NOT EXISTS `locations` (
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  `id` varchar(50) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `factgrid` varchar(255) DEFAULT NULL,
  `factgrid_address` varchar(500) DEFAULT NULL,
  `address_string` varchar(255) DEFAULT NULL,
  `note` text DEFAULT NULL,
  `quelle` text DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"

con <- connect_db("db_clean")
DBI::dbExecute(con, create_table)
DBI::dbAppendTable(con, "locations", import)
DBI::dbDisconnect(con); rm(con)


# Add to Entity Linking table ---------------------------------------------

import <- location_qid %>% 
  distinct(id, external_id = factgrid) %>% 
  mutate(entity_id_type = "entities",
         external_id = str_extract(external_id, "Q[0-9]+"),
         external_id_type = "factgrid",
         source = "forum",
         hierarchy = 1)

import

con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)

