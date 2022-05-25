library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)
library(WikidataR)
library(SPARQL)
library(lubridate)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "statements")

tw_enable_cache()
tw_set_cache_folder(path = fs::path(fs::path_home_r(), "R", "tw_data"))
tw_set_language(language = "de")
tw_create_cache_folder(ask = FALSE)

# Get input data ----------------------------------------------------------

con <- connect_db("db_clean")

entities_raw <- tbl(con, "entities") %>%
  select(-contains("_at")) %>%
  collect()

el_matches <- tbl(con, "el_matches") %>%
  select(-contains("_at"), -entity_id_type, -entity_id_combination, -entity_id_combination_type, external_id_label) %>%
  collect()

chronik_raw <- tbl(con, "chronik") %>% collect()

DBI::dbDisconnect(con)
rm(con)



# Create Statements -------------------------------------------------------


final_labels <- chronik_raw %>%
  mutate(
    Lde = paste0(date, ": ", title),
    Len = Lde,
    Les = Lde,
    Lfr = Lde,
    Dde = "Ereignis der (Münchner) LGBTIQ*-Chronik",
    Den = "Event of the (Munich) LGBTIQ* Chronicle",
    Dfr = "Événement de la chronique LGBTIQ* (munichoise)",
    Des = "Evento de la Crónica LGBTIQ* (Múnich)"
  ) %>%
  select(chronik_entry_id, Lde, Len, Lfr, Les, Dde, Den, Dfr, Des)

timestamp_df <- chronik_raw %>%
  distinct(chronik_entry_id, date) %>%
  mutate(
    month = as.character(str_extract_all(date, "[A-z].+ ")),
    month =
      case_when(
        month == "character(0)" ~ NA_character_,
        str_detect(month, "um|ab") ~ NA_character_,
        TRUE ~ month
      ),
    year = str_extract_all(date, "[0-9]{4}"),
    from_to = ifelse(str_detect(date, "–"), TRUE, FALSE),
    first_part_day = ifelse(from_to == TRUE, str_extract(date, ".*(?=\\. –)|^[0-9]{1,2}"), NA_character_),
    second_part_day = ifelse(from_to == TRUE, str_extract(date, " [0-9]{1,2}\\."), NA_character_),
    second_part_day = trimws(str_remove(second_part_day, "\\.")),
    first_part_month = ifelse(from_to == TRUE, trimws(str_extract(date, "\\s[A-z]+")), NA_character_),
    # last word in col -> 2nd month
    second_part_month = ifelse(from_to == TRUE, trimws(str_extract(month, "(\\w+).$")), NA_character_),
    from_date_string = ifelse(from_to == TRUE, paste0(first_part_day, ". ", first_part_month, " ", year), NA_character_),
    to_date_string = ifelse(from_to == TRUE, paste0(second_part_day, ". ", second_part_month, " ", year), NA_character_),
    from_date = dmy(from_date_string),
    to_date = dmy(to_date_string),

    # when full date, then just parse
    dmy_from_full_date = dmy(date),
    # set when only year to 01-01-YYYY
    dmy_from_year = ifelse(is.na(dmy_from_full_date) & is.na(to_date) & is.na(month), paste0(year, "-01-01"), NA_character_),
    # sometimes just month and year, e.g. Februar 2003
    dmy_from_month_year = ifelse(is.na(dmy_from_full_date) & is.na(to_date) & !is.na(month), paste0("01. ", month, year), NA_character_),
    single_date =
      case_when(
        !is.na(dmy_from_full_date) ~ as.character(dmy_from_full_date),
        !is.na(dmy_from_month_year) ~ as.character(dmy(dmy_from_month_year)),
        # !is.na(dmy_from_year) ~ as.character(dmy_from_full_date),
        TRUE ~ as.character(dmy_from_year)
      ),
    date_type =
      case_when(
        !is.na(dmy_from_full_date) | from_to == TRUE ~ "full_date",
        !is.na(dmy_from_month_year) ~ "month_year",
        TRUE ~ "year"
      )

    # ifelse(!is.na(dmy_from_full_date), as.character(dmy_from_full_date), as.character(dmy_from_year))
  ) %>%
  filter(!is.na(chronik_entry_id))


# fetch only needed columns
# transform to quickstatements format
# https://www.wikidata.org/wiki/Help:QuickStatements#Add_simple_statement
timestamp_final <- timestamp_df %>%
  distinct(chronik_entry_id, date_type, single_date, from_date, to_date) %>% 
  # format: +1967-01-17T00:00:00Z/11
  mutate(single_date = 
           case_when(
             !is.na(single_date) & date_type == "full_date" ~ paste0("+", single_date, "T00:00:00Z/", 11),
             !is.na(single_date) & date_type == "month_year" ~ paste0("+", single_date, "T00:00:00Z/", 10),
             !is.na(single_date) & date_type == "year" ~ paste0("+", single_date, "T00:00:00Z/", 9)
           ),
         from_date = 
           case_when(
             !is.na(from_date) ~ paste0("+", from_date, "T00:00:00Z/", 11),
             TRUE ~ NA_character_
           ),
         to_date = 
           case_when(
             !is.na(to_date) ~ paste0("+", to_date, "T00:00:00Z/", 11),
             TRUE ~ NA_character_
           )
         )

testthat::test_that(
    desc = "every entry needs a date",
    expect_equal(timestamp_final %>% filter(is.na(to_date), is.na(single_date)) %>% nrow(), 0)
  )


labels_df <- chronik_raw %>% filter(!str_detect(label, "DATE")) %>% count(label, sort = T)
# participants ------------------------------------------------------------

# persons and groups

# https://database.factgrid.de/wiki/Property:P133
participants <- chronik_raw %>% 
  distinct(chronik_entry_id, id, label) %>% 
  left_join(entities_raw %>% distinct(id, name), by = "id") %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, external_id), by = "id") %>% 
  filter(label %in% c("PER", "ORG", "CLUB", "PARTY")) %>% 
  select(chronik_entry_id, P133 = external_id)

# addresses ---------------------------------------------------------------
# P208 = address
address <- chronik_raw %>% 
  distinct(chronik_entry_id, id, label) %>% 
  left_join(entities_raw %>% distinct(id, name), by = "id") %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, external_id), by = "id") %>% 
  filter(label %in% c("ADR")) %>% 
  select(chronik_entry_id, P208 = external_id)
address

# Location ----------------------------------------------------------------

# P47

location <- chronik_raw %>% 
  distinct(chronik_entry_id, id, label) %>% 
  left_join(entities_raw %>% distinct(id, name), by = "id") %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, external_id), by = "id") %>% 
  filter(label %in% c("LOC", "DISTRICT", "COUNTRY", "CITY")) %>% 
  #helper with P47_3, "_3" will be removed during longforming
  select(chronik_entry_id, P47_3 = external_id)

location


# context -----------------------------------------------------------------

# P437

# events etc

context <- chronik_raw %>% 
  distinct(chronik_entry_id, id, label, title) %>% 
  left_join(entities_raw %>% distinct(id, name), by = "id") %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid") %>% distinct(id, external_id), by = "id") %>% 
  filter(label %in% c("EVENT", "AWARD", "PUBLICATION", "LAW")) %>% 
  select(chronik_entry_id, P437 = external_id)

# Create statements -------------------------------------------------------

final_labels
timestamp_final
location
address
participants

input_statements <- chronik_raw %>% 
  mutate(item = paste0("CREATE_", chronik_entry_id)) %>% 
  left_join(final_labels, by = "chronik_entry_id") %>% 
  left_join(timestamp_final %>% select(-date_type), by = "chronik_entry_id") %>% 
  # location in munich
  mutate(P47 = 
           case_when(
             location == "München" ~ "Q10427",
             chronik_entry_id == 193 ~ "Q10427",
             chronik_entry_id %in% c(71, 192) ~ "Q165089", # USA,
             chronik_entry_id %in% c(47, 48) ~ "Q94418", # DDR
             year > 1917 & year < 1933 ~ "Q405950", # Weimarer Republik
             year > 1948 ~ "Q94419", # BRD
             year > 1870 & year < 1918 ~ "Q405952", # Kaiserreich
             year > 1932 & year < 1946 ~ "Q409720" # NS-Zeit
             ),
         P47_2 = 
           # if not USA, then German over time
           case_when(
             P47 == "Q165089" ~ NA_character_,
             TRUE ~ "Q140530"
           )
  ) %>% 
  # Datum
  rename(P106 = single_date,
         # start
         P49 = from_date,
         P50 = to_date) %>% 
  # Forum ID 
  mutate(P728 = paste0("chronik_event_", chronik_entry_id)) %>% 
  
  # Text als Notiz
  rename(P73 = text) %>% 
  # Zielgruppe/Gruppe
  mutate(
    P573 = 
      case_when(
        group == "lesbian" ~ "Q399990",
        group == "gay" ~ "Q399989",
        TRUE ~ "Q399988"
      )
  ) %>% 
  add_statement(statements, "instance_of_event") %>% 
  add_statement(statements, "research_area") %>% 
  add_statement(statements, "research_project") %>% 
  left_join(address, by = "chronik_entry_id") %>% 
  left_join(participants, by = "chronik_entry_id") %>% 
  left_join(location, by = "chronik_entry_id") %>% 
  left_join(context, by = "chronik_entry_id")


input_statements %>% 
  distinct(chronik_entry_id, Lde) %>% 
  left_join(events) %>% 
  filter(!is.na(label)) %>% clipr::write_clip()

# Prepare import ----------------------------------------------------------

sort_helper <- input_statements %>% 
  select(matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE),
         # instance of
         P2, 
         # dates
         P106, P49, P50, 
         # location and address
         P47, P47_2, P47_3, P208,
         # target group
         P573,
         # participants
         P133,
         #context
         P437,
         
         # note = text
         P73,
         # meta data forum, research area
         P728, P97, P131) %>% 
  names() %>% 
  enframe() %>% 
  rename(sort = name, property = value)

sort_helper

import <- input_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  arrange(item) %>% 
  select(item, starts_with("L"), starts_with("D"), 
         # instance of
         P2, 
         # dates
         P106, P49, P50, 
         # location
         P47, P47_2, P47_3, P208,
         # target group
         P573,
         # participants
         P133,
         #context
         P437,
         # note = text
         P73,
         # meta data forum, research area
         P728, P97, P131) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value", values_drop_na = TRUE) %>%
  distinct() %>% 
  left_join(sort_helper, by = "property") %>%
  mutate(property = str_remove(property, "_.*")) %>% 
  arrange(item, sort) %>% 
  select(-sort) %>% 
  replace(is.na(.), "")


# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/chronik-events.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# Fetch from Factgrid -----------------------------------------------------

query <- 
  '
SELECT DISTINCT ?item ?itemLabel ?id ?idLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
  ?item wdt:P131 wd:Q400012.
  OPTIONAL { ?item wdt:P728 ?id. }
  ?item wdt:P2 wd:Q9
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         external_id = str_extract(item, "Q[0-9]+"))


factgrid_qids <- input_statements %>% 
  select(event_id = P728, Lde) %>% 
  left_join(query_res, by = c("event_id" = "idLabel")) %>% 
  distinct()


# Write Factgrid IDs in DB ------------------------------------------------

testthat::test_that(
  desc = "all posters in factgrid",
  expect_equal(factgrid_qids %>% filter(is.na(external_id)) %>% nrow(), 0)
)

import <- factgrid_qids %>% 
  distinct(id, external_id) %>% 
  filter(!is.na(external_id)) %>% 
  mutate(
    entity_id_type = "entities",
    external_id_type = "factgrid", 
    source = "forum", 
    hierarchy = 1)

con <- connect_db("db_clean")
DBI::dbAppendTable(con, "el_matches", import)
DBI::dbDisconnect(con); rm(con)



# Change labels -----------------------------------------------------------

new_labels <- import %>% 
  select(id, item = external_id) %>% 
  left_join(input_statements %>% distinct(P728, Lde, Len, Les, Lfr, year), by = c("id" = "P728")) %>% 
  mutate(
    Lde = paste0(str_remove(Lde, ".*: "), " (", year, ")"),
    Len = paste0(str_remove(Len, ".*: "), " (", year, ")"),
    Lfr = paste0(str_remove(Lfr, ".*: "), " (", year, ")"),
    Les = paste0(str_remove(Les, ".*: "), " (", year, ")")) %>% 
  filter(item != "Q409997")

import <- new_labels %>% 
  select(item, Lde, Len, Lfr, Les) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value", values_drop_na = TRUE) %>%
  distinct()
import

# Import ------------------------------------------------------------------

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/new-labels.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_books",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)

