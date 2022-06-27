

input_per <- c("P203")
input_items_filter <- "?fg_item fgt:P131 fg:Q400012."
library(tidyverse)
library(kabrutils)
persons_properties <- get_person_properties()
comparison_raw <- get_comparison(input_per, input_items_filter)
nrow(comparison_raw)

in_both <- comparison_raw %>% 
  filter(is_same == "true") %>% 
  distinct(fg_item, fg_itemLabel, fg_property_id, fg_value, fg_valueLabel, wd_value_from_fg, wd_value_from_wd) %>% 
  left_join(persons_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property), by = c("fg_property_id")) 

only_in_wd <- comparison_raw %>% 
  filter(is_same == "false") %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, wd_value_from_wd, fg_value_from_wd) %>% 
  # remove those that are present in wd
  anti_join(in_both, by = c("fg_item", "wd_value_from_wd")) %>%
  # add property info
  left_join(persons_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property), by = "fg_property_id")

only_in_fg <- comparison_raw %>% 
  filter(is.na(is_same), !is.na(fg_item)) %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, fg_value, fg_valueLabel, wd_value_from_fg, wd_value_from_wd, fg_value_from_wd) %>% 
  # remove those that are present in wd
  anti_join(in_both, by = c("fg_item", "wd_value_from_fg")) %>% 
  # add property info
  left_join(persons_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property), by = "fg_property_id")


import_in_wd <- comparison_raw %>% 
  select(-fg_value, -fg_valueLabel, -wd_value_from_fg) %>% 
  inner_join(only_in_wd %>% distinct(fg_item, wd_value_from_wd), by = c("fg_item", "wd_value_from_wd")) %>%
  #filter(str_detect(fg_itemLabel, "Klaus Mann")) %>% 
  distinct() %>% 
  mutate(
    # item: FG item
    fg_item_id = str_extract(fg_item, "Q[0-9]+"),
    # FG value derived from Wikidata
    fg_value_from_wd_id = str_extract(fg_value_from_wd, "Q[0-9]+"),
    # Wikidata value for source
    wd_value_from_wd = str_extract(wd_value_from_wd, "Q[0-9]+"),
    wd_value_from_wd = paste0('"', wd_value_from_wd, '"')) %>% 
  # add source of Factgrid-Object
  bind_cols(tibble(source = "S771", time = "S432", timestamp = paste0("+", Sys.Date(), "T00:00:00Z/", 11))) %>% 
  select(item = fg_item_id, property = fg_property_id, value = fg_value_from_wd_id, 
         source, source_value = wd_value_from_wd, 
         time, timestamp) %>% 
  # can only import those with a Factgrid ID in Wikidata
  filter(!is.na(value)) %>% 
  distinct()

comparison_raw %>% 
  select(-fg_value, -fg_valueLabel, -wd_value_from_fg) %>% 
  inner_join(only_in_wd %>% distinct(fg_item, wd_value_from_wd), by = c("fg_item", "wd_value_from_wd")) %>%
  filter(str_detect(fg_itemLabel, "Klaus Mann")) %>% 
  distinct() %>%
  mutate(
    # item: FG item
    fg_item_id = str_extract(fg_item, "Q[0-9]+"),
    # FG value derived from Wikidata
    fg_value_from_wd_id = str_extract(fg_value_from_wd, "Q[0-9]+"),
    # Wikidata value for source
    wd_value_from_wd = str_extract(wd_value_from_wd, "Q[0-9]+"),
    wd_value_from_wd = paste0('"', wd_value_from_wd, '"')) %>% 
  # add source of Factgrid-Object
  bind_cols(tibble(source = "S771", time = "S432", timestamp = paste0("+", Sys.Date(), "T00:00:00Z/", 11))) %>% 
  select(item = fg_item_id, property = fg_property_id, value = fg_value_from_wd_id, 
         source, source_value = wd_value_from_wd, 
         time, timestamp) %>% 
  # can only import those with a Factgrid ID in Wikidata
  filter(!is.na(value)) %>% 
  distinct()
