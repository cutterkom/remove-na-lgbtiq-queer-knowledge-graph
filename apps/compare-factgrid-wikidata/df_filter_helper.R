library(tidyverse)
library(kabrutils)
library(SPARQL)
source("apps/compare-factgrid-wikidata/functions.R")
source("apps/compare-factgrid-wikidata/queries.R")

input_item_filter_property <- "P131"
input_item_filter_value <- "Q400012"

# refering to people/items
input_properties_vec <- c("P117")
# input_part_of <- "Q100632"

# test websites
# input_part_of <- "Q77478"
# input_properties <- "P156"


# test time
# input_part_of <- "Q77483"
# input_properties_vec <- "P49" # beginn date -> gives 2 wikidata items

# test linked data
input_part_of <- "Q100150"
input_properties_vec <- "P533"
# input_item_filter_property <- ""
# input_item_filter_value <- ""


input_part_of <- "Q100150"
input_properties_vec <- "P304"
# input_item_filter_property <- ""
# input_item_filter_value <- ""


input_properties_vec <- "P702_P144"
all_properties <- get_properties()
input_properties <- get_input_properties(all_properties, input_properties_vec)
comparison_raw <- get_comparison(input_properties_vec, input_item_filter_property, input_item_filter_value)

comparison_raw %>% distinct(meta_property_id)

in_both <- comparison_raw %>% 
  filter(is_same == "true") %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, fg_value, fg_valueLabel, wd_value_from_fg, wd_value_from_wd, meta_property_id) %>% 
  # add property info
  left_join(input_properties %>% select(fg_property_id, fg_propertyLabel, meta_property_id, fg_property, wd_property), by = c("meta_property_id", "fg_property_id"))

in_both

only_in_wd <- comparison_raw %>% 
  filter(is_same == "false") %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, wd_property_id, wd_value_from_wd, wd_value_from_wdLabel, fg_value_from_wd, meta_property_id) %>% 
  # remove those that are present in wd
  anti_join(in_both, by = c("fg_item", "wd_value_from_wd")) %>%
  # add property info
  left_join(input_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property, wd_property_id, meta_property_id), by = c("meta_property_id"))

only_in_wd


only_in_fg <- comparison_raw %>% 
  filter(is.na(is_same), !is.na(fg_item)) %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, fg_value, fg_valueLabel, wd_value_from_fg, wd_value_from_wd, meta_property_id) %>% 
  # remove those that are present in wd
  anti_join(in_both, by = c("fg_item", "wd_value_from_fg")) %>% 
  # add property info
  left_join(input_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property, meta_property_id), by =  c("meta_property_id", "fg_property_id"))

only_in_fg %>% glimpse()

