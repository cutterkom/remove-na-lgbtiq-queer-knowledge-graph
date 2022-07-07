library(tidyverse)
library(kabrutils)

input_properties <- c("P117")
input_item_filter_property <- "P131"
input_item_filter_value <- "Q400012"
input_part_of <- "Q100632"

all_properties <- get_properties()
comparison_raw <- get_comparison(input_properties, input_item_filter_property, input_item_filter_value)
chosen_properties <- get_input_properties(all_properties, input_properties)

in_both <- comparison_raw %>% 
  filter(is_same == "true") %>% 
  distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, fg_value, fg_valueLabel, wd_value_from_fg, wd_value_from_wd) %>% 
  # add property info
  left_join(chosen_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property), by = "fg_property_id")

  
  in_both <- reactive({
    data <- comparison_raw() %>% 
      filter(is_same == "true") %>% 
      distinct(fg_item, fg_itemLabel, wd_item, fg_property_id, fg_value, fg_valueLabel, wd_value_from_fg, wd_value_from_wd) %>% 
      # add property info
      left_join(filter_properties(all_properties, input$chosen_properties) %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property), by = "fg_property_id")
  left_join(persons_properties %>% select(fg_property_id, fg_propertyLabel, fg_property, wd_property), by = c("fg_property_id")) 

in_both

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




# Test prop ---------------------------------------------------------------




get_properties <- function() {
  tmp <- readr::read_file("https://raw.githubusercontent.com/cutterkom/remove-na-lgbtiq-queer-knowledge-graph/main/data-publishing/factgrid/queries/get_all_properties_with_corresponding_prop.rq") %>% 
    sparql_to_tibble(endpoint = endpoint) %>%
    mutate(
      fg_property_id = str_extract(fg_property, "P[0-9]+"),
      wd_property_id = str_extract(wd_property, "P[0-9]+"),
      fg_part_of_id = str_extract(fg_part_of, "Q[0-9]+"),
      label = paste0(fg_propertyLabel, " (FG: ", fg_property_id, ", WD: ", wd_property_id, ")"),
      label = str_remove_all(label, '"|@en')
    ) %>% 
    mutate(across(c(fg_part_ofLabel), ~remove_lang(.)))
  
  return(tmp)
}


#' Get classes of properties
#' @param dataframe df with properties info, incl. cols named fg_part_of and fg_part_ofLabel
get_classes_of_properties <- function(data) {
  data %>% 
    select(contains("part_of")) %>% 
    distinct()
}

get_classes_of_properties(all_properties) %>% View

all_properties_raw <- get_properties()



all_properties
# relevant datatypes

all_properties %>% distinct(property_type) %>% View

all_properties <- all_properties_raw %>% 
  filter(str_detect(property_type, "WikibaseItem|String|Time|Quantity|CommonsMedia|ExternalId|Url|CommonsMedia")) 


all_properties%>% distinct(property_type) 

persons_properties <- filter_properties(all_properties, "Q100632")





all_properties %>% anti_join(persons_properties) %>% select(contains("fg_part")) %>% distinct() %>% View


all_properties %>% select(fg_propertyLabel, contains("fg_part")) %>% distinct() %>% count(fg_part_ofLabel, sort = T) %>% View


external_ids <- filter_properties(all_properties, "Q100150")

all_properties %>% anti_join(persons_properties) %>% anti_join(external_ids) %>% distinct(fg_propertyLabel) %>% View

filter_properties <- function(data, class_of_property) {
  data %>% 
    filter(str_detect(fg_part_of, class_of_property)) %>% 
    select(-contains("fg_part_of")) %>% 
    distinct() %>% 
    arrange(fg_propertyLabel)
}


filter_properties(all_properties, "Q100150")
