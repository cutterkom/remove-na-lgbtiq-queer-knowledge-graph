
# Functions for the comparison app: Factgrid - Wikidata -------------------

endpoint <- "https://database.factgrid.de/sparql"

#' Get properties related to persons
#' @return dataframe
get_properties <- function() {
  tmp <- readr::read_file("https://raw.githubusercontent.com/cutterkom/remove-na-lgbtiq-queer-knowledge-graph/main/data-publishing/factgrid/queries/get_all_properties_with_corresponding_prop.rq") %>%
    sparql_to_tibble(endpoint = endpoint) %>%
    mutate(
      fg_property_id = str_extract(fg_property, "P[0-9]+"),
      wd_property_id = str_extract(wd_property, "P[0-9]+"),
      fg_part_of_id = str_extract(fg_part_of, "Q[0-9]+"),
      label = paste0(fg_propertyLabel, " (FG: ", fg_property_id, ", WD: ", wd_property_id, ")"),
      label = str_remove_all(label, '"|@en'),
      fg_property_type = str_extract(fg_property_type, "(?<=#).+(?=>)")
    ) %>%
    mutate(across(c(fg_part_ofLabel), ~ remove_lang(.)))

  return(tmp)
}

#' Get classes of properties
#' @param data dataframe with properties info, incl. cols named fg_part_of and fg_part_ofLabel
get_parts_of_properties <- function(data) {
  data %>%
    select(contains("part_of")) %>%
    distinct()
}

#' get a list of properties filtered by part_of
#' @param data dataframe with properties
#' @param input_part_of
#' @returns list, input for checkbox input `input_properties`
get_properties_list_part_of <- function(data, input_part_of) {
  data %>%
    filter(fg_part_of_id %in% c(input_part_of)) %>%
    select(label, fg_property_id) %>%
    arrange(label) %>%
    distinct() %>%
    deframe()
}

#' get a list of properties filtered by property type
#' @param data dataframe with properties
#' @param input_property_by_type
#' @returns list, input for checkbox input `input_properties`
get_properties_list_by_type <- function(data, input_property_by_type) {
  data %>%
    filter(fg_property_type %in% c(input_property_by_type)) %>%
    select(label, fg_property_id) %>%
    arrange(label) %>%
    distinct() %>%
    deframe()
}



#' Filter properties by input
#' @param data dataframe
get_input_properties <- function(data, input_properties) {
  data %>%
    filter(fg_property_id == input_properties) %>%
    select(-contains("fg_part_of")) %>%
    distinct() %>%
    arrange(fg_propertyLabel)
}

#' Get available propery types
#' @param data dataframe
get_property_types <- function(data) {
  data %>% 
    count(fg_property_type, sort = T)
}

#' Get comparison data based on property
#'
#' Main function of the app. This functions queries data on Factgrid looking for properties related to persons that have a corresponding Wikidata property. Utilizes federated queries.
#' @param input_properties chosen properties in input$input_properties
#' @param input_items_filter filter items e.g. like ?fg_item fgt:P131 fg:Q400012 . (don't forget the period!)
#' @return dataframe
get_comparison <- function(input_properties, input_item_filter_property, input_item_filter_value) {
  all_properties %>%
    #inner_join(input_properties, by = "fg_property") %>% 
    filter(fg_property_id %in% input_properties) %>% 
    select(-contains("part_of")) %>%
    distinct() %>%
    pmap_dfr(function(...) {
      current <- tibble(...)
      cli::cli_alert_info("Fetch data for: {current$fg_propertyLabel}")
      fg_property_id <- current$fg_property_id

      cli::cli_alert_info("Property data type: {current$fg_property_type}")

      if (input_item_filter_property != "" & input_item_filter_value != "") {
        input_items_filter <- paste0("?fg_item fgt:", input_item_filter_property, " fg:", input_item_filter_value, ".")
      } else {
        input_items_filter <- ""
      }
      
      if(current$fg_property_type == "WikibaseItem") {
        query <- query_items(input_items_filter = input_items_filter, fg_property_id = fg_property_id)
      } else {
        # everything thats not an item: strings, dates
        query <- query_non_items(input_items_filter = input_items_filter, fg_property_id = fg_property_id)
      }
      
      res <- sparql_to_tibble(query, endpoint = endpoint)
      print(res)
      
      if (nrow(res) > 0) {
        res <- res %>%
          mutate(
            # properties
            fg_property_id = str_extract(fg_property, "P[0-9]+"),
            wd_property_id = str_extract(wd_property, "P[0-9]+"),
            fg_property_type = str_extract(fg_property_type, "(?<=#).+(?=>)"),
            # items
            fg_item_id = str_extract(fg_item, "Q[0-9]+"),
            wd_item_id = str_extract(wd_item, "Q[0-9]+"),
            # values
            fg_value_id = str_extract(fg_value, "Q[0-9]+"),
            # concerning WikibaseItems
            fg_value_from_wd_id = str_extract(fg_value_from_wd, "Q[0-9]+"),
            wd_value_from_fg_id = str_extract(wd_value_from_fg, "Q[0-9]+"),
            wd_value_from_wd_id = str_extract(wd_value_from_wd, "Q[0-9]+")
            
            
          )
      } else {
        res <- tibble(
          fg_item = NA_character_,
          fg_itemLabel = NA_character_,
          wd_item = NA_character_,
          fg_property = NA_character_,
          fg_propertyLabel = NA_character_,
          wd_property = NA_character_,
          fg_value = NA_character_,
          fg_valueLabel = NA_character_,
          fg_value_from_wd = NA_character_,
          wd_value_from_fg = NA_character_,
          wd_value_from_wd = NA_character_,
          is_same = NA_character_,
          fg_property_id = NA_character_,
          wd_property_id = NA_character_
        )
      }
      cli::cli_alert_success("data fetched")

      res
    })
}


# Display -----------------------------------------------------------------

#' Create clickable link
#' Function that creates a link from href and string
#' @param data dataframe with cols
#' @param url href for link
#' @param name string for inside <a href>name</a>
#' @param new_col name of new col
#' @param property_type colum with property type: different ways needed to create link
create_link <- function(data, url, name, new_col, property_type = fg_property_type) {
  data %>% 
    mutate(
      {{new_col}} := str_replace({{url}}, "<", "<a href='"),
      {{new_col}} := str_replace({{new_col}}, ">", "' target='_blank'>"),
      {{new_col}} := paste0({{new_col}}, {{name}}, "</a>")
    )  
}


# Import ------------------------------------------------------------------

#' Create data for Quickstatements import
#' @param data if only_in_wd or only_in_fg
#' @param target if import into wikidata or factgrid
#' @return dataframe
get_import_data <- function(data, target, input_property_by_type = NULL) {
  cli::cli_alert_info("Import for property type: {input_property_by_type} in {target}")
  if (target == "wikidata") {
    import_data <- comparison_raw() %>%
      inner_join(only_in_fg() %>% distinct(fg_item, wd_value_from_fg), by = c("fg_item", "wd_value_from_fg")) %>%
      mutate(
        # value: Wikidata value derived from Factgrid, either QID of WikibaseItem, or plain string
        value =
          case_when(
            fg_property_type == "WikibaseItem" ~ wd_value_from_fg_id,
            fg_property_type == "Quantity" ~ as.character(fg_value),
            TRUE ~ paste0('"', fg_value, '"')
          ),
        # source_value: Source information: Wikidata Item the value is taken from (as string)
        source_value =
          case_when(
            fg_property_type == "WikibaseItem" ~ fg_value_id,
            TRUE ~ fg_item_id
          ),
        source_value = paste0('"', source_value, '"')
      ) %>%
      # add source and timestamp of Factgrid-Object
      bind_cols(tibble(source = "S8168", time = "S813", timestamp = paste0("+", Sys.Date(), "T00:00:00Z/", 11))) %>%
      select(
        item = wd_item_id,
        property = wd_property_id,
        value,
        source,
        source_value,
        time,
        timestamp
      ) %>%
      distinct()
    
    cli::cli_alert_success("data copied for import in wikidata")

    import_data
  } else if (target == "factgrid") {
    import_data <- comparison_raw() %>%
      select(-fg_value, -fg_valueLabel, -wd_value_from_fg) %>%
      inner_join(only_in_wd() %>% distinct(fg_item, wd_value_from_wd), by = c("fg_item", "wd_value_from_wd")) %>%
      distinct() %>%
      mutate(
        # value: FG value derived from Wikidata, either QID if WikibaseItem, or plain string
        value = 
          case_when(
            fg_property_type == "WikibaseItem" ~ fg_value_from_wd_id,
            fg_property_type == "Quantity" ~ as.character(wd_value_from_wd),
            TRUE ~ paste0('"', wd_value_from_wd, '"')
          ),
        # Source information: Wikidata Item the value is taken from (as string)
        source_value =
          case_when(
            fg_property_type == "WikibaseItem" ~ wd_value_from_wd_id,
            TRUE ~ wd_item_id,
          ),
        source_value = paste0('"', source_value, '"')
      ) %>%
      # add source of Factgrid-Object
      bind_cols(tibble(source = "S771", time = "S432", timestamp = paste0("+", Sys.Date(), "T00:00:00Z/", 11))) %>% 
      select(
        item = fg_item_id, 
        property = fg_property_id, 
        value,
        source, 
        source_value,
        time, timestamp
      ) %>%
      # can only import those with a Factgrid ID in Wikidata
      filter(!is.na(value)) %>%
      distinct()
   
    cli::cli_alert_success("data copied for import in factgrid")

    import_data
  } else {
    stop("no valid target. Only `wikidata` or `factgrid` allowed")
  }
}

