# Goal: Count my revisions on Wikidata for stock taking: How many NA's removed?

library(WikipediR)
library(tidyverse)
library(tidywikidatar)
library(googlesheets4)

domain <- "wikidata.org"
project <- "wikidata"
user <- c("Removena")
limit <- 500

# data will be saved in googlesheets
gs_file <- "1zQ2CO6fkKUg4RFN8TACOxa2ZnSg2F45saBwDNphlzGY"

# Get all contributions ---------------------------------------------------

mw_api_get_inital_batch <- function(limit = 500) {
  data <- WikipediR::user_contributions(domain = domain, project = project, 
                             username = user, limit = limit)
}

mw_api_get_next_batch <- function(starting_point = NULL, limit = 500, ...) {
  # do more data exist?
  if(!is.null(starting_point)) {
    next_data <- WikipediR::user_contributions(domain = domain, project = project, 
                                    username = user, limit = limit, 
                                    continue = starting_point)
  }
}

mw_api_get_more_results_starting_point <- function(data) {
  data[["continue"]][["uccontinue"]] 
}


# Start fetching data -----------------------------------------------------

inital_data <- mw_api_get_inital_batch()
check_for_more <- mw_api_get_more_results_starting_point(inital_data)

# save all data in a list
all_data <- inital_data[["query"]][["usercontribs"]]

while(!is.null(check_for_more)){
  # get current scroll status
  current_data <- mw_api_get_next_batch(starting_point = check_for_more)
  #append to data stack
  all_data <- append(all_data, current_data[["query"]][["usercontribs"]])
  #update scroll 
  check_for_more <- mw_api_get_more_results_starting_point(current_data)
}

# fetch only relevant data
# aka: real revisions on data and not wiki talk stuff etc
# wikidata-ui: change on website
# OAuth: change with Quickstatements or API
revisions <- map_df(all_data, ~print(.)) %>% 
  filter(str_detect(tags, "wikidata-ui|OAuth")) %>% 
  # extract date
  mutate(
    timedate = lubridate::as_datetime(timestamp),
    date = lubridate::date(timedate),
    # get wikidata labels of changed items
    label_en = tw_get_label(id = title, language = "en"),
    label_de = tw_get_label(id = title, language = "de")
  )


# write to sheet
write_sheet(revisions, ss = gs_file, sheet = "raw_data")

# # test
# testdata <- user_contributions(domain = domain, project = project, 
#                                username = user, limit = 500)
# 
# testdata_df <- purrr::map_df(testdata[["query"]][["usercontribs"]], ~print(.))
# 
# testdata_df %>% count(revid, sort = T)
# testdata_df %>% distinct()
# 
# testdata[["query"]][["usercontribs"]] %>% enframe() %>% unnest_longer("value")
# all_equal(testdata_df, revisions)


# count_per_date <- revisions %>% 
#   count(date, sort = T)
# 
# count_per_date %>% 
#   ggplot(aes(x = date, y = n)) +
#   geom_col() +
#   theme_minimal() +
#   labs(title = "Anzahl der Veränderungen in Wikidata",
#        subtitle = "Account: Removena", 
#        x = "Anzahl",
#        y = "Datum" )
