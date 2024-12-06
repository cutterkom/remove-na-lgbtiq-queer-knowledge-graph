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

# cache for wikidata
tw_enable_cache()
tw_set_cache_folder(path = fs::path(fs::path_home_r(), "R", "tw_data"))
tw_set_language(language = "de")
tw_create_cache_folder(ask = FALSE)

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
revisions <- rlist::list.stack(all_data, fill=TRUE) %>% 
  filter(str_detect(tags, "wikidata-ui|OAuth")) %>% 
  mutate(
    timedate = lubridate::as_datetime(timestamp),
    date = lubridate::date(timedate)
  )

labels <- revisions %>% 
  distinct(title) %>% 
  mutate(
    label_en = tw_get_label(id = title, language = "en"),
    label_de = tw_get_label(id = title, language = "de")
  )

revisions <- revisions %>% left_join(labels, by = "title")

# write to sheet
#write_sheet(revisions, ss = gs_file, sheet = "raw_data")
write_delim(revisions, "data-publishing/wikidata/data/revisions.csv", delim = "\t")
