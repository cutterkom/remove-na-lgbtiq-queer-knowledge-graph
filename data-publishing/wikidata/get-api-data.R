# Goal: Count my revisions on Wikidata for stock taking: How many NA's removed?

library(WikipediR)
library(tidyverse)

domain <- "wikidata.org"
project <- "wikidata"
user <- c("Removena")
limit <- 5

# Get all contributions ---------------------------------------------------

get_inital_batch <- function() {
  data <- user_contributions(domain = domain, project = project, 
                             username = user, limit = limit)
}

get_next_batch <- function(previous_data = NULL) {
  # do more data exist?
  if(!is.null(previous_data[["continue"]][["uccontinue"]])) {
    next_data <- user_contributions(domain = domain, project = project, 
                                    username = user, limit = limit, 
                                    continue = previous_data[["continue"]][["uccontinue"]])
  }
}

inital_data <- get_inital_batch()
next_batch <- get_next_batch(inital_data)

next_batch2 <- get_next_batch(next_batch)



#inital_data[["query"]][["usercontribs"]] %>% map_df(., ~print(.))
#revisions_on_data <- revisions_df %>% filter(str_detect(tags, "wikidata-ui|OAuth"))