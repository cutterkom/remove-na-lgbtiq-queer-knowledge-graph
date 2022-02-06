# Für welche ISBN-Nummern gibt es eine jsonld-Datei bei lobid?

library(tidyverse)
library(DBI)
library(httr)
library(jsonlite)

# Get books data ----------------------------------------------------------

con <- dbConnect(RMariaDB::MariaDB(), username="kabr", password="", dbname ="lgbtiq_kg", host="localhost")
books <- tbl(con, "books") %>% collect()
dbDisconnect(con); rm(con)

no_isbn <- books %>% filter(is.na(isbn))
isbn <- books %>% filter(!is.na(isbn))

# gibt die lobid eine 200er zurück?

#' Is there a data in lobid?
#' 
#' If so, then totalItems	> 0
#' 
#' @param query to search lobid, see https://lobid.org/resources/api#query-syntax
#' @param parameter to specify search, eg isbn or name

check_lobid_item <- function(query, parameter) {
  if (is.null(parameter)) {
    url <- paste0("https://lobid.org/resources/search?q=", query, "&format=json")
  } else if (!is.null(parameter)) {
    url <- paste0("https://lobid.org/resources/search?q=", parameter, ":", query, "&format=json")
  }
  print(url)
  json <- fromJSON(url)
  item_exists <- json$totalItems > 0
}



# Check lobid item for books in db ----------------------------------------

check_isbn <- isbn %>% 
  mutate(check_isbn = map(.x = isbn, .f = check_lobid_item, "isbn")) %>% 
  unnest(col = "check_isbn")
  
check_isbn %>% count(check_isbn)
