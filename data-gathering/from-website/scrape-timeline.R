# title: Get data from LGBTIG Timeline
# desc: a timeline on ttps://forummuenchen.org/lgbtiq-chronik/ has many entites. This script scrapes it.
# input: text from https://forummuenchen.org/lgbtiq-chronik/
# output: database table `timeline`

library(tidyverse)
library(rvest)
library(kabrutils)

URL <- "https://forummuenchen.org/lgbtiq-chronik/"
nodes <- read_html(URL) %>% html_nodes(".timeline-item")


# Extract data from nodes into tabular data -------------------------------

timeline <- tibble(
  title = nodes %>% extract_text("h3"),
  date = nodes %>% extract_text(".timeline-date"),
  text = nodes %>% extract_text("p"),
  location = ifelse(nodes %>% as.character() %>% str_detect("muc") == TRUE, "München", NA)
) %>% 
  unnest(c(title, date, text, location)) %>% 
  group_by(title, date, location) %>% 
  summarise(
    text = glue::glue_collapse(text, sep = " "),
    .groups = "drop"
  ) %>% 
  mutate(across(where(is.character), str_trim))


# Export ------------------------------------------------------------------

con <- connect_db()
DBI::dbWriteTable(con, "timeline", timeline, overwrite = TRUE, row.names = FALSE)
DBI::dbDisconnect(con); rm(con)
