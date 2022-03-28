# title: Get data from LGBTIG Timeline (= Chronik)
# desc: a timeline on ttps://forummuenchen.org/lgbtiq-chronik/ has many entites. This script scrapes it.
# input: text from https://forummuenchen.org/lgbtiq-chronik/
# output: text_chronik

library(tidyverse)
library(rvest)
library(kabrutils)
library(DBI)

URL <- "https://forummuenchen.org/lgbtiq-chronik/"
nodes <- read_html(URL) %>% html_nodes(".timeline-item")


# Extract data from nodes into tabular data -------------------------------

chronik <- tibble(
  title = nodes %>% extract_text("h3"),
  date = nodes %>% extract_text(".timeline-date"),
  text = nodes %>% extract_text("p"),
  location = ifelse(nodes %>% as.character() %>% str_detect("muc") == TRUE, "München", NA),
  group = 
    case_when(
      nodes %>% as.character() %>% str_detect("timeline-icon star") == TRUE ~ "misc",
      nodes %>% as.character() %>% str_detect("timeline-icon round") == TRUE ~ "lesbian",
      nodes %>% as.character() %>% str_detect("timeline-icon square") == TRUE ~ "gay",
      TRUE ~ NA_character_)
) %>%
  unnest(c(title, date, text, location, group)) %>%
  group_by(title, date, location, group) %>%
  summarise(
    text = glue::glue_collapse(text, sep = " "),
    .groups = "drop"
  ) %>%
  mutate(
    across(where(is.character), str_trim),
    year = as.numeric(str_extract(date, "[0-9]{4}")))


# Export ------------------------------------------------------------------

import <- chronik

create_table <- "
CREATE TABLE IF NOT EXISTS `text_chronik` (
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp(),
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(255) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  `text` varchar(2000) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  `location` varchar(100) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  `group` varchar(100) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  `date` varchar(100) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  `year` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `text` (`text`(1024)),
  KEY `loc` (`location`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci;
"

con <- connect_db()
dbExecute(con, create_table)
dbAppendTable(con, "text_chronik", import)
dbDisconnect(con); rm(con)