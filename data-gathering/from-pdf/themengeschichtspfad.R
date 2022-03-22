# title: Get clean text from Themengeschichtspfad 
# desc: Themengeschichtspfad is a book in which Forum Queeres Archiv narrates about queer history in Munich. Everything is related to certain locations within the city. This script extracts text and locations from the pdf. Important: pages with full screen images need to be deleted, otherwise pdftools won't get detailed data. It's a bug.)
# input: themengeschichtspfad-no-fullpage-images.pdf
# output: text_tgp

library(tidyverse)
library(pdftools)
library(kabrutils)
library(DBI)

# in order to get font info, there cannot be a fullscreen image in the pdf
pdf <- "data-gathering/data/input/themengeschichtspfad-no-fullpage-images.pdf"
pdf_data_list <- pdf_data(pdf = pdf, font_info = TRUE)

# set page number as name of df in list
pdf_data_list <- setNames(pdf_data_list, c(1:length(pdf_data)))

# page as column
pdf_as_data <- bind_rows(pdf_data_list, .id = "page") %>% 
  mutate(
    page = as.numeric(page),
    
    # mark content: before and after can be ignored
    content = 
      case_when(
        page >= 14 & page < 149 ~ 1,
        TRUE ~ 0
      )
  ) %>% 
  group_by(page) %>% 
  mutate(order = row_number()) %>% 
  ungroup() %>% 
  # keep only real content, remove table of content, sources etc
  filter(content == 1) %>% 
  mutate(
    format = 
      case_when(
        str_detect(text, "^[0-9]{1,3}$") ~ "page",
        font_size == 12.0 ~ "heading",
        font_size == 8.8 & font_name == "AAAAAB+Univers-Bold" ~ "location_header",
        font_size == 8.8 & font_name == "AAAAAD+Univers-Light" ~ "p",
        font_size == 8.8 & font_name == "AAAAAC+Univers" ~ "chapter_intro",
        font_size == 7.0 & font_name == "AAAAAB+Univers-Bold" ~ "location_roof_line",
        font_size == 7.0 & font_name == "AAAAAC+Univers" ~ "image_caption",
        font_size == 7.0 & font_name == "AAAAAD+Univers-Light" ~ "source",
        round(font_size, 2) == 7.16 & font_name == "AAAAAC+Univers" ~ "image_caption_direction",
        TRUE ~ "none" # there's one element, but does not matter (word "dem")
      )
  )

pdf_as_data

collapsed_text <- pdf_as_data %>% 
  group_by(page, format) %>%
  summarise(text = glue::glue_collapse(text, sep = " ", last = ""), .groups = "drop") %>% 
  mutate(text = str_replace_all(text, "\\s-\\s|-\\s", "")) %>% 
  # remove, when text is only page number
  # page number not correct, since I had to remove full screen image pages
  filter(format != "page") %>% 
  mutate(
    location = 
      case_when(
        format == "location_header" ~ text,
        format == "heading" ~ str_extract(text, ".+(?=:)"),
        TRUE ~ "München"
      )
  ) %>% 
  fill(location, .direction = "down")

# Import in DB ------------------------------------------------------------

import <- collapsed_text

create_table <- "
CREATE TABLE IF NOT EXISTS `text_tgp` (
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT NULL ON UPDATE current_timestamp(),
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `page` int(11) DEFAULT NULL COMMENT 'page number not totally correct, because full image pages were removed from pdf',
  `format` varchar(25) COLLATE utf8mb3_german2_ci DEFAULT NULL COMMENT 'p, header, image caption ...',
  `text` varchar(2000) COLLATE utf8mb3_german2_ci DEFAULT NULL COMMENT 'cleaned for Bindestriche',
  `location` varchar(100) COLLATE utf8mb3_german2_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `text` (`text`(1024)),
  KEY `loc` (`location`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_german2_ci;
"

con <- connect_db()
dbExecute(con, create_table)
dbAppendTable(con, "text_tgp", import)
dbDisconnect(con); rm(con)
