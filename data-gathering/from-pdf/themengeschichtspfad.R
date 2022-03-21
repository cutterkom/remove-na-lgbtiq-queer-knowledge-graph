library(tidyverse)
library(pdftools)

# in order to get font info, there cannot be a fullscreen image in the pdf
pdf <- "data-gathering/data/input/themengeschichtspfad-no-fullpage-images.pdf"
pdf_data_list <- pdf_data(pdf = pdf, font_info = TRUE)

# set page number as name of df in list
pdf_data_list <- setNames(pdf_data_list, c(1:length(pdf_data)))

# page as column
pdf_as_data <- bind_rows(pdf_data_list, .id = "page") %>% 
  mutate(
    page = as.numeric(page) - 2,
    
    # mark content: before and after can be ignored
    content = 
      case_when(
        page >= 15 & page < 150 ~ 1,
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
        font_size == 12.0 ~ "heading",
        font_size == 8.8 & font_name == "AAAAAB+Univers-Bold" ~ "location_header",
        font_size == 7.0 & font_name == "AAAAAB+Univers-Bold" ~ "location_roof_line",
        font_size == 7.0 & font_name == "AAAAAC+Univers" ~ "image_caption",
        font_size == 8.8 & font_name == "AAAAAD+Univers-Light" ~ "p",
        font_size == 8.8 & font_name == "AAAAAC+Univers" ~ "chapter_intro",
        font_size == 7.0 & font_name == "AAAAAD+Univers-Light" ~ "source",
        round(font_size, 2) == 7.16 & font_name == "AAAAAC+Univers" ~ "image_caption_direction",
        str_detect(text, "^[0-9]{1,3}$") ~ "page",
        TRUE ~ "none" # there's one element, but does not matter (word "dem")
      )
  )

collapsed_text <- pdf_as_data %>% 
  group_by(page, format) %>% #filter(page == 18) %>% View
  summarise(text_string = glue::glue_collapse(text, sep = " ", last = ""), .groups = "drop") %>% 
  mutate(text_string = str_replace(text_string, "\\s-\\s", "")) %>% 
  # remove, when text_string is only page number
  filter(nchar(text_string) <= 3)
