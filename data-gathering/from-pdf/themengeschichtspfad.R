library(tidyverse)
library(pdftools)

pdf <- "data-gathering/data/input/themengeschichtspfad.pdf"
pdf_data_list <- pdf_data(pdf = pdf)

# set page number as name of df in list
pdf_data_list <- setNames(pdf_data_list, c(1:length(pdf_data)))

# page as column
pdf_as_data <- bind_rows(pdf_data_list, .id = "page") %>% 
  mutate(
    page = as.numeric(page) - 2,
    
    # mark content: before and after can be ignored
    content = 
      case_when(
        page >= 15 & page < 154 ~ 1,
        TRUE ~ 0
      )
  ) %>% 
  group_by(page) %>% 
  mutate(order = row_number()) %>% 
  # keep only real content, remove table of content, sources etc
  filter(content == 1) 


pdf_as_data <- pdf_as_data %>% 
  mutate(
    format = 
      case_when(
        height == 10 ~ "heading",
        height == 6 ~ "image_caption",
        height == 8 ~ "p",
      )
  ) 

pdf_as_data %>% count(height, format, sort = T)

collapsed_text <- pdf_as_data %>% 
  group_by(page, format) %>% 
  summarise(text_string = glue::glue_collapse(text, sep = " ", last = ""), .groups = "drop") %>% 
  mutate(text_string = str_replace(text_string, "\\s-\\s", "")) %>% 
  # remove, when text_string is only page number
  filter(nchar(text_string) != 2)
