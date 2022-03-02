library(tidyverse)
pdf <- "data-gathering/data/input/themengeschichtspfad.pdf"
pdf_data_list <- pdf_data(pdf = pdf)

# set page number as name of df in list
pdf_data_list <- setNames(pdf_data_list, c(1:length(pdf_data)))
# page as column
pdf_as_data <- bind_rows(pdf_data_list, .id = "page") %>% 
  mutate(
    page = as.numeric(page),
    
    # mark content: before and after can be ignored
    content = 
      case_when(
        page >= 15 & page < 154 ~ 1,
        TRUE ~ 0
      )
  ) %>% 
  group_by(page) %>% 
  mutate(order = row_number()) %>% 
  # keep only real content
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


bindestriche <- pdf_as_data %>% 
  filter(space == FALSE, text == "-")

collapsed_text <- pdf_as_data %>% 
 # anti_join(bindestriche) %>% 
  group_by(page, format) %>% 
  summarise(text_string = glue::glue_collapse(text, sep = " ", last = ""), .groups = "drop")


pdf_as_data %>% 
    filter(space == FALSE, text == "-") %>% View

collapsed_text %>% head(10) %>% 
  filter(str_detect(text_string, "[a-z]\\s-\\s[a-z]")) %>% pull(text_string)
