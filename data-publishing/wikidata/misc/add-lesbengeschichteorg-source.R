# title: Get bios from Lesbengeschichte.org and add source to wikidatas sexual p
# desc: 
# input: 
# output: 



library(tidyverse)
library(kabrutils)
library(rvest)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "wikidata")

base_url <- "https://lesbengeschichte.org/"
url <- "https://lesbengeschichte.org/sitemap_d.html"



# Get data ----------------------------------------------------------------

page <- read_html(url)



links_raw <- page %>% html_nodes("tr td a") 

links <- tibble(
  link = links_raw %>% html_attr("href") %>% paste0(base_url, .),
  text = links_raw %>% html_text(),
) %>% distinct()

bios <- links %>% filter(str_detect(link, "bio"))
bios %>% clipr::write_clip()


input_raw <- rrefine::refine_export("lesbengeschichte-bios") %>% 
  filter(!is.na(wikidata)) %>% 
  distinct(item = wikidata, link)

import <- input_raw %>% 
  add_statement(statements, "sexual_orientation_lesbian") %>%
  long_for_quickstatements(start_at = 3) %>% 
  mutate(
    # set source
    source_statement_1 = "S854",
    source_statement_1_value = paste0('"', link, '"'),
    # set date
    source_statement_2 = "S813",
    source_statement_2_value = paste0("+", Sys.Date(), "T00:00:00Z/", 11)) %>% 
  select(-link)

import

csv_file <- tempfile(fileext = "csv")
write.table(import, csv_file, row.names = FALSE, quote = FALSE, sep = "\t")
fs::file_show(csv_file)  
