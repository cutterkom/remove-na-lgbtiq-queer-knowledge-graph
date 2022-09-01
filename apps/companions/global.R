library(shiny)
library(shinythemes)
library(tidyverse)
library(kabrutils)
library(SPARQL)
library(DT)
library(shinyFeedback)
shinyFeedback::useShinyFeedback()

endpoint <- "https://database.factgrid.de/sparql"
source("queries.R", local = TRUE)

# run choose-relevant-persons.R to generate data
persons <- readRDS("data/persons.Rds")
persons_list <- persons %>% 
  select(item) %>% 
  deframe()

names(persons_list) <- persons$label


build_src_url <- function(fg_item = NULL) {
  base_url <- "https://database.factgrid.de/query/embed.html#"
  src_url <- paste0(base_url, query_companions(fg_item = fg_item))
  src_url <- str_replace_all(src_url, "\n", "%0A")
  src_url <- str_replace_all(src_url, "\\s{2,}", "")
  src_url
}