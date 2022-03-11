library(shiny)
library(tidyverse)
library(kabrutils)
library(DBI)

load("data/candidates-authors.Rdata")

source("get_infos.R")
ui <- shinyUI(fluidPage(
  
  actionButton('button_start', 'Start'),
  br(),
  actionButton("button_positive", "Ja, identisch"),
  br(),
  actionButton("button_negative", "Nein, verschieden"),
  br(),
  actionButton("button_not_sure", "Unsicher"),
  # br(),
  # actionButton('save_inputs', 'Save inputs'),
  # display text output
  textOutput("candidate_1_name"),
  textOutput("candidate_2_name")
  
)) 

server <-  shinyServer(function(input, output,session) {
  
  # toListen <- reactive({
  #   c(input$button_positive, input$button_negative, input$button_not_sure)
  # })
  
  # get data
 
  
  candidates_to_check <- eventReactive(input$button_start, {
    con <- connect_db()
    candidates <- tbl(con, "matching_candidates_authors_books_posters") %>% 
      dplyr::filter(decision == "no_judgement") %>% 
      dplyr::arrange(rank) %>% 
      dplyr::filter(row_number() == 1) %>% 
      dplyr::collect()

    DBI::dbDisconnect(con); rm(con)
    return(candidates)
  })
  
  candidate_names <- eventReactive(input$button_start, {
    candidates_to_check() %>% get_candidates_names()
  })
  
  # candidate_1_name
  output$candidate_1_name <- renderText({
    paste("Candidate 1: ", candidate_names() %>% pull(name_1))
  })
  
  # candidate_2_name
  output$candidate_2_name <- renderText({
    paste("Candidate 1: ", candidate_names() %>% pull(name_2))
  })
  
  decision_positive <- observeEvent(input$button_positive, {
    print(candidates_to_check())
    save_decision(candidates_to_check(), "positive")
  })
  
  decision_negative <- observeEvent(input$button_negative, {
    save_decision(candidates_to_check(), "negative")
  })
  
  decision_not_sure <- observeEvent(input$button_not_sure, {
    save_decision(candidates_to_check(), "not_sure")
  })

  
})

shinyApp(ui=ui,server=server)