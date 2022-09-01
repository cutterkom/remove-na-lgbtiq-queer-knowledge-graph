library(shiny)

source('global.R', local = TRUE)

ui <- fluidPage(
  theme = shinytheme("cosmo"), 
  tags$head(
    # Note the wrapping of the string in HTML()
    tags$style(HTML("
      h2 {
        font-size: 39.6px;
        font-weight: 400;
      }
      h3 {
        border-bottom: 1px solid #dee2e6;
        padding-bottom: .5rem;
      }
      p,div {
        font-size: 18px;
        font-weight: 400;
        line-height: 27px;
      }
      .shiny-input-container {
        color: #474747;
      }"))
  ),              
  titlePanel("Companions"),
  h3(""),
  sidebarLayout(
    sidebarPanel(
      textInput("input_fg_item_id", value = "Q402214", label = "Choose a person with Factgrid QID:", placeholder = "Q409762"),
      
      actionButton("button_input_fg_item_id", "Explore surrounding", class = "btn-primary"),
      
      actionButton("button_random", "Choose random person", class = "btn-secondary"),
      h3("Available persons"),
      tags$div("In the followingtable you will find all the people from the project from which companions can be queried Copy the value from the column",
               tags$i("Identifier"),
               "into the field above."),
      tags$p(""),
      tags$div("IIf the screen is blank, then there is no connection to another person - so very likely data is still missing. "),
      dataTableOutput('table')
    ),
    mainPanel(
      tags$div("This page is used to take a look at the data of the",
      tags$a(href='https://queerdata.forummuenchen.org', "Remove NA project"), 
      "All of the individuals in this queer history dataset did not exist on their own. They were and are surrounded by people they love, people they collaborated with, people they were followed by, people they were influenced by, and people who were inspired by them. In short: their companions.",
      tags$p(""),
      " If you only take the data from Remove NA, you will not do justice to this network of relationships. Only in interaction with other data sources can one get halfway close to reality. It makes sense to connect Wikidata and Wikipedia. DBpedia is a Knowledge Graph derived from Wikipedia and updated several times a year."
      ),
      tags$p(""),
      tags$div("Accordingly, the",
        tags$b("definition of a companion is: Someone appears in connection with the person in focus either in Factgrid, the database for historical projects, Wikidata or Wikipedia"),
        "Technically this page utilizes federated queries starting at the",
      tags$a(href="https://database.factgrid.de/query/", "Factgrid SPARQL Endpoint"),
      "."
      ),
      uiOutput("label_desc_links"),
      htmlOutput("iframe")
    )
  )
)

server <- function(session, input, output) {
  observeEvent(input$button_random, {
    cli::cli_alert("choose random")
    updateTextInput(session, "input_fg_item_id", value = sample(persons$item, 1))
  })
  
  output$label_desc_links = renderUI({
    
    label <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(label)
    
    item <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(item)
    
    wd_item <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(wd_item)
    
    desc <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(desc)
    
    Sdewiki <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(Sdewiki)
    
    Senwiki <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(Senwiki)
    
    Swikidatawiki <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(wd_item)
    
    Swikidatawiki <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(Swikidatawiki)
    
    dbpedia <- persons %>% 
      filter(item == input$input_fg_item_id) %>% 
      pull(dbpedia)
    
    tagList(
      h3(label),
      p(desc),
      p("Find", span(label), "on: ", 
        a("Factgrid", href = paste0("https://database.factgrid.de/wiki/Item:", item)),
        tags$span(" - "),
        a("Wikidata", href = Swikidatawiki),
        tags$span(" - "),
        a("DBPedia", href = dbpedia),
        tags$span(" - "),
        a("German Wikipedia", href = Sdewiki), 
        tags$span(" - "),
        a("English Wikipedia", href = Senwiki)
      )
    )
    
  })

  build_src_url_fun <- eventReactive(input$input_fg_item_id, {
    cli::cli_alert("button clicked")
    if(is.null(input$input_fg_item_id)) {
      print(paste("chosen properties:", input$input_fg_item_id))
      shinyFeedback::showToast(
        "error",
        "You have to select a Factgrid QID of a person."
      )
    } else {
      src_url <- build_src_url(input$input_fg_item_id)
      
      shinyFeedback::showToast(
        "success",
        "loading data from three data sources"
      )
      #print(data)
      src_url
    }
  })
  
  output$iframe <-  renderUI({
    cli::cli_alert("build query")
    cli::cli_alert("Input fg_item: {input$input_fg_item_id}")
    iframe_tmp <- tags$iframe(src = build_src_url_fun(), height = 600, width="100%", frameborder = "no", data_external="1")
    iframe_tmp
  })
  
  output$table <- renderDataTable(persons %>% select(Identifier = item, Label = label, Description = desc))
}

shinyApp(ui = ui, server = server)