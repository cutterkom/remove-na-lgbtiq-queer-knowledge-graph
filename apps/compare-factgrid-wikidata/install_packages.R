packages <- c("tidyverse", 
              "shiny", 
              "glue", 
              "here", 
              "flexdashboard", 
              "shinyFeedback", 
              "devtools")
install.packages(packages)
devtools::install_github("cutterkom/kabrutils")
devtools::install_github("cutterkom/SPARQL")
devtools::install_github("TS404/WikidataR")