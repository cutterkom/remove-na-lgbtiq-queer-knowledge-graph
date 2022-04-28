write_wikibase <- function(items,
                           properties      = NULL,
                           values          = NULL,
                           qual.properties = NULL,
                           qual.values     = NULL,
                           src.properties  = NULL,
                           src.values      = NULL,
                           remove          = FALSE,
                           format          = "tibble",
                           api.username    = NULL,
                           api.token       = NULL, # Find yours from [your user page](https://tools.wmflabs.org/quickstatements/#/user)
                           api.format      = "v1",
                           api.batchname   = NULL,
                           api.submit      = TRUE
){
  
  print(paste0("properties: ", properties))
  # Check if username and token provided
  if(format=="api"){
    if(is.null(api.username)){stop("Enter your Wikimedia username")}
    if(is.null(api.token))   {stop("Enter your api.token (Find yours at https://tools.wmflabs.org/quickstatements/#/user)")}
  }
  
  # Place all the quickstatements variables into a list 
  QS <- list(items           = items,
             properties      = properties,
             values          = values,
             qual.properties = qual.properties,
             qual.values     = qual.values,
             src.properties  = src.properties,
             src.values      = src.values)
  message("prop in QS list:")
  print(QS$properties)
  QS <- lapply(QS,function(x){if(!is.null(x)){tibble(x)}})
  #View(QS)
  
  # If new QIDs are being created via the "CREATE" keyword, need to insert blank lines across the other parameters to align correctly into rows
  # This is the most similar to the standard quickstatements method, though the "CREATExyz" method is preferred (see createrows.tidy function later)
  QS$properties      <- createrows(QS$items,QS$properties)
  QS$values          <- createrows(QS$items,QS$values)
  QS$qual.properties <- createrows(QS$items,QS$qual.properties)
  QS$qual.values     <- createrows(QS$items,QS$qual.values)
  QS$src.properties  <- createrows(QS$items,QS$src.properties)
  QS$src.values      <- createrows(QS$items,QS$src.values)
  
  # If same number of rows as the rowmax, do nothing
  # If only one row, repeat it rowmax times
  # If wrong number of rows, stop with an error message
  rowcount <- unlist(lapply(QS,nrow))
  rowmax   <- max(rowcount)
  stoprun  <- FALSE
  
  if(var(unlist(rowcount))!=0){
    for (x in 1:length(QS)){
      if(nrow(QS[[x]])==rowmax){ 
        QS[[x]] <- QS[[x]]
      }else if (nrow(QS[[x]])==1){ 
        QS[[x]] <- slice(QS[[x]],rep(1:n(), each=rowmax)) 
      }else{
        stoprun<-TRUE
        warning(paste0("Not all quickstatement columns have equal rows: ",
                       nrow(QS$items)," items (including ",
                       sum(is.create(unlist(QS$items)))," new QIDs to CREATE) were provided, but ",
                       names(QS)[x],
                       " has ",
                       nrow(QS[[x]]),
                       " rows (expecting ",
                       nrow(QS$items),
                       ")."))
      }
    }
  }
  if(stoprun){stop("Therefore stopping")}
  
  # Convert values to QIDs where possible and identify which (if any) to remove
  
  # Removed to due error -> no idea of consequences -------------------------
  
  # QS$items           <- as_qid(QS$items)
  QS$items[remove,]  <- paste0("-",unlist(QS$items[remove,]))
  
  # Convert properties to PIDs where possible, unless special functions (such as lables and aliases)
  #QS$properties      <- as_pid(QS$properties)
  print("QS$properties:")
  print(QS$properties)
  # Convert values to QIDs where possible, unless property is expecting a string
  QS$values          <- tibble(QS$values)
  # if(any(sapply(QS$properties,check.PID.WikibaseItem))){
  #   QS$values[sapply(QS$properties,check.PID.WikibaseItem),] <- as_qid(QS$values[sapply(QS$properties,check.PID.WikibaseItem),])
  # }
  QS$values          <- as_quot(QS$values,format)
  
  # Convert first three columns into tibble (tibbulate?)
  colnames(QS$items)      <- "Item"
  colnames(QS$properties) <- "Prop"
  colnames(QS$values)     <- "Value"
  
  QS.tib <- bind_cols(QS$items,
                      QS$properties,
                      QS$values)  
  
  # optionally, append columns for qualifier properties and qualifier values for those statements
  if(!is.null(QS$qual.properties)|!is.null(QS$qual.values)){
    QS$qual.properties <- as_pid(QS$qual.properties)
    QS$qual.values     <- as_quot(QS$qual.values,format)
    
    colnames(QS$qual.properties) <- paste0("Qual.prop.",1:ncol(QS$qual.properties))
    colnames(QS$qual.values)     <- paste0("Qual.value.",1:ncol(QS$qual.values))
    
    QSq <- list(QS$qual.properties,
                QS$qual.values)
    QSq.check  <- var(sapply(c(QS,QSq),function(x){if(is.null(dim(x))){length(x)}else{nrow(x)}}))==0
    if(!QSq.check){stop("Incorrect number of qualifiers provided. If no qualifers needed for a statement, use NA or \"\".")}
    
    QS.qual.tib <- as_tibble(cbind(QSq[[1]],QSq[[2]])[,c(rbind(1:ncol(QSq[[1]]),ncol(QSq[[1]])+1:ncol(QSq[[2]])))])
    
    QS.tib <- tibble(QS.tib,
                     QS.qual.tib)
  }
  
  # optionally, append columns for source properties and source values for those statements
  if(!is.null(src.properties)|!is.null(src.values)){
    QS$src.properties <- as_sid(QS$src.properties)
    QS$src.values     <- as_quot(QS$src.values,format)
    
    colnames(QS$src.properties) <- paste0("Src.prop.",1:ncol(QS$src.properties))
    colnames(QS$src.values)     <- paste0("Src.values.",1:ncol(QS$src.values))
    
    QSs <- list(QS$src.properties,
                QS$src.values)
    QSs.check  <- var(sapply(c(QS,QSs),function(x){if(is.null(dim(x))){length(x)}else{nrow(x)}}))==0
    if(!QSs.check){stop("incorrect number of sources provided")}
    
    QS.src.tib <- as_tibble(cbind(QSs[[1]],QSs[[2]])[,c(rbind(1:ncol(QSs[[1]]),ncol(QSs[[1]])+1:ncol(QSs[[2]])))])
    
    QS.tib <<- tibble(QS.tib,
                      QS.src.tib)
  }
  
  # if new QIDs are being created via tidy "CREATExyz" keywords, need to insert CREATE lines above and replace subsequent "CREATExyz" with "LAST"
  QS.tib <- createrows.tidy(QS.tib)
  View(QS.tib)
  # output
  if (format=="csv"){
    write.table(QS.tib,quote = FALSE,row.names = FALSE,sep = ",")
  }
  # format up the output
  if (format=="tibble"){
    return(QS.tib)
  }
  if (format=="api"|format=="website"){
    
    api.temp1 <- format_tsv(QS.tib, col_names = FALSE)
    api.temp2 <- gsub("\t", "%7C",api.temp1) # Replace TAB with "%7C"
    api.temp3 <- gsub("\n", "%7C%7C",api.temp2) # Replace end-of-line with "%7C%7C"
    api.temp4 <- gsub(" ",  "%20",api.temp3) # Replace space with "%20"
    api.temp5 <- gsub("\\+","%2B",api.temp4) # Replace plus with "%2B"
    api.data  <- gsub("/",  "%2F",api.temp5) # Replace slash with "%2F"
    
    if (format=="api"){
      if (is.null(api.token)){stop(paste0("API token needed. Find yours at", quickstatement_url, "#/user"))}
      url <- paste0(quickstatement_url, "api.php",
                    "?action=",   "import",
                    "&submit=",   "1",
                    "&format=",   api.format,
                    "&batchname=",api.batchname,
                    "&username=", api.username,
                    "&token=",    api.token,
                    "&data=",     api.data)
    }
    if (format=="website"){
      # not working with v2
      url <- paste0(quickstatement_url, "#/v1=",
                    "&data=", api.data)
    }
    if(api.submit){
      browseURL(url)
    }else{
      return(url)
    }
  }
}





# Test --------------------------------------------------------------------

wikibase_url <- "https://database.factgrid.de/"
wikibase_domain <- "database.factgrid.de"
sparql_endpoint <- "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
wikibase_api <- "https://www.wikidata.org/w/api.php"
quickstatement_url <- "https://database.factgrid.de/quickstatements/"
token <- "$2y$10$sZqLZ3rwMtebHZktneERZuJxquO5yfTZQb/aUukxB9HrMlimEkwsu"
username <- "Katharina Brunner"
api.username <-  "Katharina Brunner"
api.token <- "$2y$10$sZqLZ3rwMtebHZktneERZuJxquO5yfTZQb/aUukxB9HrMlimEkwsu"






write_wikibase(items = "Q399986",
               properties      = "P2",
               values          = "Q10427",
               qual.properties = NULL,
               qual.values     = NULL,
               src.properties  = NULL,
               src.values      = NULL,
               remove          = FALSE,
               format          = "api",
               api.username    = api.username,
               api.token       = api.token,
               api.format      = "v1",
               api.batchname   = "api_test",
               api.submit      = T)



# write from table --------------------------------------------------------


# klappt, aber dann ist jede call ein eigener batch - ist das klug?

test_data %>% 
  rowid_to_column() %>% 
  pmap(function(...) {
    current <- tibble(...)
    write_wikibase(items = current$item,
                   properties      = current$prop,
                   values          = current$value,
                   qual.properties = NULL,
                   qual.values     = NULL,
                   src.properties  = NULL,
                   src.values      = NULL,
                   remove          = FALSE,
                   format          = "api",
                   api.username    = api.username,
                   api.token       = api.token,
                   api.format      = "v1",
                   api.batchname   = paste0("api_test", current$rowid),
                   api.submit      = T)
  })


# klappt: mehrere änderungen mit 1 Call
write_wikibase(items = "Q399986",
               properties      = "P2",
               values          = c("Q10427", "Q400012"),
               qual.properties = NULL,
               qual.values     = NULL,
               src.properties  = NULL,
               src.values      = NULL,
               remove          = FALSE,
               format          = "api",
               api.username    = api.username,
               api.token       = api.token,
               api.format      = "v1",
               api.batchname   = paste0("api_test_multiple"),
               api.submit      = T)

# klappt das auch bei zwei verschiedenen Items?

test_data <- tribble(
  ~item, ~prop, ~value,
  c("Q399986", "Q399988"), "P2", list("Q10427", "Q400012")
) %>% 
  unnest(c(value, item))

# klappt
write_wikibase(items = test_data$item,
               properties      = test_data$prop,
               values          = test_data$value,
               qual.properties = NULL,
               qual.values     = NULL,
               src.properties  = NULL,
               src.values      = NULL,
               remove          = FALSE,
               format          = "api",
               api.username    = api.username,
               api.token       = api.token,
               api.format      = "v1",
               api.batchname   = paste0("api_test_multiple_from_one_df"),
               api.submit      = T)



# Neues Item --------------------------------------------------------------

write_wikibase(items = "",
               properties      = "P2",
               values          = "Q10427",
               qual.properties = NULL,
               qual.values     = NULL,
               src.properties  = NULL,
               src.values      = NULL,
               remove          = FALSE,
               format          = "api",
               api.username    = api.username,
               api.token       = api.token,
               api.format      = "v1",
               api.batchname   = "api_test",
               api.submit      = T)


