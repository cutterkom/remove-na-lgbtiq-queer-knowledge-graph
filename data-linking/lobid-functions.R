#' Call lobid API
#'
#' see: http://lobid.org/resources/api
#'
#' @param query string
#' @param parameter string, e.g. isbn
#' @param as_list default: ``FALSE``. If `TRUE` then json is converted to a R list, otherwise just url
#' @return list
#'
#' @importFrom jsonlite fromJSON
#' @importFrom cli cli_alert_info
#' @example
#' 3894090685 %>% fetch_lobid(query = ., parameter = "isbn")
#'

call_lobid_api <- function(query, parameter = NULL, verbose = TRUE, as_list = FALSE) {

  # build URL according to   cli::cli_alert_info("URL: {url}")
  if (is.null(parameter)) {
    url <- paste0("https://lobid.org/resources/search?q=", query, "&format=json")
    url <- URLencode(url)
  } else if (!is.null(parameter)) {
    url <- paste0("https://lobid.org/resources/search?q=", parameter, ":", query, "&format=json")
  }

  if (verbose == TRUE) {
    cli::cli_alert_info("URL: {url}")
  }

  if (as_list == TRUE) {
    json <- jsonlite::fromJSON(url)
    json 
  } else {
      url
    }
  
}


#' Get value of a special field
#' 
#' This function fetches the value of fields in a nested json, no matter on which level.
#' Based on the very popular js, JSON command line processor https://stedolan.github.io/jq/
#' 
#' @param url the json url
#' @param field fieldname/key to be fetched
#' @importFrom curl curl
#' @importFrom jqr jq
#' @example 
#' "https://lobid.org/resources/search?q=isbn:3596237785&format=json" %>% get_field_value("gndIdentifier")
get_field_values <- function(url, jq_syntax) {
  curl::curl(url) %>% jqr::jq(jq_syntax)
}


