#' Split a human full name in its parts
#' 
#' ## Rules:
#' 
#' * lastname = last word
#' * firstname = first word
#' * middlename = everything not first word or last word
#' * initial = first character
#' 
#' Beware that there will be a warning `argument is not an atomic vector; coercing `
#' It works nonetheless.
#' 
#' @param data dataframe containing a column with a human name
#' @param name_col string; name of the column containing the name, defaults to `name`
#' @importFrom stringr word
#' @importFrom stringr str_sub
#' @importFrom dplyr mutate
#' @export
#' @return dataframe with 4 new colums: lastname, firstname, middlename, initial
#' @example
#' df <- data.frame(name = c("Rita Mae Brown", "vorname df dsafasf", "sdf asdfd"))
#' split_human_name(df)

split_human_name <- function(data, name_col = "name") {
  data %>% 
    dplyr::mutate(
      !!name_col := trimws(.data[[name_col]]),
      # lastname = last word
      lastname = stringr::word(.data[[name_col]], start = -1),
      # firstname = first word
      firstname = stringr::word(.data[[name_col]], start = 1),
      # middlename = everything not first word or last word
      middlename = stringr::word(.data[[name_col]], start = 2, end = -2),
      middlename = ifelse(middlename == "", NA_character_, middlename),
      # initial = first character
      initial = stringr::str_sub(.data[[name_col]], start = 1, end = 1)
  )
}