#' Clean string
#' 
#' - remove leading and trailing whitespace
#' - change all characters to their lowercase representation
#' - remove all punctuation and numbers
#' - transforming all special characters (ä, ß, ...) to 1) de-ASCII and 2) Latin-ASCII (see `stri_trans_list()`)
#'
#' @param data dataframe
#' @param col string; column name to clean
#' @importFrom dplyr mutate
#' @importFrom stringr str_replace_all
#' @importFrom stringi stri_trans_general
#' @export
#' @examples
#' df <- data.frame(name = "Fritz Müller-Scherz 2")
#' clean_string(df, "name")
clean_string <- function(data, col) {
  
  data %>% 
    dplyr::mutate(
      # remove all punctuations and digits
      !!col := str_replace_all(.data[[col]], "[:punct:]|[:digit:]", " "),
      # transform to ascii, keeping german Umlauts
      !!col := stringi::stri_trans_general(.data[[col]], "de-ASCII; Latin-ASCII"),
      # remove whitespace and transform to lowercase
      !!col := trimws(tolower(.data[[col]])))
}


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
#' @param col string; name of the column containing the name, defaults to `name`
#' @importFrom stringr word
#' @importFrom stringr str_sub
#' @importFrom dplyr mutate
#' @export
#' @return dataframe with 4 new colums: lastname, firstname, middlename, initial
#' @examples
#' df <- data.frame(name = c("Rita Mae Brown", "vorname df dsafasf", "sdf asdfd"))
#' split_human_name(df)

split_human_name <- function(data, col = "name") {
  data %>% 
    dplyr::mutate(
      !!col := trimws(.data[[col]]),
      # lastname = last word
      lastname = stringr::word(.data[[col]], start = -1),
      # firstname = first word
      firstname = stringr::word(.data[[col]], start = 1),
      # middlename = everything not first word or last word
      middlename = stringr::word(.data[[col]], start = 2, end = -2),
      middlename = ifelse(middlename == "", NA_character_, middlename),
      # initial = first character
      initial = stringr::str_sub(.data[[col]], start = 1, end = 1)
  )
}