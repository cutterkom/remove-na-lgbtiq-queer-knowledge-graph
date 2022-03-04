#' Clean string
#' 
#' What it does:
#' - transforming to lower-case
#' - removing everything that is not a character A-z (also spaces)
#' - transforming all special characters (ä, ß, ...) to 1) de-ASCII and 2) Latin-ASCII (see `stri_trans_list()`)
#' 
#' @name clean_string
#' 
#' @param df input dataframe
#' @param col columns to clean, given as list of string
#' @param address if `TRUE` than `strasse` etc will be abbreviated to `str`
#' @param backup boolean, TRUE create a new column as backup
#' @param backup_suffix string, used to add a suffix in case of backup == TRUE, default to "_backup"
#' 
#' @importFrom stringr str_replace_all
#' @importFrom stringr str_remove_all
#' @importFrom stringi stri_trans_general
#' @importFrom stringr str_replace
#' @importFrom dplyr enquo
#' 
#' @export
#' @examples
#' Data <- data.frame(
#' firstname = c("Gildas"), 
#' address1 = c("Ismaninger Straße 22, 81675 Munich"),
#' address2 = c("10 Pl. Jean de Berry, Frankreich"))
#' Data %>% clean_string(col = c("address1", "address2"), backup = TRUE)
#' 

# clean_string <- function(df, col, backup = FALSE, backup_suffix = "_orig") {
#   for( this_col in col ){
#     column <- sym(this_col) #rlang
#     #column <- enquo(col) #rlang
#     if(backup) { df <- df %>% mutate("{{column}}{backup_suffix}" := {{ column }}) }
#     #lower, ascii only, no punct
#     df <- df %>% 
#       dplyr::mutate(
#         {{ column }} := tolower({{ column }}),
#         {{ column }} := str_replace_all({{ column }}, "\\s|[[:punct:]]", ""),
#         {{ column }} := stri_trans_general({{ column }}, "de-ASCII; Latin-ASCII")
#       )
#   }
#   return(df)
# }


#' Collapse ID cols to distinct rows
#' 
#' Imagine you have a dataframe with two id columns that contain two rows that actually are just one combination. 
#' This function distincts them in a `purrr` workflow.
#' 
#' Copied from: https://stackoverflow.com/a/62381665/2646974
#' 
#' @importFrom dplyr mutate
#' @importFrom dplyr distinct
#' @importFrom dplyr select
#' @importFrom purrr map2_chr
#' @examples 
#' df <- data.frame(id_1 = c(1,2), id_2 = c(2,1))
#' df %>% 
#'   dplyr::mutate(id_1_id_2 = purrr::map2_chr(id_1, id_2, collapse_to_distinct_rows)) %>% 
#'   dplyr::distinct(id_1_id_2, .keep_all = TRUE) %>% 
#'   dplyr::select(-id_1_id_2)

collapse_to_distinct_rows <- function(...){
  c(...) %>% 
    sort() %>% 
    str_c(collapse = ".")
}
