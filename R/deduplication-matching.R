#' Calculate Similarities 
#' 
#' This functions compute similarities between entries in a document frequency matrix `dfm()` and return a dataframe with distinct id combinations.
#' It heavily relies on the quanteda package
#' 
#' @param data data as a document frequency matrix with a set `doc_id`
#' @param method character; the method identifying the similarity or distance measure to be used, see `?quanteda::textstat_simil`
#' @param min_sim numeric; a threshold for the similarity values below which similarity values will not be returned
#' @export
#' @return dataframe containing the two id's and the similarity value

calc_similarity <- function(data, method, min_sim) {
  
  # calculate similarities
  sims <- quanteda.textstats::textstat_simil(data, method = method, min_simil = min_sim)
  
  # transform similarities to a distinct dataframe containing the id's and the similarity value
  sims_df <- as.data.frame(as.matrix(sims)) %>%
    tibble::rownames_to_column("id_1") %>%
    tidyr::pivot_longer(2:ncol(.), names_to = "id_2", values_to = "value") %>% 
    # not with itself
    # and only when similiarity measure > 0
    dplyr::filter(id_1 != id_2, value > 0) %>% 
    # make them distinct
    dplyr::mutate(id_1_id_2 = map2_chr(id_1, id_2, collapse_to_distinct_rows)) %>% 
    dplyr::distinct(id_1_id_2, .keep_all = TRUE) %>% 
    dplyr::select(-id_1_id_2)
}


