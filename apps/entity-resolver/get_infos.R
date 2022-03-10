

#' get a cancidate pair to check
#' 
#' rule to choose
#' - not yet checked: `decision == "no_judgement"`
#' - minimum rank: highest similarity
#' - minimum id: in order to choose when similar rank
#' 
#' @param data dataframe with similarties
#' @return dataframe with 1 row
#' 
get_candidates_to_check <- function(data) {
  data %>% 
    dplyr::filter(decision == "no_judgement" & rank == min(rank) & id == min(id))
}


#' Get candidates names
#' 
#' Looks for id_1 and if source is books or poster. 
#' Then joins name from additional_infos df depending in source
#' Does same for id_2.
#' Then joins results for both together
#' 
get_candidates_names <- function(data) {
  candidate_1 <- data %>% 
    dplyr::distinct(id, source_1, id_1)
  
  if(candidate_1$source_1 == "book") {
    candidate_1 <- candidate_1 %>% 
      dplyr::left_join(candidates$books_additional_infos %>% 
                         dplyr::distinct(author_id, name_1 = author), by = c("id_1" = "author_id"))
  } else if (candidate_1$source_1 == "poster") { 
    candidate_1 <- candidate_1 %>% 
      dplyr::left_join(candidates$poster_additional_infos %>% 
                         dplyr::distinct(author_id, name_1 = author), by = c("id_1" = "author_id"))
  } else {
    print("not implemented")
  }
  
  candidate_2 <- data %>% 
    dplyr::distinct(id, source_2, id_2)
  
  if(candidate_2$source_2 == "book") {
    candidate_2 <- candidate_2 %>% 
      dplyr::left_join(candidates$books_additional_infos %>% 
                  dplyr::distinct(author_id, name_2 = author), by = c("id_2" = "author_id"))
  } else if (candidate_2$source_2 == "poster") { 
    candidate_2 <- candidate_2 %>% 
      dplyr::left_join(candidates$poster_additional_infos %>% 
                         dplyr::distinct(author_id, name_2 = author), by = c("id_2" = "author_id"))
  } else {
    print("not implemented")
  }
  
  candidate_names <- dplyr::inner_join(candidate_1, candidate_2, by = "id")
  candidate_names
}
  

#######


decision <- "next_candidates"

save_decision <- function(data, candidates) {
  
  if (decision == "positive") {
    print("it's positive, baby")
    result <- candidates_to_check %>% 
      mutate(
        decision = "positive",
        updated_at = Sys.time())
  } else if (decision == "positive") {
    print("nope, not the same")
    result <- candidates_to_check %>% 
      mutate(
        decision = "negative",
        updated_at = Sys.time())
  } else if (decision == "not_sure") {
    print("can't decide, sorry. needs to be checked again")
    result <- candidates_to_check %>% 
      mutate(
        decision = "not_sure",
        updated_at = Sys.time())
  } else if (decision == "next_candidates") {
    print("want to check next candidates")
  }
}

if (decision == "positive") {
  print("it's positive, baby")
  result <- candidates_to_check %>% 
    mutate(
      decision = "positive",
      updated_at = Sys.time())
} else if (decision == "positive") {
  print("nope, not the same")
  result <- candidates_to_check %>% 
    mutate(
      decision = "negative",
      updated_at = Sys.time())
} else if (decision == "not_sure") {
  print("can't decide, sorry. needs to be checked again")
  result <- candidates_to_check %>% 
    mutate(
      decision = "not_sure",
      updated_at = Sys.time())
} else if (decision == "next_candidates") {
  print("want to check next candidates")
}








decision <- "positive"

if (decision == "positive") {
  print("it's positive, baby")
  result <- candidates$similarities %>%
    mutate(
      decision = ifelse(id == candidates_to_check$id, "positive", decision),
      updated_at = Sys.time()
      )
} else if (decision == "positive") {
  print("nope, not the same")
  result <- candidates_to_check %>% 
    mutate(
      decision = "negative",
      updated_at = Sys.time())
} else if (decision == "not_sure") {
  print("can't decide, sorry. needs to be checked again")
  result <- candidates_to_check %>% 
    mutate(
      decision = "not_sure",
      updated_at = Sys.time())
} else if (decision == "next_candidates") {
  print("want to check next candidates")
}
  
result


candidates$similarities



















