# find possible duplicates within chronik entities

library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)
library(testdat)

con <- connect_db("db_clean")
entities <- tbl(con, "entities") %>% 
  select(id, name) %>% 
  collect()
DBI::dbDisconnect(con); rm(con)

# Distinct Entities -------------------------------------------------------

input <- entities

# Get string mappings -----------------------------------------------------

# used to remove authors who seem to be organisations
string_mappings <- config::get(file = "static/string-mapping.yml") 

bigrams_input <- input %>% 
  select(id, name) %>% 
  clean_string(col = "name") %>% 
  filter(
    !str_detect(tolower(name), paste0(string_mappings$nonsense, collapse = "|")),
    # remove if there is no space in the name -> just 1 name or abbreviation
    str_detect(name, "\\s")
  ) %>% 
  mutate(
    name = str_remove_all(tolower(name), paste0(string_mappings$organisations_marker, collapse = "|"))
    )

bigrams_input 

# Prepare text data -------------------------------------------------------

corp <- corpus(bigrams_input, text_field = "name", docid_field = "id")
toks <- tokens(corp)

# create bigrams on word level = shingles
bigrams <- corp %>% 
  tokens(what = "character") %>%
  tokens_keep("[A-Za-z]", valuetype = "regex") %>%
  tokens_ngrams(n = 2, concatenator = "") %>%
  dfm()

# Calculate similarities -> create candidate pairs -------------------------

min_sim <- 0.75
candidates <- calc_similarity(bigrams, method = "cosine", min_sim = min_sim) %>% 
  left_join(input, by = c("id_1" = "id")) %>% 
  left_join(input, by = c("id_2" = "id"), suffix = c("_1", "_2"))




# JQ Stringdist -----------------------------------------------------------

library(stringdist)

calculate_similarity_tidy <- function(data_1, data_2, method, min_sim) {
  expand_grid(data_1, data_2, .name_repair = "check_unique") %>%
    filter(id_1 != id_2) %>%
    filter(str_sub(name_1, 1, 1) == str_sub(name_2, 1, 1)) %>%
    mutate(value = 1 - stringdist(a = name_1, b = name_2, method = method)) %>%
    filter(value > min_sim) %>%
    dplyr::mutate(id_1_id_2 = map2_chr(
      id_1,
      id_2, collapse_to_distinct_rows
    )) %>%
    dplyr::distinct(id_1_id_2, .keep_all = TRUE) %>%
    dplyr::select(-id_1_id_2) %>%
    dplyr::mutate(rank = dplyr::dense_rank(dplyr::desc(value))) %>%
    dplyr::arrange(rank)
}


jw_from_fun <- calculate_similarity_tidy(
  bigrams_input %>% select(name_1 = name, id_1 = id), 
  bigrams_input %>% select(name_2 = name, id_2 = id), 
  "jw", 
  min_sim = 0) %>% 
filter(!str_detect(name_1, "traße"), !str_detect(name_2, "traße"))

# than manually deduplicated


bigrams_input %>% kabrutils::split_human_name("name") %>% View
