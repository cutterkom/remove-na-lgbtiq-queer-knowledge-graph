# replicate fingerprinting method in open refine

# https://github.com/OpenRefine/OpenRefine/wiki/Clustering-In-Depth


# remove leading and trailing whitespace x
# change all characters to their lowercase representation x
# remove all punctuation and control characters x
# normalize extended western characters to their ASCII representation (for example "gödel" → "godel") x
# split the string into whitespace-separated tokens
# sort the tokens and remove duplicates
# join the tokens back together


input_df <- authors %>% 
  select(id = author_id, name = author) %>% 
  mutate(id = paste0("books_", id)) %>% 
  dplyr::mutate(
    # remove whitespace and transform to lowercase
    name = trimws(tolower(name)),
    # remove all punctuations and digits
    name = str_remove_all(name, "[:punct:]|[:digit:]"),
    name = stringi::stri_trans_general(name, "de-ASCII; Latin-ASCII"))