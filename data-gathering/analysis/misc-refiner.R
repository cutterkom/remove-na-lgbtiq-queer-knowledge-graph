library(tidyverse)
library(kabrutils)
library(refinr)

x <- c(
  "Clemsson University", 
  "university-of-clemson", 
  "CLEMSON", 
  "Clem son, U.", 
  "college, clemson u", 
  "M.I.T.", 
  "Technology, Massachusetts' Institute of", 
  "Massachusetts Inst of Technology", 
  "UNIVERSITY:  mit"
)


x <- books_authors$author
ignores <- c("university", "college", "u", "of", "institute", "inst")

x_refin <- x %>% 
  refinr::key_collision_merge(ignore_strings = ignores) %>% 
  refinr::n_gram_merge(ignore_strings = ignores)

# Create df for comparing the original values to the edited values.
# This is especially useful for larger input vectors.
inspect_results <- data_frame(original_values = x, edited_values = x_refin) %>% 
  mutate(equal = original_values == edited_values)

# Display only the values that were edited by refinr.

inspect_results[!inspect_results$equal, c("original_values", "edited_values")]


# Findet die Umgedrehten Vor- und Nachnamen super gut
books_authors %>% 
  mutate(key_coll = refinr::key_collision_merge(author, ignore_strings = ignores),
         key_coll_change = author != key_coll,
         ngram = refinr::n_gram_merge(author, ignore_strings = ignores),
         ngram_change = author != ngram) %>% 
  filter(ngram_change == TRUE | key_coll_change == TRUE) %>% View
