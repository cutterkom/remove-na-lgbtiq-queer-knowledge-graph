# title: Check Duplicates - Prototype
# desc: In order to get a feeling of the quality of the entities, I need to find possible duplicates. Deduplication will be done later. 
# input: DB tbl books_authors
# output: 

# Example duplicates in data:
# August Graf Platen - August Graf Platen
# Angstmann Gustl - Gustl Angstmann
# Armistad Maupin - Armistead Maupin

library(tidyverse)
library(kabrutils)
library(quanteda)
library(quanteda.textstats)


con <- connect_db()
authors <- tbl(con, "books_authors") %>%
  collect()
DBI::dbDisconnect(con); rm(con)


authors <- authors %>% 
  select(author_id, author) %>% 
  # rough guess of lastname: take last word in authors column
  mutate(
    author = trimws(author),
    lastname = word(author, start = -1),
    firstname = word(author, start = 1),
    initial = str_sub(author, start = 1, end = 1)
    ) %>% 
  
  filter(
    # remove Verlage and Vereine
    !str_detect(tolower(author), "verlag|e.v."),
    # remove ? 
    !author %in% c("?", "et al."),
    # remove if there is no space in the name -> just 1 name or abbreviation
    str_detect(author, "\\s")
    )

# Similarity measures using Quanteda --------------------------------------

# create a document frequency matrix
authors_dfm <- corpus(authors, text_field = "author", docid_field = "author_id") %>% 
  tokens() %>% 
  dfm()

sims_jaccard <- textstat_simil(authors_dfm, authors_dfm, method = "jaccard", sorted = TRUE, diag = FALSE)

possible_duplicates_jaccard <- as.data.frame(as.matrix(sims_jaccard)) %>%
  rownames_to_column("id_1") %>%
  pivot_longer(2:ncol(.), names_to = "id_2", values_to = "value") %>% 
  mutate(id_1 = as.numeric(id_1),
         id_2 = as.numeric(id_2)) %>% 
  # not with itself
  # and only when similiarity measure > 0
  filter(id_1 != id_2, value > 0) %>% 
  # add plain strings
  left_join(authors, by = c("id_1" = "author_id")) %>% 
  left_join(authors, by = c("id_2" = "author_id"), suffix = c("_1", "_2")) #%>% 
  #filter(lastname_1 == lastname_2)


# Cosine ------------------------------------------------------------------

sims_cosine <- textstat_simil(authors_dfm, authors_dfm, method = "cosine", sorted = TRUE, diag = FALSE)

possible_duplicates_cosine <- as.data.frame(as.matrix(sims_cosine)) %>%
  rownames_to_column("id_1") %>%
  pivot_longer(2:ncol(.), names_to = "id_2", values_to = "value") %>% 
  mutate(id_1 = as.numeric(id_1),
         id_2 = as.numeric(id_2)) %>% 
  # not with itself
  # and only when similiarity measure > 0
  filter(id_1 != id_2, value > 0) %>% 
  # add plain strings
  left_join(authors, by = c("id_1" = "author_id")) %>% 
  left_join(authors, by = c("id_2" = "author_id"), suffix = c("_1", "_2")) #%>% 
  #filter(lastname_1 == lastname_2)




# Add similarites ---------------------------------------------------------

test <- authors %>% 
  left_join(possible_duplicates_cosine #%>% 
              #select(id_1, id_2, cosine_sim = value)
              , 
            by = c("author_id" = "id_1")) %>% 
  left_join(possible_duplicates_jaccard #%>% 
              #select(id_1, id_2, jaccard_sim = value)
              , 
            by = c("author_id" = "id_1"))
  
# wo sagt der eine treffer und der andere nicht?
test %>% filter(is.na(id_2.y), !is.na(id_2.x))

possible_duplicates_cosine
possible_duplicates_jaccard



# Test cases --------------------------------------------------------------

# Angstmann Gustl - Gustl Angstmann -> wird NICHT gefunden
# Armistad -> wird gefunden, aber Levenstein wäre viel besser



# Armistad Maupin - Armistead Maupin --------------------------------------

# best found with Levensthein
# not token based

dfA <- authors %>% 
  rename(id_1 = author_id,
         author_1 = author,
         lastname_1 = lastname,
         initial_1 = initial,
         firstname_1 = firstname)

dfB <- authors %>% 
  rename(id_2 = author_id,
         author_2 = author,
         lastname_2 = lastname,
         initial_2 = initial,
         firstname_2 = firstname)

distances <- expand_grid(dfA, dfB) %>% 
  filter(id_1 != id_2) %>% 
  mutate(
    lv = stringdist::stringdist(author_1, author_2, method = "lv"),
    osa = stringdist::stringdist(author_1, author_2, method = "osa"),
    dl = stringdist::stringdist(author_1, author_2, method = "dl"),
    same_lastname = lastname_1 == lastname_2)


possible_duplicates <- distances %>% 
  filter(lv < 3 | osa < 3 | dl < 3) 




# Wrap up -----------------------------------------------------------------

# 2 similiarty measures and 1 distinance measure

check_multiple_tests <- authors %>% 
  left_join(possible_duplicates_cosine %>% 
              select(id_1, cosine_sim_id_2 = id_2, cosine_sim = value), 
            by = c("author_id" = "id_1")) %>% 
  left_join(possible_duplicates_jaccard %>% 
              select(id_1, jaccard_sim_id_2 = id_2, jaccard_sim = value), 
            by = c("author_id" = "id_1")) %>% 
  left_join(possible_duplicates %>% select(id_1, lv_id_2 = id_2, lv_dist = lv),
            by = c("author_id" = "id_1")) %>% 
  # get possible duplicate strings from original data per sim/dist
  left_join(authors, by = c("cosine_sim_id_2" = "author_id"), suffix = c("", "_cosine")) %>% 
  left_join(authors, by = c("jaccard_sim_id_2" = "author_id"), suffix = c("", "_jaccard")) %>% 
  left_join(authors, by = c("lv_id_2" = "author_id"), suffix = c("", "_lv")) %>% 
  # get only those that got a positive value
  filter(!is.na(cosine_sim) | !is.na(jaccard_sim) | !is.na(lv_dist)) %>% 
  # group cols for output
  select(contains("id"), contains("author"), contains("firstname"), contains("lastname"), contains("initial"), contains("sim"), contains("dist")) %>% 
  mutate(
    same_initial = 
      case_when(
        initial == initial_cosine ~ 1,
        initial == initial_jaccard ~ 1,
        initial == initial_lv ~ 1,
        TRUE ~ 0
      ),
    same_lastname = 
      case_when(
        lastname == lastname_cosine ~ 1,
        lastname == lastname_jaccard ~ 1,
        lastname == lastname_lv ~ 1,
        TRUE ~ 0
      ),
    same_firstname = 
      case_when(
        firstname == firstname_cosine ~ 1,
        firstname == firstname_jaccard ~ 1,
        firstname == firstname_lv ~ 1,
        TRUE ~ 0
      ),
    
    # do the different methods propse the same entities?
    same_id = 
      case_when(
        cosine_sim_id_2 == jaccard_sim_id_2 ~ 1,
        cosine_sim_id_2 == lv_id_2 ~ 1,
        jaccard_sim_id_2 == lv_id_2 ~ 1,
        TRUE ~ 0
      )
  ) %>% 
  filter(
    cosine_sim >= 0.5 |
    jaccard_sim >= 0.3 |
    lv_dist < 3
  ) %>% 
  rowwise() %>% 
  mutate(
    # calculate a meta metric in order to determine a hierarchy of tests
    # since lv_dist is > 1 and it's calculates as 1/dist and * 2 in order to preserve
    # a high relevance when it occurs
    meta_sim = sum(cosine_sim, jaccard_sim, (1/lv_dist)*2, na.rm = TRUE)
  )




# netto Prüffälle
check_multiple_tests %>% filter(meta_sim >= 1, same_id == 1)  %>% View#distinct(author_id)

# Educated Guess
# cosine_sim > 0.5 seems to be relevant
# jaccard_sim > 0.3 seems to be relevant
# lv_dist > 3


# Testfälle:
# August Graf Platen - August Graf Platen -> wird gefunden, meta_sim = 1.1666667, same_id = 1
# Angstmann Gustl - Gustl Angstmann -> wird gefunden, meta_sim = 2, same_id = 1
# Armistad Maupin - Armistead Maupin -> wird gefunden, meta_sim = 2.8, same_id = 1



# Zwischenfazit -----------------------------------------------------------

# An sich kann das funktionieren, aber das Datenhandling ist noch zu durcheinander
# Wide und rectangualar ist für diesen Prototypen okay. 
# Refatcoring und effizienter machen notwendig
