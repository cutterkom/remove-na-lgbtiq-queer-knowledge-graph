
library(tidyverse)
library(kabrutils)
library(config)
library(testdat)
library(DBI)

verbose <- F

con <- connect_db()
er_candidates <- tbl(con, "er_candidates") %>% 
  filter(created_at > "2022-04-29 00:00:00") %>% 
  filter(decision == "positive") %>% 
  rename(cand_id = id) %>% 
  collect()

DBI::dbDisconnect(con); rm(con)

con <- connect_db("db_clean")
entities <- tbl(con, "entities") %>% 
  collect()

DBI::dbDisconnect(con); rm(con)

# Resolve manual decisions and create a new ID for those ------------------

new_ids_raw <- er_candidates %>%
  filter(decision == "positive") %>% 
  #head(1) %>% 
  pmap_dfr(function(...) {
    
    current <- tibble(...)
    
    if (verbose == TRUE) {
      message("current data:")
      print(current)
    }
    
    candidate_1 <- entities %>% 
      inner_join(current, by = c("id" = "id_1")) %>% 
      mutate(id_new = paste0("er_", cand_id)) %>% 
      #select(id, id_new, name_1 = name, keep_label_2, new_label)
      select(id, cand_id, name_1 = name, keep_label_2, new_label)
    
    if (verbose == TRUE) {
      message("candidate_1:")
      print(candidate_1)
    }
    
    candidate_2 <- entities %>% 
      inner_join(current, by = c("id" = "id_2")) %>% 
      #mutate(id_new = paste0("er_", cand_id)) %>% 
      #select(id, id_new, name_2 = name, keep_label_2, new_label)
      select(id, cand_id, name_2 = name, keep_label_2, new_label)
    
    if (verbose == TRUE) {
      message("candidate_2:")
      print(candidate_2)
    }
    
    er_pair <- candidate_1 %>% 
      #left_join(candidate_2, by = c("id", "keep_label_2", "new_label"), suffix = c("_1", "_2")) %>% 
      left_join(candidate_2, by = c("cand_id", "keep_label_2", "new_label"), suffix = c("_1", "_2")) %>% 
      mutate(
        name = 
          case_when(
            keep_label_2 == 1 ~ name_2,
            !is.na(new_label) ~ new_label,
            TRUE ~ name_1
          ),
        id = paste0("er_", cand_id)
      ) %>% 
      filter(!is.na(name)) %>% 
      select(-cand_id) %>% 
      select(id, everything())
    
    if (verbose == TRUE) {
      message("er_pair:")
      print(er_pair)
    }
    
    er_pair
    
  })



# Update books_authors ----------------------------------------------------

books_authors <- new_ids_raw %>% 
  filter(str_detect(id_1, "book_author"))

books_authors

books_authors %>% 
  filter(id_1 != "book_author_178") %>% 
  pmap(function(...) {
    current <- tibble(...)
    print(current)
    
    update <- paste0('
      UPDATE books_authors
      SET id = "', current$id_2, '"
      WHERE id = "', current$id_1, '"; 
    ')
    print(update)
    
    delete <- paste0(
      'DELETE FROM entities WHERE id = "', current$id_1, '";'
    )
    print(delete)
    con <- connect_db("db_clean")
    dbExecute(con, update)
    dbExecute(con, delete)
    dbDisconnect(con); rm(con)
  })


# Todo --------------------------------------------------------------------

to_do <- new_ids_raw %>% 
  anti_join(
    books_authors
  )

deutsche_eiche <- to_do %>% filter(str_detect(name_1, "Eiche"))

# manually done

to_do <- new_ids_raw %>% 
  anti_join(
    books_authors
  ) %>% anti_join(    deutsche_eiche)



posters_authors <- to_do %>% 
  filter(str_detect(id_2, "poster_author"))

posters_authors

posters_authors %>% 
  #tail(1) %>% View
  pmap(function(...) {
    current <- tibble(...)
    print(current)
    
    update <- paste0('
      UPDATE posters_authors
      SET id = "', current$id_1, '"
      WHERE id = "', current$id_2, '"; 
    ')
    print(update)
    
    delete <- paste0(
      'DELETE FROM entities WHERE id = "', current$id_2, '";'
    )
    print(delete)
    con <- connect_db("db_clean")
    dbExecute(con, update)
    dbExecute(con, delete)
    dbDisconnect(con); rm(con)
  })


to_do <- new_ids_raw %>% 
  anti_join(
    books_authors
  ) %>% anti_join(deutsche_eiche) %>% 
  anti_join(posters_authors)

to_do %>% clipr::write_clip()
