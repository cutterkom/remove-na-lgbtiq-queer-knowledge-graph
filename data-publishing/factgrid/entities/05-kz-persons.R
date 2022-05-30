library(tidyverse)
library(kabrutils)
library(googlesheets4)


persons <- read_sheet("https://docs.google.com/spreadsheets/d/14wtFqTTJ9n2hpuZIj6I3lVXH5xG4jlr3tr1vkQsJuSU/edit#gid=411652859", sheet = "KZ Häftlinge") %>% 
  janitor::clean_names() %>% 
  rowid_to_column(var = "id")



# Birthdate ---------------------------------------------------------------

birthdates <- 
  bind_rows(
    persons %>% 
      distinct(id, geburtsdatum) %>% 
      filter(str_detect(geburtsdatum, "\\.")) %>% 
      mutate(birthdate = lubridate::dmy(geburtsdatum)),
    persons %>% 
      distinct(id, geburtsdatum) %>% 
      filter(str_detect(geburtsdatum, "\\/")) %>% 
      mutate(birthdate = lubridate::mdy(geburtsdatum))
  ) %>% 
  distinct(id, birthdate)


# Deathdates --------------------------------------------------------------


deathdates <- 
  bind_rows(
    persons %>% 
      distinct(id, todesdatum) %>% 
      filter(str_detect(todesdatum, "\\.")) %>% 
      mutate(deathdate = lubridate::dmy(todesdatum)),
    persons %>% 
      distinct(id, todesdatum) %>% 
      filter(str_detect(todesdatum, "\\/")) %>% 
      mutate(deathdate = lubridate::mdy(todesdatum))
  ) %>% 
  distinct(id, deathdate)



# Birthplace --------------------------------------------------------------

persons %>% 
  distinct(geburtsort) %>% clipr::write_clip()

rrefine::refine_export("kz-birthplaces")

# Haft --------------------------------------------------------------------

haft_long <- persons %>% 
  distinct(id, haft_ort_von_bis_art) %>% 
  separate_rows(haft_ort_von_bis_art, sep = ";") %>% 
  mutate(haft_ort_von_bis_art = trimws(haft_ort_von_bis_art)) %>% 
  distinct(id, haft = haft_ort_von_bis_art)

haft_count_per_person <- haft_long %>% count(id) %>% filter(n>3)



types <- persons %>% 
  distinct(id, txt = haft_ort_von_bis_art) %>% 
  mutate(
    schutzhaft = ifelse(str_detect(txt, "Schutzhaft|Schtuzhaft|Schutzhäftling"), 1, 0),
    sicherungsverwahrung = ifelse(str_detect(txt, "Sicherungsverwahrung"), 1, 0),
    uhaft = ifelse(str_detect(txt, "Untersuchungshaft|U-Haft"), 1, 0),
    gefängnis = ifelse(str_detect(txt, "Gefängnis"), 1, 0),
    strafhaft = ifelse(str_detect(txt, "Strafhaft"), 1, 0)
    )




# citizenship -------------------------------------------------------------

citizenship <- persons %>% 
  distinct(id, staatsangehorigkeit) %>% 
  mutate(
    P616 = 
      case_when(
        staatsangehorigkeit == "deutsch" ~ "Q256463",
        staatsangehorigkeit == "tschechisch" ~ "Q418268",
        staatsangehorigkeit == "österreichisch" ~ "Q418269",
        TRUE ~ NA_character_
      )
  )


# Religion ----------------------------------------------------------------

religion <- persons %>% 
  distinct(id, religion) %>% 
  mutate(
    P172 = 
      case_when(
        religion == "katholisch" ~ "Q22233",
        religion == "evangelisch" ~ "Q230447",
        religion == "jüdisch" ~ "Q23252",
        religion == "konfessionslos" ~ "Q418270",
        TRUE ~ NA_character_
      )
  )


# beruf --------------------------------------------------------------

persons %>% 
  distinct(beruf) %>% 
  separate_rows(beruf) %>% 
  mutate(beruf=trimws(beruf)) %>% 
  clipr::write_clip()

rrefine::refine_export("kz-beruf")


# Prepare for reconciliation ----------------------------------------------

persons %>% 
  distinct(id, firstname = vorname, lastname = nachname, place_of_birth = geburtsort) %>%
  left_join(birthdates, by = "id") %>% 
  left_join(deathdates, by = "id") %>% View
  




persons %>% 
  left_join(birthdates, by = "id") %>% 
  left_join(deathdates, by = "id") %>% 
  left_join(religion, by = "id")



# kz_persons <- readxl::read_excel("data-gathering/data/input/Personenliste Verfolgung Wohnort München.xls") %>% 
#   janitor::clean_names() %>% 
#   rowid_to_column(var = "id")
# 
# 
# 
# kz_persons %>% 
#   distinct(id, wohnort_ort_strasse_von_bis) %>% 
#   separate(wohnort_ort_strasse_von_bis, into = c("address", "date"), sep = ";") %>% 
#   mutate(city = str_extract(address, ".*,"),
#          street = str_remove(address, city)) %>% View
#   
#   