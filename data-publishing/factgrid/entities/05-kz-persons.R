library(tidyverse)
library(kabrutils)
library(googlesheets4)
library(tidygeocoder)


# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- googlesheets4::read_sheet(config$statements$gs_table, sheet = "statements")

persons <- read_sheet("https://docs.google.com/spreadsheets/d/14wtFqTTJ9n2hpuZIj6I3lVXH5xG4jlr3tr1vkQsJuSU/edit#gid=411652859", sheet = "KZ Häftlinge") %>% 
  janitor::clean_names() %>% 
  rowid_to_column(var = "id")


# names -------------------------------------------------------------------

labels_desc <- persons %>% 
  distinct(id, vorname, nachname) %>% 
  mutate(
    Lde = paste(vorname, nachname),
    Len = Lde,
    Lfr = Lde, 
    Les = Lde,
  ) %>% 
  rename(P247 = nachname, P248 = vorname) %>% 
  mutate(
    Dde = "Homosexuelles NS-Opfer",
    Den = "Homosexual Nazi victim",
    Des = "Víctima nazi homosexual",
    Dfr = "Victime homosexuelle du nazisme"
  )
  
labels_desc %>% select(id, Lde) %>% clipr::write_clip()
labels_desc %>% select(id, P248) %>% 
  separate_rows(P248, sep = " ") %>% 
  clipr::write_clip()

labels_desc %>% select(id, P247) %>% 
  clipr::write_clip()

factgrid_vorname <- rrefine::refine_export("kz-vorname") %>% select(id, P248 = factgrid)
factgrid_nachname <- rrefine::refine_export("kz-nachname") %>% select(id, P247 = factgrid)

labels_desc <- labels_desc %>% 
  select(-P247, -P248) %>% 
  left_join(factgrid_vorname, by = "id") %>% 
  left_join(factgrid_nachname, by = "id")

# Birthdates ---------------------------------------------------------------

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
  distinct(id, P77 = birthdate, birthdate_raw = birthdate) %>% 
  mutate(P77 = paste0("+", P77, "T00:00:00Z/", 11))


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
  distinct(id, P38 = deathdate, deathdate_raw = deathdate) %>% 
  mutate(P38 = paste0("+", P38, "T00:00:00Z/", 11))

# Birthplace --------------------------------------------------------------

persons %>% 
  distinct(geburtsort) %>% clipr::write_clip()

birthplaces_factgrid <- rrefine::refine_export("kz-birthplaces") %>% 
  rename(geburtsort = `Column 1`) %>% 
  filter(geburtsort != "geburtsort")

birthplaces <- persons %>% 
  distinct(id, geburtsort) %>% 
  left_join(birthplaces_factgrid, by = "geburtsort") %>%
  mutate(
    factgrid = 
      case_when(
        geburtsort == "Prien" ~ "Q90382",
        geburtsort == "Schwaberwegen/Forstinning" ~ "Q289803",
        geburtsort == "Landsberg / Lech" ~ "Q10405",
        geburtsort == "Frankfurt/Oder" ~ "Q22562",
        geburtsort == "Durlach/Baden" ~ "Q310940",
        geburtsort == "Obersohland/Löbau" ~ "Q88158",
        geburtsort == "Immenstadt" ~ "Q86746",
        geburtsort == "Hochmutting/Oberschleißheim" ~ "Q297892",
        geburtsort == "Annweiler" ~ "Q82329",
        geburtsort == "Weilheim/Obb." ~ "Q93311",
        geburtsort == "Pfaffenhofen/Ilm" ~ "Q90157",
        geburtsort == "Kaars bei Neuß" ~ "Q86945",
        geburtsort == "Freisen bei St. Wendel" ~ "Q85036",
        geburtsort == "Hamm/Westf." ~ "Q22037",
        geburtsort == "Dörflas/Marktredwitz" ~ "Q295677",
        TRUE ~ factgrid
      )
  ) %>% 
  rename(P82 = factgrid, geburtsort_raw = geburtsort)
  
birthplaces_for_desc <- birthplaces %>% 
  filter(!is.na(P82)) %>% 
  right_join(birthplaces_factgrid, c("P82" = "factgrid")) %>% 
  filter(!is.na(P82))

# citizenship -------------------------------------------------------------

citizenship <- persons %>% 
  distinct(id, staatsangehorigkeit) %>% 
  mutate(
    P616 = 
      case_when(
        staatsangehorigkeit == "deutsch" ~ "Q140530",
        staatsangehorigkeit == "tschechisch" ~ "Q256356", # alt:"Q418268",
        staatsangehorigkeit == "österreichisch" ~ "Q140532", # alt: "Q418269",
        TRUE ~ NA_character_
      )
  ) %>% 
  select(-staatsangehorigkeit)


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
  ) %>% 
  select(-religion)


# Place of death ----------------------------------------------------------

persons %>% 
  distinct(id, gestorben_in) %>% filter(!is.na(gestorben_in)) %>% clipr::write_clip()

factgrid_place_death <- rrefine::refine_export("kz-häftlinge-place-of-death")

place_of_death <- persons %>% 
  distinct(id, geburtsort) %>% 
  left_join(factgrid_place_death, by = "id") %>% 
  # Killed in a National Socialist concentration camp
  mutate(P550 = ifelse(str_detect(gestorben_in, "KZ|Konzentrationslager|NS"), "Q220577", NA)) %>% 
  select(id, P168 = factgrid, P550, death_place_raw = gestorben_in)

# beruf --------------------------------------------------------------

persons %>% 
  distinct(id, beruf) %>% 
  filter(!is.na(beruf)) %>% 
  separate_rows(beruf, sep = "und|,|;") %>% 
  mutate(beruf=trimws(beruf)) %>% 
  clipr::write_clip()

factgrid_beruf <- rrefine::refine_export("kz-beruf")

berufe <- persons %>% 
  distinct(id, beruf) %>% 
  left_join(factgrid_beruf, by = "id") %>% 
  select(id, P165 = factgrid, beruf_raw = beruf.x)



# Address -----------------------------------------------------------------

address_input <- persons %>%
  distinct(id, addresse) %>%
  separate_rows(addresse, sep = ";") %>%
  mutate(addresse = trimws(addresse),
         addresse = str_replace(addresse, "str.", "straße"),
         addresse = str_replace(addresse, "Str.", "Straße"),
         city =
           case_when(
            addresse == "München" ~ "München",
            TRUE ~ str_extract(addresse, ".*,")
             ),
         city = trimws(str_remove(city, ",")),
         street = str_extract(addresse, ",.*"),
         street = trimws(str_remove(street, ","))
  ) %>%
  geocode(street = street, city = city, method = "osm", lat = latitude, long = longitude)

# address_input %>% 
#   filter(!is.na(street)) %>% 
#   select(id, addresse) %>% clipr::write_clip()
# 
# address_statements <- address_input %>% 
#   filter(!is.na(street)) %>% 
#   mutate(item = paste0("CREATE_", id)) %>% 
#   #select(id, addresse, street) %>% 
#   # already in factgrid
#   filter(!id %in% c(116, 65, 46)) %>% 
#   mutate(
#     Lde = addresse,
#     Len = Lde,
#     Les = Lde, 
#     Lfr = Lde,
#     Dde = "Straße in München",
#     Den = "Street in Munich",
#     Des = "Calle en Múnich",
#     Dfr = "Rue en Munich"
#   ) %>% 
#   add_statement(statements, "instance_of_address") %>% 
#   add_statement(statements, "address_as_string", col_for_row_content = "street", qid_from_row = T) %>% 
#   add_statement(statements, "location_in_munich") %>% 
#   add_statement(statements, "coordinates", qid_from_row = TRUE) %>% 
#   mutate(P48 = case_when(
#     !is.na(P48) ~ paste0('"""', P48, '"""'),
#     TRUE ~ P48
#   )) %>% 
#   add_statement(statements, "research_project") %>% 
#   add_statement(statements, "research_area") 
# 
# import <- address_statements %>% 
#   select(item, 
#          matches("^L", ignore.case = FALSE), 
#          matches("^D", ignore.case = FALSE), 
#          matches("^S", ignore.case = FALSE), 
#          matches("^P", ignore.case = FALSE)) %>% #clipr::write_clip()
#   long_for_quickstatements()
# 
# 
# # Import ------------------------------------------------------------------
# 
# WikidataR::write_wikibase(
#   items = import$item,
#   properties = import$property,
#   values = import$value,
#   format = "csv",
#   format.csv.file = "data-publishing/factgrid/data/kz-addresses.csv",
#   api.username = config$connection$api_username,
#   api.token = config$connection$api_token,
#   api.format = "v1",
#   api.batchname = "entities_clubs",
#   api.submit = F,
#   quickstatements.url = config$connection$quickstatements_url
# )



factgrid_addresses <- rrefine::refine_export("kz-adressen")

address_per_person <- persons %>% 
  distinct(id, addresse) %>% 
  left_join(factgrid_addresses, by = "id") %>% 
  filter(!is.na(factgrid)) %>% 
  select(id, P208 = factgrid)


city_per_person <- persons %>% 
  distinct(id, addresse) %>% 
  left_join(address_input %>% select(id, city), by = "id") %>% 
  mutate(P83 = 
           case_when(
             city == "München-Pasing" ~ "Q311578",
             city == "München" ~ "Q10427",
             city == "Unterhaching" ~ "Q92774",
             city == "Augsburg" ~ "Q10305",
           )) %>% 
  select(id, P83)
  
  

# Haft --------------------------------------------------------------------

haft_long <- persons %>% 
  distinct(id, haft_ort_von_bis_art) %>% 
  separate_rows(haft_ort_von_bis_art, sep = ";") %>% 
  mutate(haft_ort_von_bis_art = trimws(haft_ort_von_bis_art)) %>% 
  distinct(id, haft = haft_ort_von_bis_art)

haft_count_per_person <- haft_long %>% count(id) %>% filter(n>3)


kzs_gefaengnisse <- persons %>% 
  distinct(id, txt = haft_ort_von_bis_art) %>% 
  mutate(
        dachau = ifelse(str_detect(txt, "Dachau"), 1, 0),
        mauthausen = ifelse(str_detect(txt, "Mauthausen"), 1, 0),
        buchenwald = ifelse(str_detect(txt, "Buchenwald"), 1, 0),
        flossenbürg = ifelse(str_detect(txt, "Flossenbürg"), 1, 0),
        auschwitz = ifelse(str_detect(txt, "Auschwitz"), 1, 0),
        Sachsenhausen = ifelse(str_detect(txt, "Sachsenhausen"), 1, 0),
        Neuengamme = ifelse(str_detect(txt, "Neuengamme"), 1, 0),
        Stutthof = ifelse(str_detect(txt, "Stutthof"), 1, 0),
        Ravensbrück = ifelse(str_detect(txt, "Ravensbrück"), 1, 0),
        Natzweiler = ifelse(str_detect(txt, "Natzweiler"), 1, 0),
        Majdanek = ifelse(str_detect(txt, "Majdanek"), 1, 0),
        Lichtenburg = ifelse(str_detect(txt, "Lichtenburg"), 1, 0),
        Gefängnis_Landsberg = ifelse(str_detect(txt, "Landsberg"), 1, 0),
        Kaisheim = ifelse(str_detect(txt, "Kaisheim"), 1, 0),
        Esterwegen = ifelse(str_detect(txt, "Esterwegen"), 1, 0),
        Börgermoor = ifelse(str_detect(txt, "Börgermoor"), 1, 0),
        XIV_Bathorn = ifelse(str_detect(txt, "XIV Bathorn"), 1, 0),
        Lingen = ifelse(str_detect(txt, "Lingen"), 1, 0),
        Emsland = ifelse(str_detect(txt, "Emsland"), 1, 0),
        Stadelheim = ifelse(str_detect(txt, "Stadelheim"), 1, 0),
        Leipzig = ifelse(str_detect(txt, "Leipzig"), 1, 0),
        Hamm = ifelse(str_detect(txt, "Hamm"), 1, 0),
        Nürnberg = ifelse(str_detect(txt, "Nürnberg"), 1, 0),
        Neustadt = ifelse(str_detect(txt, "Neustadt"), 1, 0),
        Torgau = ifelse(str_detect(txt, "Torgau"), 1, 0),
        Germersheim = ifelse(str_detect(txt, "Germersheim"), 1, 0),
        Landau = ifelse(str_detect(txt, "Landau"), 1, 0),
        Kislau = ifelse(str_detect(txt, "Kislau"), 1, 0),
        Amberg = ifelse(str_detect(txt, "Amberg"), 1, 0)#,
        #AG_München  = ifelse(str_detect(txt, "AG München "), 1, 0)
        )
        
## Stadelheim
# Gefängnis Landsberg/L. 
# Zuchthaus Kaisheim
#  
# Lager XIV Bathorn
# Gef. Lingen 
# Gefängnis Leipzig -> nur nach Leipzig suchen
# Gefängnis Hamm -> nur nach Hamm suchen
# Gefängnis Nürnberg
# Gefängnis Neustadt a.d. Aisch
# Mil.gef. Torgau 
# Mil.gef. Germersheim
# Bez.gef. Landau/Pf. 
# Strafgef. Kislau
# Zuchthaus Amberg 
# AG München -> Amtsgericht?
# Straflager Esterwegen
# Straflager Börgermoor


kzs_gefaengnisse %>% 
  select(-txt) %>% 
  pivot_longer(cols = 2:last_col()) %>% 
  distinct(name) %>% 
  mutate(name_for_rec = name) %>% clipr::write_clip()
 

factgrid_kzs_gefaengnisse <- rrefine::refine_export("kz-gefängnisse") %>% 
  filter(!is.na(factgrid_ids) | !is.na(factgrid_new)) %>% 
  mutate(factgrid = ifelse(!is.na(factgrid_ids), factgrid_ids, factgrid_new)) %>% 
  select(name, haftort = factgrid) %>% 
  right_join(kzs_gefaengnisse %>% 
               select(-txt) %>% 
               pivot_longer(cols = 2:last_col()), by = "name") %>% 
  filter(value == 1) %>% 
  mutate(haftort = ifelse(name == "Stadelheim", "Q405721", haftort)) %>% 
  mutate(haftort = ifelse(name == "Stadelheim", "Q405721", haftort)) %>% 
  filter(!is.na(haftort)) %>% 
  select(id, P216 = haftort)


rrefine::refine_export("kz-gefängnisse") %>% 
  filter(is.na(factgrid_ids)) %>% clipr::write_clip()



# Type of haft ------------------------------------------------------------



types <- persons %>% 
  distinct(id, txt = haft_ort_von_bis_art) %>% 
  mutate(
    schutzhaft = ifelse(str_detect(txt, "Schutzhaft|Schtuzhaft|Schutzhäftling"), 1, 0),
    sicherungsverwahrung = ifelse(str_detect(txt, "Sicherungsverwahrung"), 1, 0),
    uhaft = ifelse(str_detect(txt, "Untersuchungshaft|U-Haft"), 1, 0),
    gefängnis = ifelse(str_detect(txt, "Gefängnis"), 1, 0),
    strafhaft = ifelse(str_detect(txt, "Strafhaft"), 1, 0)
  )



# Quellen -----------------------------------------------------------------

quellen <- persons %>% 
  separate_rows(quellen, sep = ";") %>% 
  mutate(quellen = trimws(quellen)) %>% 
  #count(quellen, sort = T) %>%# View
  mutate(
    quelle_big = 
      case_when(str_detect(quellen, "Staatsarchiv München") ~ "Staatsarchiv München",
                str_detect(quellen, "KZ-Gedenkstätte Dachau") ~ "KZ-Gedenkstätte Dachau",
                str_detect(quellen, "Arolsen") ~ "Arolsen Archives",
                str_detect(quellen, "Stadtarchiv München") ~ "Stadtarchiv München",
                str_detect(quellen, "NS-Dokuzentrum") ~ "NS-Dokuzentrum"
  )
  )
quellen %>% 
  distinct(quelle_big) %>% 
  clipr::write_clip()

quellen_input <- rrefine::refine_export("kz-quellen") %>% 
  filter(!is.na(factgrid)) %>% 
  right_join(quellen, by = c("Column 1" = "quelle_big")) %>% 
  mutate(
    factgrid = ifelse(`Column 1` == "NS-Dokuzentrum", "Q418266", factgrid),
    factgrid = ifelse(str_detect(quellen, "Rainer"), "Q419307", factgrid),
    factgrid = ifelse(str_detect(quellen, "Buchenwald"), "Q195179", factgrid),
    factgrid = ifelse(str_detect(quellen, "Mauthausen"), "Q220826", factgrid),
    S73 = paste0('"', quellen ,'"')) %>% 
  distinct(id, P12 = factgrid, S73) %>% 
  filter(!is.na(P12))

# Better descriptions -----------------------------------------------------
#  * 1906-06-28 Munich, + 1943-06-04 Dachau, Maler, Homosexuelles NS-Opfer
desc_better <- labels_desc %>% 
  left_join(birthdates, by = "id") %>% 
  left_join(deathdates, by = "id") %>% 
  left_join(birthplaces_for_desc %>% distinct(id, geburtsort), by = "id") %>% 
  left_join(place_of_death, by = "id") %>% 
  left_join(berufe %>% distinct(id, beruf_raw), by = "id") %>% 
  mutate(deathdate_raw = as.character(deathdate_raw),
         birthdate_raw = as.character(birthdate_raw)) %>% 
  replace(is.na(.), "") %>% 
  mutate(
    Dde = 
      case_when(
        deathdate_raw == "" ~ paste0("* ", birthdate_raw, " ", geburtsort, ", ", beruf_raw, ", ", Dde),
        TRUE ~ paste0("* ", birthdate_raw, " ", geburtsort, ", + ", deathdate_raw, " ", death_place_raw, ", ", Dde)
        ),
    Dde = str_replace_all(Dde, "  ", " "),
    Dde = str_replace_all(Dde, " , ", ", "),
    Dde = str_replace_all(Dde, ",,", ","),
  ) %>%  
  
  mutate(
    Den = 
      case_when(
        deathdate_raw == "" ~ paste0("* ", birthdate_raw, " ", geburtsort, ", ", beruf_raw, ", ", Den),
        TRUE ~ paste0("* ", birthdate_raw, " ", geburtsort, ", + ", deathdate_raw, " ", death_place_raw, ", ", Den)
      ),
    Den = str_replace_all(Den, "  ", " "),
    Den = str_replace_all(Den, " , ", ", "),
    Den = str_replace_all(Den, ",,", ","),
  ) %>%  
  
  mutate(
    Dfr = 
      case_when(
        deathdate_raw == "" ~ paste0("* ", birthdate_raw, " ", geburtsort, ", ", beruf_raw, ", ", Dfr),
        TRUE ~ paste0("* ", birthdate_raw, " ", geburtsort, ", + ", deathdate_raw, " ", death_place_raw, ", ", Dfr)
      ),
    Dfr = str_replace_all(Dfr, "  ", " "),
    Dfr = str_replace_all(Dfr, " , ", ", "),
    Dfr = str_replace_all(Dfr, ",,", ","),
  ) %>%  
  
  mutate(
    Des = 
      case_when(
        deathdate_raw == "" ~ paste0("* ", birthdate_raw, " ", geburtsort, ", ", beruf_raw, ", ", Des),
        TRUE ~ paste0("* ", birthdate_raw, " ", geburtsort, ", + ", deathdate_raw, " ", death_place_raw, ", ", Des)
      ),
    Des = str_replace_all(Des, "  ", " "),
    Des = str_replace_all(Des, " , ", ", "),
    Des = str_replace_all(Des, ",,", ","),
  ) %>%  

  distinct(id, Dde, Den, Dfr, Des)



# Notiz -------------------------------------------------------------------

notizen <- persons %>% 
  select(id, haft_ort_von_bis_art, strafverfahren_behorde_aktenzeichen_abschlussdatum_ausgang, quellen) %>% 
  replace(is.na(.), "") %>% 
  mutate(P73 = 
           case_when(
             strafverfahren_behorde_aktenzeichen_abschlussdatum_ausgang == "" ~ paste0('"', 'Haft: ', haft_ort_von_bis_art, '; Quellen: ', quellen, '"'
             ),
             TRUE ~ paste0('"', 'Haft: ', haft_ort_von_bis_art, '; Stafverfahren: ', strafverfahren_behorde_aktenzeichen_abschlussdatum_ausgang, '; Quellen: ', quellen, '"'
             )
           )
  )
          
# Statements --------------------------------------------------------------

# haftorte wo und wie? factgrid_kzs_gefaengnisse


input_statements <- persons %>% 
  distinct(id) %>% 
  left_join(labels_desc %>% select(-Dde, -Den, -Dfr, -Des), by = "id") %>% 
  left_join(desc_better, by = "id") %>% 
  mutate(item = 
           case_when(
             Lde == "Max Ursprung" ~ "Q403954",
             Lde == "Rudolf Peters" ~ "Q403965",
             TRUE ~paste0("CREATE_", id)
           )
  ) %>% 
    mutate(
      Dde = ifelse(str_detect(item, "CREATE"), Dde, NA_character_),
      Den = ifelse(str_detect(item, "CREATE"), Den, NA_character_),
      Dfr = ifelse(str_detect(item, "CREATE"), Dfr, NA_character_),
      Des = ifelse(str_detect(item, "CREATE"), Des, NA_character_),
    ) %>% 
    # Labels only if new item
    mutate(
      Lde = ifelse(str_detect(item, "CREATE"), Lde, NA_character_),
      Len = ifelse(str_detect(item, "CREATE"), Len, NA_character_),
      Lfr = ifelse(str_detect(item, "CREATE"), Lfr, NA_character_),
      Les = ifelse(str_detect(item, "CREATE"), Les, NA_character_),
    ) %>% 
  add_statement(statements, "instance_of_human") %>% 
  left_join(birthdates %>% select(-birthdate_raw), by = "id") %>% 
  left_join(birthplaces %>% select(-geburtsort_raw), by = "id") %>% 
  left_join(deathdates %>% select(-deathdate_raw), by = "id") %>% 
  left_join(place_of_death %>% select(-death_place_raw), by = "id") %>% 
  # geschlecht
  mutate(P154 = "Q18") %>% 
  left_join(citizenship, by = "id") %>% 
  left_join(religion, by = "id") %>% 
  # Betoffen von Verfolgung von Homosexuellen im Dritten Reich
  mutate(P550 = "Q409833") %>% 
  # Betroffen von KZ-Internierung
  mutate(P550_2 = "Q220582") %>% 
  # Gefangenschaft in ...
  left_join(berufe %>% select(-beruf_raw), by = "id") %>% 
  left_join(address_per_person, by = "id") %>% 
  left_join(city_per_person, by = "id") %>% 
  left_join(factgrid_kzs_gefaengnisse, by = "id") %>% 
  left_join(quellen_input, by = "id") %>% 
  left_join(notizen, by = "id") %>% 
  add_statement(statements, "research_project") %>% 
  add_statement(statements, "research_area")

# Prepare import ----------------------------------------------------------

sort_helper <- input_statements %>% 
    select(matches("^L", ignore.case = FALSE), 
           matches("^D", ignore.case = FALSE), 
           matches("^S", ignore.case = FALSE),
           # instance of
           P2, 
           #names 
           P248, P247,
           # gender
           P154,
           # birth
           P77, P82,
           # death
           P38, P168, 
           
           P550, P216,
           # religion
           P172,
           # citizenship
           P616, 
           # beruf
           P165,

           # location and address
           P83, P208,
           # quelle, notiz
           P12, P73,
           # meta data forum, research area
           P97, P131) %>% 
    names() %>% 
    enframe() %>% 
    rename(sort = name, property = value)
  

import_test <- input_statements %>% 
  filter(!id %in% c(30, 32)) %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         #matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  long_for_quickstatements() %>% 
  left_join(sort_helper, by = "property") %>%
  arrange(item, sort) %>% 
  select(-sort)


WikidataR::write_wikibase(
  items = import_test$item,
  properties = import_test$property,
  values = import_test$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/kz-test.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_clubs",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


query <- '
SELECT ?fg_item ?fg_itemLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  ?fg_item wdt:P131 wd:Q400012.
  ?fg_item wdt:P550 wd:Q409833.
}
'

query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(across("fg_item", ~extract_id(.))) %>% 
  mutate(across(c(fg_itemLabel), ~remove_lang(.)))


import <- query_res %>% 
  left_join(labels_desc, by = c("fg_itemLabel" = "Lde")) %>% 
  select(fg_item, fg_itemLabel, id) %>% 
  left_join(desc_better, by = "id") %>% 
  distinct() %>% 
  select(item = fg_item, Dde, Den, Des, Dfr) %>% 
  long_for_quickstatements()

csv_file <- tempfile(fileext = "csv")

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = csv_file
)

fs::file_show(csv_file)  
