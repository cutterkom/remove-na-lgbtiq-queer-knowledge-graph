# title: Write person entities in Factgrid
# desc: Data modeling for different kind of data, e.g. gender, forum id and others. If decriptions, gender or sexual orientations is avaible from wikidata, I fetch it from there.
# input: 
# output: 


library(tidyverse)
library(kabrutils)
library(testdat)
library(tidywikidatar)

# Some config ------------------------------------------------------------

config_file <- "data-publishing/factgrid/config.yml"
config <- yaml::read_yaml(config_file)
statements <- read_sheet(config$statements$gs_table, sheet = "statements")

manual_sex_or_gender <- read_sheet("https://docs.google.com/spreadsheets/d/1V1pO2lawpn7j8uQfWSwPYb2F6lvhcUO6pXaj9qvM-cw/edit#gid=0", sheet = "sex_or_gender_manual")

# Enable Caching ----------------------------------------------------------

tw_enable_cache()
tw_set_cache_folder(path = fs::path(fs::path_home_r(), "R", "tw_data"))
tw_set_language(language = "de")
tw_create_cache_folder(ask = FALSE)

# Get input data ----------------------------------------------------------

con <- connect_db("db_clean")

entities_raw <- tbl(con, "entities") %>% 
  select(-contains("_at")) %>% 
  collect()

el_matches <- tbl(con, "el_matches") %>% 
  select(-contains("_at"), -entity_id_type, -entity_id_combination, -entity_id_combination_type, external_id_label) %>% 
  collect()

entities_per_type <- tbl(con, "entities_per_type") %>% 
  select(contains("id")) %>% 
  collect()

chronik <- tbl(con, "chronik") %>% collect()

DBI::dbDisconnect(con); rm(con)


# Add info if book_author,  poster_author or publisher --------------------

persons <- entities_raw %>% 
  filter(person == 1) %>% 
  left_join(entities_per_type, by = "id") %>% 
  distinct() %>% 
  mutate(
    book_author = ifelse(!is.na(book_id), 1, 0),
    poster_author = ifelse(!is.na(poster_id), 1, 0),
    publisher = ifelse(!is.na(publisher_book_id), 1, 0)
  ) %>% 
  distinct(name, id, book_author, poster_author, publisher)

# Create Item ID ----------------------------------------------------------

# if already in Factgrid -> Factgrid QID, otherwise CREATE_id

items_id <- persons %>% 
  left_join(el_matches %>% filter(external_id_type == "factgrid"), by = "id") %>% 
  mutate(
    item = ifelse(!is.na(external_id), external_id, paste0("CREATE_", id))
  ) %>% 
  distinct(id, item)


# Get Wikidata ID ---------------------------------------------------------

wikidata <- persons %>% 
  inner_join(
    el_matches %>% 
      filter(external_id_type == "wikidata", external_id != "no_wikidata_id") %>% 
      mutate(external_id = str_remove(external_id, "http://www.wikidata.org/entity/")), 
    by = "id") %>% 
  distinct(id, name, qid = external_id)


# Get Wikipedia Sitelinks -------------------------------------------------

sitelinks_file <- "data-publishing/factgrid/data/wikipedia_sitelinks.Rds"
if (!file.exists(sitelinks_file)) {
  wikipedia_sitelinks <- wikidata %>% 
    pmap_df(function(...) {
      current <- tibble(...)
      tmp <- tibble(
        id = current$id,
        Sdewiki = tw_get_wikipedia(id = current$qid, language = tidywikidatar::tw_get_language()),
        Senwiki = tw_get_wikipedia(id = current$qid, language = "en"),
        Sfrwiki = tw_get_wikipedia(id = current$qid, language = "fr"),
        Seswiki = tw_get_wikipedia(id = current$qid, language = "es")
      )
    })
  
  saveRDS(wikipedia_sitelinks, file = sitelinks_file)
} else {
  wikipedia_sitelinks <- readRDS(sitelinks_file)
}

# Keep only last part of URL and transform spaces to underscores
wikipedia_sitelinks <- wikipedia_sitelinks %>% 
  mutate(
    Sdewiki = str_extract(Sdewiki, "(?<=\\/wiki\\/).*"),
    Senwiki = str_extract(Senwiki, "(?<=\\/wiki\\/).*"),
    Sfrwiki = str_extract(Sfrwiki, "(?<=\\/wiki\\/).*"),
    Seswiki = str_extract(Seswiki, "(?<=\\/wiki\\/).*"),
    Sdewiki = ifelse(!is.na(Sdewiki), paste0(config$import_helper$single_quote, Sdewiki, config$import_helper$single_quote), Sdewiki),
    Senwiki = ifelse(!is.na(Senwiki), paste0(config$import_helper$single_quote, Senwiki, config$import_helper$single_quote), Senwiki),
    Sfrwiki = ifelse(!is.na(Sfrwiki), paste0(config$import_helper$single_quote, Sfrwiki, config$import_helper$single_quote), Sfrwiki),
    Seswiki = ifelse(!is.na(Seswiki), paste0(config$import_helper$single_quote, Seswiki, config$import_helper$single_quote), Seswiki)
  )


# Get Gender from Wikidata ------------------------------------------------

wikidata_sex_or_gender_file <- "data-publishing/factgrid/data/wikidata_sex_or_gender_raw.Rds"

if (!file.exists(sitelinks_file)) {
  
  wikidata_sex_or_gender_raw <- wikidata %>% 
    pmap_df(function(...) {
      current <- tibble(...)
      tmp <- tibble(
        id = current$id,
        sex_or_gender = tw_get_property(id = current$qid, p = "P21")
      )
    }) 
  saveRDS(wikidata_sex_or_gender_raw, file = wikidata_sex_or_gender_file)
  } else {
    wikidata_sex_or_gender_raw <- readRDS(wikidata_sex_or_gender_file)
  }

wikidata_sex_or_gender <- wikidata_sex_or_gender_raw %>% 
  unnest(sex_or_gender, names_repair = "unique") %>% 
  janitor::clean_names() %>% 
  rename(item = id_2, id = id_1) %>% 
  mutate(
    wikidata_label = tw_get_label(value),
    trans_male_helper = ifelse(value == "Q2449503", "transmale", "cis"),
    trans_female_helper = ifelse(value == "Q1052281", "transfemale", "cis"),
    factgrid = # sex_or_gender_factgrid
      case_when(
      # Sex or Gender in Factgrid https://database.factgrid.de/wiki/Property:P154
        value == "Q6581097" ~ "Q18", # Q18 = male in Factgrid,
        value == "Q6581072" ~ "Q17", # Q17 = female in Factgrid,
        value == "Q189125" ~ "Q399991", # transgender person Q189125
        value == "Q1097630" ~ "Q402120", # intersex Q1097630
        value == "Q48270" ~ "Q402116",# non-binary Q48270
        value == "Q2449503" ~ "Q399991",# transgender male Q2449503
        value == "Q1052281" ~ "Q399991",# transgender female Q1052281
        TRUE ~ "I need a translation")
    ) %>% 
  filter(value != "NA") %>% 
  bind_rows(
    manual_sex_or_gender %>% 
      filter(sex_or_gender %in% c("m", "w", "transfrau")) %>% 
      distinct(id, sex_or_gender) %>% 
      mutate(
        trans_male_helper = ifelse(sex_or_gender == "transmann", "transmale", "cis"),
        trans_female_helper = ifelse(sex_or_gender == "transfrau", "transfemale", "cis"),
        factgrid = 
          case_when(
            sex_or_gender == "m" ~ "Q18",
            sex_or_gender == "w" ~ "Q17",
            sex_or_gender == "transfrau" ~ "Q399991"
          )) %>% 
          distinct(id, trans_male_helper, trans_female_helper, factgrid)
      ) 

# Make sure a trans person as both labels: male/female and trans
wikidata_sex_or_gender_final <- wikidata_sex_or_gender %>% 
  pivot_longer(cols = c("factgrid", "trans_male_helper", "trans_female_helper"), names_to = "source", values_to = "new_value") %>% 
  filter(new_value != "cis") %>% 
  mutate(
    new_value =
      case_when(
        new_value == "transfemale" ~ "Q17",
        new_value == "transmale" ~ "Q18",
        TRUE ~ new_value
      )
  ) %>% 
  distinct(id, item, factgrid = new_value)

testthat::test_that(
  desc = "all gender of sex labels from wikidata are translated to Factgrid",
  expect_no_match(wikidata_sex_or_gender$factgrid, "I need a translation")
)

testthat::test_that(
  desc = "if trans male or trans female, the (fe)male as well as trans item",
  # book_author_1469
  
  # book_author_314 wo es nicht passt, Mann fehlt noch
  expect_equal(wikidata_sex_or_gender_final %>% 
                filter(id == "book_author_314") %>% 
                nrow(),
              2)
)


# Get sexual orientation from Wikidata ------------------------------------

wikidata_sex_orientation_file <- "data-publishing/factgrid/data/wikidata_sex_orientation_raw.Rds"

if (!file.exists(wikidata_sex_orientation_file)) {
  
  wikidata_sex_orientation_raw <- wikidata %>% 
    pmap_df(function(...) {
      current <- tibble(...)
      tmp <- tibble(
        id = current$id,
        sex_or_gender = tw_get_property(id = current$qid, p = "P91")
      )
    })
  
  saveRDS(wikidata_sex_orientation_raw, file = wikidata_sex_orientation_file)
} else {
  wikidata_sex_orientation_raw <- readRDS(wikidata_sex_orientation_file)
}

wikidata_sex_orientation <- wikidata_sex_orientation_raw %>% 
  unnest(sex_or_gender, names_repair = "unique") %>% 
  janitor::clean_names() %>% 
  rename(item = id_2, id = id_1) %>% 
  mutate(
    factgrid = 
      # Sex or Gender in Factgrid https://database.factgrid.de/wiki/Property:P154
      case_when(
        value == "Q6649" ~ "Q399990", # lesbian
        value == "Q43200" ~ "Q400014", # bisexuality
        value == "Q592" ~ "Q399989", # Schwul 
        # homosexuality, non-heterosexuality will be under Factgrid LGBT 
        # https://database.factgrid.de/wiki/Item:Q399988
        value == "Q339014" ~ "Q399988", # non-heterosexuality
        value == "Q6636"  ~ "Q399988", # homosexuality
        TRUE ~ "I need a translation")
  ) %>% 
  filter(value != "NA")

testthat::test_that(
  desc = "all labels from wikidata are translated to Factgrid",
  expect_no_match(wikidata_sex_orientation$factgrid, "I need a translation")
)


# GND link as external identifier -----------------------------------------

# All external IDs can be found here: https://database.factgrid.de/query/#SELECT%20%3FPropertyLabel%20%3FProperty%20%3FPropertyDescription%20%3Fexample%20%3Fwd%20WHERE%20%7B%0A%20%20SERVICE%20wikibase%3Alabel%20%7B%20bd%3AserviceParam%20wikibase%3Alanguage%20%22en%22.%20%7D%0A%20%20%3FProperty%20wdt%3AP8%20wd%3AQ100150.%0A%20%20OPTIONAL%20%7B%20%3FProperty%20wdt%3AP364%20%3Fexample.%20%7D%0A%20%20OPTIONAL%20%7B%20%3FProperty%20wdt%3AP343%20%3Fwd.%20%7D%0A%7D%0AORDER%20BY%20%28%3FPropertyLabel%29


gnd_ids <- el_matches %>% 
  filter(external_id_type == "gnd") %>% 
  distinct(id, external_id)


# Add description ---------------------------------------------------------

# If there is one from Wikidata, I take that. 
# If not, I build a simple string from available information.

# Get Wikidata Descriptions -----------------------------------------------

wikidata_descriptions_file <- "data-publishing/factgrid/data/wikidata_descriptions.Rds"

if (!file.exists(wikidata_descriptions_file)) {
  
  wikidata_descriptions <- wikidata %>% 
    pmap_df(function(...) {
      current <- tibble(...)
      tmp <- tibble(
        id = current$id,
        Dde = tw_get_description(id = current$qid, language = tidywikidatar::tw_get_language()),
        Den = tw_get_description(id = current$qid, language = "en"),
        Des = tw_get_description(id = current$qid, language = "es"),
        Dfr = tw_get_description(id = current$qid, language = "fr")
      )
    })
  

# Translate labels --------------------------------------------------------

# automated translation via deepl API
  meta_desc <- wikidata_descriptions %>% 
    pivot_longer(cols = starts_with("D")) %>% 
    mutate(nchar = nchar(value)) %>% 
    group_by(id) %>% 
    # find the longest string -> will be input for translation
    mutate(max_nchar = ifelse(nchar == max(nchar, na.rm = TRUE), 1, 0)) %>% 
    select(id, name, contains("nchar"))
  
  wikidata_descriptions <- wikidata_descriptions %>% 
    mutate(needs_translation = ifelse(rowSums(is.na(wikidata_descriptions)) > 0, TRUE, FALSE),
           no_trans_possible = ifelse(rowSums(is.na(wikidata_descriptions)) == 4, TRUE, FALSE))
  
  wikidata_descriptions_incl_translations <- wikidata_descriptions %>% 
    filter(needs_translation == TRUE, no_trans_possible == FALSE) %>% View
    select(-contains("trans")) %>% 
    pmap_df(function(...) {
      current <- tibble(...)
      current_meta <- meta_desc %>% filter(id == current$id)
      
      # get the one with the longest string, will be input for translation;
      # include also a slice if there there are multiple string with same length
      translate_from <- current_meta %>% filter(max_nchar == 1) %>% slice(1) %>% pull(name)
      
      # get the languages/cols that need a translation
      # can be found in meta df: dont have an nchar
      translate_to <- current_meta %>% filter(is.na(nchar)) %>% pull(name)
      
      map2_df(translate_from, translate_to, function(from, to) {
        
        # get text to translate
        text <- pull(current, {{from}})
        
        # translate target_lang to deepl abbr
        to_deepl <- replace(to, to == "Dde", "DE")
        to_deepl <- replace(to_deepl, to_deepl == "Den", "EN")
        to_deepl <- replace(to_deepl, to_deepl == "Dfr", "FR")
        to_deepl <- replace(to_deepl, to_deepl == "Des", "ES")
        
        # call Deepl API
        current %>%
          mutate({{to}}:= deeplr::translate2(text = text, target_lang = to_deepl, auth_key = Sys.getenv("DEEPL_API_KEY")))
      })
    })
  
  wikidata_descriptions_incl_translations <- wikidata_descriptions_incl_translations %>% 
    pivot_longer(cols = 2:last_col()) %>% 
    filter(!is.na(value)) %>% distinct() %>% 
    pivot_wider(id_col = "id", names_from = "name", values_from = "value") %>% 
    mutate(across(starts_with("D"), ~ as.character(.)))
  
  wikidata_descriptions_final <-
    bind_rows(
      wikidata_descriptions %>% 
        filter(needs_translation == FALSE),
      wikidata_descriptions %>% 
        filter(no_trans_possible == TRUE),
      wikidata_descriptions_incl_translations
    )
  
  saveRDS(wikidata_descriptions_final, file = wikidata_descriptions_file)
} else {
  wikidata_descriptions_final <- readRDS(wikidata_descriptions_file)
}


# Build Factgrid descriptions ---------------------------------------------

vocab <- list(
  de = list(
    poster_author = "Plakatgestalter*in",
    book_author = "Autor*in",
    publisher = "Verleger*in",
    person_in_lgbt_history = "Person im Zusammenhang mit LGBT-Geschichte"
  ),
  en = list(
    poster_author = "poster publisher",
    book_author = "author",
    publisher = "publisher",
    person_in_lgbt_history = "Person related to LGBT history"
  ),
  fr = list(
    poster_author = "créateur d'affiches",
    book_author = "auteur",
    publisher = "éditeur",
    person_in_lgbt_history = "Personne en rapport avec l'histoire LGBT"
  ),
  es = list(
    poster_author = "editor de pósteres",
    book_author = "autor",
    publisher = "editor",
    person_in_lgbt_history = "La persona en el contexto de la historia LGBT"
  )
)


# Add descriptions for 4 languages ----------------------------------------

descriptions <- persons %>% 
  left_join(wikidata_descriptions, by = "id") %>% #filter(is.na(Dde))
  mutate(
    Dde = 
      case_when(
        is.na(Dde) & book_author == 1 & poster_author == 1 & publisher == 1 ~ glue::glue("{vocab$de$book_author}, {vocab$de$publisher} und {vocab$de$poster_author}"),
        is.na(Dde) & book_author == 1 & poster_author == 1 & publisher == 0 ~ glue::glue("{vocab$de$book_author} und {vocab$de$poster_author}"),
        is.na(Dde) & book_author == 1 & publisher == 1 & poster_author ==  0 ~  glue::glue("{vocab$de$book_author} und {vocab$de$publisher}"),
        is.na(Dde) & book_author == 0 & publisher == 1 & poster_author ==  1 ~  glue::glue("{vocab$de$publisher} und {vocab$de$poster_author}"),
        is.na(Dde) & book_author == 0 & publisher == 0 & poster_author ==  1 ~  glue::glue("{vocab$de$poster_author}"),
        is.na(Dde) & book_author == 0 & publisher == 1 & poster_author ==  0 ~  glue::glue("{vocab$de$publisher}"),
        is.na(Dde) & book_author == 1 & publisher == 0 & poster_author ==  0 ~  glue::glue("{vocab$de$book_author}"),
        !is.na(Dde) ~ Dde,
        TRUE ~  glue::glue("{vocab$de$person_in_lgbt_history}")
        ),
    Den = 
      case_when(
        is.na(Den) & book_author == 1 & poster_author == 1 & publisher == 1 ~ glue::glue("{vocab$en$book_author}, {vocab$en$publisher} und {vocab$en$poster_author}"),
        is.na(Den) & book_author == 1 & poster_author == 1 & publisher == 0 ~ glue::glue("{vocab$en$book_author} und {vocab$en$poster_author}"),
        is.na(Den) & book_author == 1 & publisher == 1 & poster_author ==  0 ~  glue::glue("{vocab$en$book_author} und {vocab$en$publisher}"),
        is.na(Den) & book_author == 0 & publisher == 1 & poster_author ==  1 ~  glue::glue("{vocab$en$publisher} und {vocab$en$poster_author}"),
        is.na(Den) & book_author == 0 & publisher == 0 & poster_author ==  1 ~  glue::glue("{vocab$en$poster_author}"),
        is.na(Den) & book_author == 0 & publisher == 1 & poster_author ==  0 ~  glue::glue("{vocab$en$publisher}"),
        is.na(Den) & book_author == 1 & publisher == 0 & poster_author ==  0 ~  glue::glue("{vocab$en$book_author}"),
        !is.na(Den) ~ Den,
        TRUE ~  glue::glue("{vocab$en$person_in_lgbt_history}")
      ),
    Dfr = 
      case_when(
        is.na(Dfr) & book_author == 1 & poster_author == 1 & publisher == 1 ~ glue::glue("{vocab$fr$book_author}, {vocab$fr$publisher} und {vocab$fr$poster_author}"),
        is.na(Dfr) & book_author == 1 & poster_author == 1 & publisher == 0 ~ glue::glue("{vocab$fr$book_author} und {vocab$fr$poster_author}"),
        is.na(Dfr) & book_author == 1 & publisher == 1 & poster_author ==  0 ~  glue::glue("{vocab$fr$book_author} und {vocab$fr$publisher}"),
        is.na(Dfr) & book_author == 0 & publisher == 1 & poster_author ==  1 ~  glue::glue("{vocab$fr$publisher} und {vocab$fr$poster_author}"),
        is.na(Dfr) & book_author == 0 & publisher == 0 & poster_author ==  1 ~  glue::glue("{vocab$fr$poster_author}"),
        is.na(Dfr) & book_author == 0 & publisher == 1 & poster_author ==  0 ~  glue::glue("{vocab$fr$publisher}"),
        is.na(Dfr) & book_author == 1 & publisher == 0 & poster_author ==  0 ~  glue::glue("{vocab$fr$book_author}"),
        !is.na(Dfr) ~ Dfr,
        TRUE ~  glue::glue("{vocab$fr$person_in_lgbt_history}")
      ),
    Des = 
      case_when(
        is.na(Des) & book_author == 1 & poster_author == 1 & publisher == 1 ~ glue::glue("{vocab$es$book_author}, {vocab$es$publisher} und {vocab$es$poster_author}"),
        is.na(Des) & book_author == 1 & poster_author == 1 & publisher == 0 ~ glue::glue("{vocab$es$book_author} und {vocab$es$poster_author}"),
        is.na(Des) & book_author == 1 & publisher == 1 & poster_author ==  0 ~  glue::glue("{vocab$es$book_author} und {vocab$es$publisher}"),
        is.na(Des) & book_author == 0 & publisher == 1 & poster_author ==  1 ~  glue::glue("{vocab$es$publisher} und {vocab$es$poster_author}"),
        is.na(Des) & book_author == 0 & publisher == 0 & poster_author ==  1 ~  glue::glue("{vocab$es$poster_author}"),
        is.na(Des) & book_author == 0 & publisher == 1 & poster_author ==  0 ~  glue::glue("{vocab$es$publisher}"),
        is.na(Des) & book_author == 1 & publisher == 0 & poster_author ==  0 ~  glue::glue("{vocab$es$book_author}"),
        !is.na(Des) ~ Des,
        TRUE ~  glue::glue("{vocab$es$person_in_lgbt_history}")
      )
    )



# Statements --------------------------------------------------------------

person_statements <- 
  bind_rows(
    persons %>% filter(book_author == 1) %>% add_statement(statements, "career_is_author"),
    persons %>% filter(publisher == 1) %>% add_statement(statements, "career_is_publisher"),
    # add poster authors
    persons %>% filter(poster_author == 1) %>% add_statement(statements, "career_is_poster_creator"),
    
    # die, die kein career_statement haben: stammen aus Chronik
    persons %>% filter(book_author == 0, publisher == 0, poster_author == 0)
  ) %>% 
  # Human
  add_statement(statements, "instance_of_human") %>% 
  select(id, name, P2, everything()) %>% 
  right_join(items_id, by = "id") %>% 
  # Add Wikipedia Sitelinks in four languages
  left_join(wikipedia_sitelinks, by = "id") %>% 
  # Add Wikidata QIDs
  left_join(wikidata %>% distinct(id, Swikidatawiki = qid), by = "id") %>% 
  # quote links
  mutate(Swikidatawiki = 
           case_when(
             !is.na(Swikidatawiki) ~ paste0(config$import_helper$single_quote, Swikidatawiki, config$import_helper$single_quote),
             TRUE ~ Swikidatawiki)) %>% 
  # GND ID
  left_join(gnd_ids %>% distinct(id, P622 = external_id), by = "id") %>% 
  # Forum ID
  add_statement(statements, "external_id_forum", qid_from_row = TRUE, col_for_row_content = "id") %>% 
  # descriptions: If person already in Factgrid, then don't add an new description
  left_join(descriptions %>% distinct(id, Dde, Den, Dfr, Des), by = "id") %>% 
  mutate(
    Dde = ifelse(str_detect(item, "CREATE"), Dde, NA_character_),
    Den = ifelse(str_detect(item, "CREATE"), Den, NA_character_),
    Dfr = ifelse(str_detect(item, "CREATE"), Dfr, NA_character_),
    Des = ifelse(str_detect(item, "CREATE"), Des, NA_character_),
    ) %>% 
  # Labels only if new item
  mutate(
    Lde = ifelse(str_detect(item, "CREATE"), name, NA_character_),
    Len = ifelse(str_detect(item, "CREATE"), name, NA_character_),
    Lfr = ifelse(str_detect(item, "CREATE"), name, NA_character_),
    Les = ifelse(str_detect(item, "CREATE"), name, NA_character_),
  ) %>% 
  # SEX or GENDER
  left_join(wikidata_sex_or_gender_final %>% select(id, P154 = factgrid), by = "id") %>% 
  # this is risky: set gender automatically
  # Implemented in Factgrid https://database.factgrid.de/wiki/Property:P625
  # add_statement(statements, "set_automated_gender") %>% 
  # Statements for Research project/area Remove NA
  
  # Sexual orientation (from Wikidata)
  left_join(wikidata_sex_orientation %>% select(id, P711 = factgrid), by = "id") %>% 
  add_statement(statements, "research_area") %>% 
  add_statement(statements, "research_project") 
  

# Naming ------------------------------------------------------------------

# decision: No automated last name detection. I have function for this. But then I read that text and it was überzeugend: No, I dont want to do that in an automated way. 

# Transform for Quickstatements Input -------------------------------------

import <- person_statements %>% 
  select(item, 
         matches("^L", ignore.case = FALSE), 
         matches("^D", ignore.case = FALSE), 
         matches("^S", ignore.case = FALSE), 
         matches("^P", ignore.case = FALSE)) %>% 
  distinct() %>% 
  
  # move columns: determine the order on an item page
  # I want instance of, gender, sex. orientation at the beginning, 
  # external IDs at the end
  # rest in between
  relocate(P154, .after = P2) %>% 
  relocate(P711, .after = P154) %>% 
  relocate(P165, .after = P711) %>% 
  relocate(P622, .after = last_col()) %>% 
  relocate(P728, .after = P622) %>% 
  
  # create a group variable for every 20 rows
  # to upload data in bulks of 20 items
  # https://stackoverflow.com/questions/32078578/how-to-group-by-every-7-rows-and-aggregate-those-7-values-by-median
  mutate(group = c(0, rep(1:(nrow(.)-1)%/%20)), .before = "item")

import_long <- import %>% 
  pivot_longer(cols = 3:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*")) %>% 
  filter(!is.na(value))

import_list <- import_long %>% group_split(group, .keep = TRUE)

# Tests -------------------------------------------------------------------

testthat::test_that(
  desc = "John Rechy is just a book author",
  expect_equal(import_long %>% 
                 filter(item == "CREATE_book_author_1", property == "P165") %>% 
                 pull(value),
               statements %>% filter(statement == "career_is_author") %>% pull(qid)
  )
)

testthat::test_that(
  desc = "if item already exists in Factgrid, then no Description or Labels are added",
  expect_equal(
    import_long %>% filter(!str_detect(item, "CREATE"), 
                           property %in% c("Dde", "Den", "Des", "Dfr", "Lde", "Len", "Lfr", "Les")) %>% 
      nrow(),
    0
  )
)


# Write to factgrid -------------------------------------------------------

# test case 
# CREATE_book_author_1
# CREATE_book_author_10
# CREATE_book_author_1013
# Q383677
# CREATE_book_author_1013
# CREATE_book_author_1006
# CREATE_book_author_1019

test_long <- import_long %>% filter(item %in% c("CREATE_book_author_1006", "CREATE_book_author_1019"))
test_long
# test_import_list <- test_long %>% group_split(group, .keep = TRUE)

write_wikibase(
  items = test_long$item,
  properties = test_long$property,
  values = test_long$value,
  format = "csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_persons",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


# Test cases --------------------------------------------------------------

# automated gender


# Manual wiki stuff 
# - book_author_1312: GlaxoSmithKline
# - https://de.wikipedia.org/wiki/GlaxoSmithKline


# Geschlecht checken: Cosy Piero

# Pierre et Gilles Gruppe, keine person https://de.wikipedia.org/wiki/Pierre_et_Gilles


# book_author_885 fictional human
