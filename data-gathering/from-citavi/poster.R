# title: ETL poster data
# desc: extract poster data from citavi db tables, create clean relations, add pk's and fk's and load in DB
# ref: https://scriptsandstatistics.wordpress.com/2017/05/19/how-to-parse-citavi-files-using-r/

library(tidyverse)
library(kabrutils)
library(RSQLite)
library(DBI)
library(dm)
library(testdat)

con_poster <- dbConnect(RSQLite::SQLite(), "data-gathering/data/input/citavi-poster.ctt4")
dbListTables(con_poster)

####
## Leere Tabellen
### ReferenceReference, KnowledgeItemCategory, SeriesTitle, KnowledgeItem, KnowledgeItemKeyword, EntityLink, Library,
### Periodical, Publisher, ReferenceCategory, ReferenceCollaborator, ReferenceEditor, ReferenceOthersInvolved, ReferencePublisher
### TaskItem
## unverständlich: CitaviInternal, 
### citavi übergreifend
## ProjectSettings, DBVersion

### Die Poster liegen in der Reference Tabelle
### Die Schlagworte in der Keyword und über die Tabelle ReferenceKeyword sind sie miteinander verbunden. 
### wer das Poster veröffentlicht hat steht in der Person, verknüpft sind sie über die ReferenceAuthor. 
### Die Kategorien zu den Postern liegen ebenfalls in Person, sind aber verknüpft über die ReferenceOrganization

df_poster <- dbGetQuery(con_poster, "select ID, ShortTitle, Title, Date, CustomField3, CustomField1, OnlineAddress FROM Reference")

### Schlagworte holen

df_keyword <- dbGetQuery(con_poster, "select ID, Name FROM Keyword")
df_link_keyword_poster <- dbGetQuery(con_poster, "select * FROM ReferenceKeyword")

df_link_keyword_poster <- df_link_keyword_poster %>% 
  left_join(., df_keyword, by = c("KeywordID" = "ID")) %>% 
  select(-KeywordID, -Index)


### die Tabelle Person enthält als einzige Information in der Spalte LastName von wem das Poster stammt
df_person <- dbGetQuery(con_poster, "select ID, LastName FROM Person")
df_link_person_poster <- dbGetQuery(con_poster, "select * FROM ReferenceAuthor")

names(df_person)

df_link_person_poster <- df_link_person_poster %>% 
  left_join(., df_person, by = c("PersonID" = "ID")) %>% 
  select(-PersonID, -Index)

df_poster_prep <- df_poster %>% 
  left_join(., df_link_person_poster, by = c("ID" = "ReferenceID")) %>% 
  rename("Origin" = "LastName", "Year" = "Date","Size" = "CustomField3", "NrItems" = "CustomField1") %>% 
  group_by(ID, Title, ShortTitle, Year, Size, NrItems, OnlineAddress) %>% 
  summarise(Origin = paste(Origin, collapse = ",")) %>% 
  left_join(., df_link_keyword_poster, by = c("ID" = "ReferenceID")) %>% 
  group_by(ID, Title, ShortTitle, Year, Size, NrItems, OnlineAddress, Origin) %>% #View()
  summarise(Name = paste(Name, collapse = ",")) %>% 
  rename("Keyword" = "Name") %>% 
  ungroup()

names(df_poster)

# Create Relations --------------------------------------------------------

poster_wide_raw <- df_poster_prep %>%
  as_tibble() %>%
  janitor::clean_names() %>% 
  mutate(
    filename = str_replace(online_address, "\\\\", "/"),
    filename = str_remove(filename, "Bilder/")) %>% 
  select(-online_address) %>% 
  separate_rows(origin, sep = ",") %>%
  mutate(origin = trimws(origin)) %>%
  separate_rows(keyword, sep = ",") %>%
  mutate(keyword = trimws(keyword)) %>% 
  rename(author = origin)


poster_wide_ids <- poster_wide_raw %>% 
  distinct(id) %>% 
  mutate(poster_id = row_number())

poster_wide <- poster_wide_ids %>% 
  left_join(poster_wide_raw, by = "id") %>% 
  select(poster_id, everything()) %>% 
  select(-id) %>% 
  distinct()


# Poster - Year relation --------------------------------------------------

posters_years <- poster_wide %>%
  filter(!is.na(year)) %>%
  distinct(year) %>%
  mutate(year_id = row_number())

poster_year <- poster_wide %>%
  filter(!is.na(year)) %>%
  distinct(poster_id, year) %>%
  left_join(posters_years, by = "year") %>%
  select(-year)

test_that(
  desc = "unique combinations",
  expect_unique(c(year, year_id), data = posters_years)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(poster_id, year_id), data = poster_year)
)


# Poster - Author Relation ------------------------------------------------
# define Origin as Author

posters_authors <- poster_wide %>%
  filter(!is.na(author)) %>%
  distinct(author) %>%
  mutate(author_id = row_number())

poster_author <- poster_wide %>%
  filter(!is.na(author)) %>%
  distinct(poster_id, author) %>%
  left_join(posters_authors, by = "author") %>%
  select(-author)


test_that(
  desc = "unique combinations",
  expect_unique(c(author, author_id), data = posters_authors)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(poster_id, author_id), data = poster_author)
)

# Poster - Keyword Relation -----------------------------------------------

posters_keywords <- poster_wide %>%
  filter(!is.na(keyword)) %>%
  distinct(keyword) %>%
  mutate(keyword_id = row_number())

poster_keyword <- poster_wide %>%
  filter(!is.na(keyword)) %>%
  distinct(poster_id, keyword) %>%
  left_join(posters_keywords, by = "keyword") %>%
  select(-keyword)

test_that(
  desc = "unique combinations",
  expect_unique(c(keyword, keyword_id), data = posters_keywords)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(poster_id, keyword_id), data = poster_keyword)
)

# Poster distinct ----------------------------------------------------------

posters <- poster_wide %>%
  select(-year, -author, -keyword) %>%
  distinct()

test_that(
  desc = "unique combinations",
  expect_unique(everything(), data = posters)
)


# Construct dm ------------------------------------------------------------
# create a relational data model from the dataframes above

dm_raw <- dm(
  poster_author, poster_keyword, poster_year,
  posters_authors, posters, posters_years, posters_keywords
)

dm <- dm_raw %>%
  dm_add_pk(posters_authors, author_id, check = T) %>%
  dm_add_pk(posters, poster_id, check = T) %>%
  dm_add_pk(posters_years, year_id, check = T) %>%
  dm_add_pk(posters_keywords, keyword_id, check = T) %>%
  dm_add_fk(poster_author, poster_id, posters, poster_id, check = T) %>%
  dm_add_fk(poster_author, author_id, posters_authors, author_id, check = T) %>%
  dm_add_fk(poster_keyword, poster_id, posters, poster_id, check = T) %>%
  dm_add_fk(poster_keyword, keyword_id, posters_keywords, keyword_id, check = T) %>%
  dm_add_fk(poster_year, year_id, posters_years, year_id, check = T) %>%
  dm_add_fk(poster_year, poster_id, posters, poster_id, check = T)

# Inspect dm object -------------------------------------------------------

# Draw 
# for now only DiagrammeR 1.0.6.1 works 
# Issue: https://github.com/cynkra/dm/issues/823
# devtools::install_version("DiagrammeR", version = "1.0.6.1", repos = "http://cran.us.r-project.org")
dm %>% dm_draw()
dm %>% dm_nrow()
dm %>% dm_get_all_fks()
dm %>% dm_get_all_pks()
dm %>% dm_examine_constraints()

# Write to DB -------------------------------------------------------------

con <- connect_db()

deployed_dm <- copy_dm_to(con, dm, temporary = FALSE)
deployed_dm

DBI::dbDisconnect(con)
