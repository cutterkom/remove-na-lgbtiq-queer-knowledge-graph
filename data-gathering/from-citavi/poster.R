# title: ETL poster data
# desc: extract poster data from citavi db tables, create clean relations, add pk's and fk's and load in DB


library(RSQLite)
library(DBI)
library(tidyverse)
library(kabrutils)

link_to_cloud <- "/Users/kabr/Nextcloud/Forum (2)/"

con_poster <- DBI::dbConnect(RSQLite::SQLite(), paste0(link_to_cloud,"data/Archiv fhm Poster (Team).ctt4"))
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
  select(-online_address)

poster_wide <- poster_wide_raw %>% 
  separate_rows(origin, sep = ",") %>%
  mutate(origin = trimws(origin)) %>%
  separate_rows(keyword, sep = ",") %>%
  mutate(keyword = trimws(keyword)) %>%
  rename(poster_id = id,
         author = origin)


# Poster - Year relation --------------------------------------------------

years <- poster_wide %>%
  filter(!is.na(year)) %>%
  distinct(year) %>%
  mutate(year_id = row_number())

poster_year <- poster_wide %>%
  filter(!is.na(year)) %>%
  select(poster_id, year) %>%
  left_join(years, by = "year") %>%
  select(-year)

poster_year


# Poster - Author Relation ------------------------------------------------
# define Origin as Author

authors <- poster_wide %>%
  filter(!is.na(author)) %>%
  distinct(author) %>%
  mutate(author_id = row_number())

poster_author <- poster_wide %>%
  filter(!is.na(author)) %>%
  select(poster_id, author) %>%
  left_join(authors, by = "author") %>%
  select(-author)
poster_author


# Poster - Keyword Relation -----------------------------------------------

keywords <- poster_wide %>%
  filter(!is.na(keyword)) %>%
  distinct(keyword) %>%
  mutate(keyword_id = row_number())

poster_keyword <- poster_wide %>%
  filter(!is.na(keyword)) %>%
  select(poster_id, keyword) %>%
  left_join(keywords, by = "keyword") %>%
  select(-keyword)
poster_keyword

# Poster distinct ----------------------------------------------------------

posters <- poster_wide %>%
  select(-year, -author, -keyword) %>%
  distinct()


# Construct dm ------------------------------------------------------------

dm_raw <- dm(
  poster_author, poster_keyword, poster_year,
  authors, posters, years, keywords
)

dm <- dm_raw %>%
  dm_add_pk(authors, author_id) %>%
  dm_add_pk(posters, poster_id) %>%
  dm_add_pk(years, year_id) %>%
  dm_add_pk(keywords, keyword_id) %>%
  dm_add_fk(poster_author, poster_id, posters, poster_id) %>%
  dm_add_fk(poster_author, author_id, authors, author_id) %>%
  dm_add_fk(poster_keyword, poster_id, posters, poster_id) %>%
  dm_add_fk(poster_keyword, keyword_id, keywords, keyword_id) %>%
  dm_add_fk(poster_year, year_id, years, year_id) %>%
  dm_add_fk(poster_year, poster_id, posters, poster_id)

# Inspect dm object -------------------------------------------------------

dm %>% dm_draw()
dm %>% dm_nrow()
dm %>% dm_get_all_fks()
dm %>% dm_get_all_pks()
dm %>% dm_examine_constraints()

# Write to DB -------------------------------------------------------------

con <- connect_db()

deployed_dm <-
  copy_dm_to(con, dm, temporary = FALSE)

deployed_dm
DBI::dbDisconnect(con)