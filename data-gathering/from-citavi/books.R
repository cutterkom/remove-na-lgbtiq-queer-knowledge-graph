# title: Extract Books data from Citavi relational DB and load in DB
# desc: script extracts db tables and creates clean relations. With dm pk and fk are created and loaded in DB

# ref: https://scriptsandstatistics.wordpress.com/2017/05/19/how-to-parse-citavi-files-using-r/

library(RSQLite)
library(DBI)
library(tidyverse)
library(stringr)
library(glue)
library(dm)
library(kabrutils)

# Pfad zur Nextcloud
link_to_cloud <- "/Users/kabr/Nextcloud/Forum (2)/"

con_books <- DBI::dbConnect(RSQLite::SQLite(), paste0(link_to_cloud, "data/Archiv fhm Buecher (Team).ctt4"))
dbListTables(con_books)

df_ReferenceKeyword <- dbGetQuery(con_books, "select * from Publisher")

df_Author <- dbGetQuery(con_books, "select * from Person")
df_Reference <- dbGetQuery(con_books, "select * from Reference")

### Die Bücher liegen in der Reference Tabelle, die Schlagworte in der Keyword und über die Tabelle ReferenceKeyword sind sie miteinander verbunden.
### wer das Buch geschireben hat in der Person.
df_books_big <- dbGetQuery(con_books, "select * FROM Reference")
df_books <- dbGetQuery(con_books, "select ID, Title, Subtitle, TitleSupplement, Year, PlaceOfPublication, ISBN, SeriesTitleID FROM Reference")

df_keyword <- dbGetQuery(con_books, "select ID, Name FROM Keyword")
df_link_keyword_books <- dbGetQuery(con_books, "select * FROM ReferenceKeyword")

df_link_keyword_books_join <- df_link_keyword_books %>%
  left_join(., df_keyword, by = c("KeywordID" = "ID"))

df_link_keyword_books <- df_link_keyword_books %>%
  left_join(., df_keyword, by = c("KeywordID" = "ID")) %>%
  select(-KeywordID, -Index)

### die Tabelle Person enthält die Inforamtion über den Autor
df_author <- dbGetQuery(con_books, "select ID, FirstName, MiddleName, LastName FROM Person")
df_link_author_book <- dbGetQuery(con_books, "select * FROM ReferenceAuthor")

df_person_book <- df_link_author_book %>%
  left_join(., df_author, by = c("PersonID" = "ID")) %>%
  select(-PersonID, -Index) %>%
  mutate(
    author1 = ifelse(is.na(MiddleName) & !is.na(FirstName), paste(FirstName, LastName), NA),
    author2 = ifelse(is.na(MiddleName) & is.na(FirstName), LastName, NA),
    author3 = ifelse(!is.na(MiddleName), paste(FirstName, MiddleName, LastName), NA) # ,
    # author4 = ifelse(str_detect(author2, ","),  %>% paste(first, second), NA)
  ) %>%
  separate(author2, into = c("second", "first"), sep = ", ", remove = FALSE) %>%
  mutate(
    author4 = ifelse(!is.na(first), paste(first, second), NA),
    author2 = ifelse(!is.na(first), NA, author2)
  ) %>%
  select(-first, -second) %>%
  pivot_longer(cols = 5:8) %>%
  filter(!is.na(value)) %>%
  select(ReferenceID, value) %>%
  group_by(ReferenceID) %>%
  summarise(Autor_in = glue::glue_collapse(value, sep = ", "))


# Herausgaber -------------------------------------------------------------

df_link_herausgeber_person <- dbGetQuery(con_books, "select * FROM ReferenceEditor") %>% as_tibble()


df_herausgeber_book <- df_link_herausgeber_person %>%
  left_join(., df_author, by = c("PersonID" = "ID")) %>%
  select(-PersonID, -Index) %>%
  mutate(
    author1 = ifelse(is.na(MiddleName) & !is.na(FirstName), paste(FirstName, LastName), NA),
    author2 = ifelse(is.na(MiddleName) & is.na(FirstName), LastName, NA),
    author3 = ifelse(!is.na(MiddleName), paste(FirstName, MiddleName, LastName), NA) # ,
    # author4 = ifelse(str_detect(author2, ","),  %>% paste(first, second), NA)
  ) %>%
  separate(author2, into = c("second", "first"), sep = ", ", remove = FALSE) %>%
  mutate(
    author4 = ifelse(!is.na(first), paste(first, second), NA),
    author2 = ifelse(!is.na(first), NA, author2)
  ) %>%
  select(-first, -second) %>%
  pivot_longer(cols = 5:8) %>%
  filter(!is.na(value)) %>%
  mutate(value = paste0(value, " (Hg.)")) %>%
  select(ReferenceID, value) %>%
  group_by(ReferenceID) %>%
  summarise(Autor_in = glue::glue_collapse(value, sep = ", "))


# Autor_in und Herausgeber zusammenfassen ---------------------------------

df_person_book <- bind_rows(df_person_book, df_herausgeber_book)

# Verlag ------------------------------------------------------------------

df_publisher <- dbGetQuery(con_books, "select ID, Name from Publisher")
df_link_publisher_book <- dbGetQuery(con_books, "select * from ReferencePublisher")

df_publisher_book <- df_link_publisher_book %>%
  left_join(., df_publisher, by = c("PublisherID" = "ID")) %>%
  select(-PublisherID, -Index) %>%
  rename("Publisher" = "Name")


### die Tabelle SeriesTitle enthält unterschiedliche Informationen, mal den Namen einer Zeitschrift, mal einen Verlag
df_SeriesTitle <- dbGetQuery(con_books, "select ID, Name from SeriesTitle") %>% rename("SeriesTitle" = "Name")
# df_SeriesTitleEditor <- dbGetQuery(con_books,'select * from SeriesTitleEditor')

### Alle Informationen zusammenführen

df_book_prep <- df_books %>%
  left_join(., df_person_book, by = c("ID" = "ReferenceID")) %>%
  left_join(., df_publisher_book, by = c("ID" = "ReferenceID")) %>%
  left_join(., df_SeriesTitle, by = c("SeriesTitleID" = "ID")) %>%
  select(-SeriesTitleID) %>% # names(.)
  rename(
    "SKU" = "ID",
    "Name" = "Title",
    "meta:Subtitle" = "Subtitle",
    "meta:TitleSupplement" = "TitleSupplement",
    "meta:PlaceOfPublication" = "PlaceOfPublication",
    "meta:ISBN" = "ISBN",
    "meta:SeriesTitle" = "SeriesTitle"
  ) %>%
  mutate(Autor_in = as.character(Autor_in))


# Create Relations --------------------------------------------------------

books_wide_raw <- df_book_prep %>%
  as_tibble() %>%
  janitor::clean_names()

books_wide <- books_wide_raw %>%
  separate_rows(autor_in, sep = ",") %>%
  mutate(autor_in = trimws(autor_in)) %>%
  separate_rows(year, sep = ";") %>%
  mutate(year = trimws(year)) %>%
  mutate(
    is_herausgeber = ifelse(str_detect(autor_in, "Hg."), 1, 0),
    herausgeber = ifelse(is_herausgeber == 1, str_remove(autor_in, " \\(Hg\\.\\)"), NA),
    autor_in = ifelse(is_herausgeber == 1, NA, autor_in)
  ) %>%
  rename(book_id = sku)



# Book - Author Relation --------------------------------------------------

authors <- books_wide %>%
  filter(!is.na(autor_in)) %>%
  distinct(autor_in) %>%
  mutate(author_id = row_number())

book_author <- books_wide %>%
  filter(!is.na(autor_in)) %>%
  select(book_id, autor_in) %>%
  left_join(authors, by = "autor_in") %>%
  select(-autor_in)
book_author


# Book - Editor Relation --------------------------------------------------

editors <- books_wide %>%
  filter(is_herausgeber == 1) %>%
  distinct(herausgeber) %>%
  mutate(editor_id = row_number())

book_editor <- books_wide %>%
  filter(is_herausgeber == 1) %>%
  select(book_id, herausgeber) %>%
  left_join(editors, by = "herausgeber") %>%
  select(-herausgeber)
book_editor


# Book - Publisher Relation -----------------------------------------------

publishers <- books_wide %>%
  filter(!is.na(publisher)) %>%
  distinct(publisher) %>%
  mutate(publisher_id = row_number())

book_publisher <- books_wide %>%
  filter(!is.na(publisher)) %>%
  select(book_id, publisher) %>%
  left_join(publishers, by = "publisher") %>%
  select(-publisher)
book_publisher


# Book - Series relation --------------------------------------------------

series <- books_wide %>%
  filter(!is.na(meta_series_title)) %>%
  distinct(meta_series_title) %>%
  mutate(series_id = row_number())

book_series <- books_wide %>%
  filter(!is.na(meta_series_title)) %>%
  select(book_id, meta_series_title) %>%
  left_join(series, by = "meta_series_title") %>%
  select(-meta_series_title)
book_series


# Book - Place of Publication relation ------------------------------------

locations <- books_wide %>%
  filter(!is.na(meta_place_of_publication)) %>%
  distinct(meta_place_of_publication) %>%
  mutate(location_id = row_number())

book_location <- books_wide %>%
  filter(!is.na(meta_place_of_publication)) %>%
  select(book_id, meta_place_of_publication) %>%
  left_join(locations, by = "meta_place_of_publication") %>%
  select(-meta_place_of_publication)
book_location


# Book - Year relation ----------------------------------------------------

years <- books_wide %>%
  filter(!is.na(year)) %>%
  distinct(year) %>%
  mutate(year_id = row_number())

book_year <- books_wide %>%
  filter(!is.na(year)) %>%
  select(book_id, year) %>%
  left_join(years, by = "year") %>%
  select(-year)
book_year

# Books distinct ----------------------------------------------------------

books <- books_wide %>%
  select(-year, -meta_place_of_publication, -autor_in, -publisher, -meta_series_title, -is_herausgeber, -herausgeber) %>%
  distinct()


relations <-
  list(
    "books" = books,
    "book_year" = book_year,
    "years" = years,
    "book_location" = book_location,
    "locations" = locations,
    "book_series" = book_series,
    "series" = series,
    "book_publisher" = book_publisher,
    "publishers" = publishers,
    "book_editor" = book_editor,
    "editors" = editors,
    "book_author" = book_author,
    "authors" = authors
  )

# save(
#   books,
#   book_year,
#   years,
#   book_location,
#   locations,
#   book_series,
#   series,
#   book_publisher,
#   publishers,
#   book_editor,
#   editors,
#   book_author,
#   authors,
#   file = "construct-knowledge-graph/data/books.Rdata")


# Construct dm ------------------------------------------------------------

dm_raw <- dm(
  book_author, book_editor, book_location,
  book_publisher, book_series, book_year,
  authors, books, editors, locations, publishers, series, years
)

dm <- dm_raw %>%
  dm_add_pk(authors, author_id) %>%
  dm_add_pk(books, book_id) %>%
  dm_add_pk(editors, editor_id) %>%
  dm_add_pk(locations, location_id) %>%
  dm_add_pk(publishers, publisher_id) %>%
  dm_add_pk(series, series_id) %>%
  dm_add_pk(years, year_id) %>%
  dm_add_fk(book_author, book_id, books, book_id) %>%
  dm_add_fk(book_author, author_id, authors, author_id) %>%
  dm_add_fk(book_editor, editor_id, editors, editor_id) %>%
  dm_add_fk(book_editor, book_id, books, book_id) %>%
  dm_add_fk(book_location, location_id, locations, location_id) %>%
  dm_add_fk(book_location, book_id, books, book_id) %>%
  dm_add_fk(book_publisher, publisher_id, publishers, publisher_id) %>%
  dm_add_fk(book_publisher, book_id, books, book_id) %>%
  dm_add_fk(book_series, series_id, series, series_id) %>%
  dm_add_fk(book_series, book_id, books, book_id) %>%
  dm_add_fk(book_year, year_id, years, year_id) %>%
  dm_add_fk(book_year, book_id, books, book_id)


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
