# title: Extract Books data from Citavi relational DB and load in DB
# desc: script extracts db tables and creates clean relations. With dm pk and fk are created and loaded in DB
# ref: https://scriptsandstatistics.wordpress.com/2017/05/19/how-to-parse-citavi-files-using-r/

library(tidyverse)
library(kabrutils)
library(RSQLite)
library(DBI)
library(dm)

con_books <- dbConnect(RSQLite::SQLite(), "data-gathering/data/input/citavi-buecher.ctt4")
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

df_link_editor_person <- dbGetQuery(con_books, "select * FROM ReferenceEditor") %>% as_tibble()


df_editor_book <- df_link_editor_person %>%
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


# Autor_in und editor zusammenfassen ---------------------------------

df_person_book <- bind_rows(df_person_book, df_editor_book)

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
  janitor::clean_names() %>%
  separate_rows(autor_in, sep = ",") %>%
  mutate(author = trimws(autor_in)) %>%
  separate_rows(year, sep = ";") %>%
  mutate(year = trimws(year)) %>%
  mutate(
    is_editor = ifelse(str_detect(author, "Hg."), 1, 0),
    editor = ifelse(is_editor == 1, str_remove(author, " \\(Hg\\.\\)"), NA),
    author = ifelse(is_editor == 1, NA, author)
  ) %>% 
  rename(
    series = meta_series_title,
    location = meta_place_of_publication,
    subtitle = meta_subtitle, 
    title_supplement = meta_title_supplement, 
    isbn = meta_isbn
    )

books_wide_ids <- books_wide_raw %>% 
  distinct(sku) %>% 
  mutate(book_id = row_number())

books_wide <- books_wide_raw %>% 
  left_join(books_wide_ids, by = "sku") %>% 
  select(book_id, everything()) %>% 
  select(-sku) %>% 
  distinct()

# Book - Author Relation --------------------------------------------------

books_authors <- books_wide %>%
  filter(!is.na(author)) %>%
  distinct(author) %>%
  mutate(author_id = row_number())

book_author <- books_wide %>%
  filter(!is.na(author)) %>%
  distinct(book_id, author) %>%
  left_join(books_authors, by = "author") %>%
  select(-author)

test_that(
  desc = "unique combinations",
  expect_unique(c(author, author_id), data = books_authors)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(book_id, author_id), data = book_author)
)


# Book - Editor Relation --------------------------------------------------

books_editors <- books_wide %>%
  filter(is_editor == 1) %>%
  distinct(editor) %>%
  mutate(editor_id = row_number())

book_editor <- books_wide %>%
  filter(is_editor == 1) %>%
  distinct(book_id, editor) %>%
  left_join(books_editors, by = "editor") %>%
  select(-editor)

test_that(
  desc = "unique combinations",
  expect_unique(c(editor, editor_id), data = books_editors)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(book_id, editor_id), data = book_editor)
)

# Book - Publisher Relation -----------------------------------------------

books_publishers <- books_wide %>%
  filter(!is.na(publisher)) %>%
  distinct(publisher) %>%
  mutate(publisher_id = row_number())

book_publisher <- books_wide %>%
  filter(!is.na(publisher)) %>%
  distinct(book_id, publisher) %>%
  left_join(books_publishers, by = "publisher") %>%
  select(-publisher)

test_that(
  desc = "unique combinations",
  expect_unique(c(publisher, publisher_id), data = books_publishers)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(book_id, publisher_id), data = book_publisher)
)

# Book - Series relation --------------------------------------------------

books_series <- books_wide %>%
  filter(!is.na(series)) %>%
  distinct(series) %>%
  mutate(series_id = row_number())

book_series <- books_wide %>%
  filter(!is.na(series)) %>%
  distinct(book_id, series) %>%
  left_join(books_series, by = "series") %>%
  select(-series)

test_that(
  desc = "unique combinations",
  expect_unique(c(series, series_id), data = books_series)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(book_id, series_id), data = book_series)
)


# Book - Place of Publication relation ------------------------------------

books_locations <- books_wide %>%
  filter(!is.na(location)) %>%
  distinct(location) %>%
  mutate(location_id = row_number())

book_location <- books_wide %>%
  filter(!is.na(location)) %>%
  distinct(book_id, location) %>%
  left_join(books_locations, by = "location") %>%
  select(-location)

test_that(
  desc = "unique combinations",
  expect_unique(c(location, location_id), data = books_locations)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(book_id, location_id), data = book_location)
)


# Book - Year relation ----------------------------------------------------

books_years <- books_wide %>%
  filter(!is.na(year)) %>%
  distinct(year) %>%
  mutate(year_id = row_number())

book_year <- books_wide %>%
  filter(!is.na(year)) %>%
  distinct(book_id, year) %>%
  left_join(books_years, by = "year") %>%
  select(-year)


test_that(
  desc = "unique combinations",
  expect_unique(c(year, year_id), data = books_years)
)

test_that(
  desc = "unique combinations",
  expect_unique(c(book_id, year_id), data = book_year)
)

# Books distinct ----------------------------------------------------------

books <- books_wide %>%
  distinct(book_id, name, subtitle, title_supplement, isbn)

test_that(
  desc = "unique combinations",
  expect_unique(everything(), data = books)
)

relations <-
  list(
    "books" = books,
    "book_year" = book_year,
    "years" = books_years,
    "book_location" = book_location,
    "locations" = books_locations,
    "book_series" = book_series,
    "series" = books_series,
    "book_publisher" = book_publisher,
    "publishers" = books_publishers,
    "book_editor" = book_editor,
    "editors" = books_editors,
    "book_author" = book_author,
    "authors" = books_authors
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
  books_authors, books, books_editors, books_locations, books_publishers, books_series, books_years
)

dm <- dm_raw %>%
  dm_add_pk(books_authors, author_id, check = T) %>%
  dm_add_pk(books, book_id, check = T) %>%
  dm_add_pk(books_editors, editor_id, check = T) %>%
  dm_add_pk(books_locations, location_id, check = T) %>%
  dm_add_pk(books_publishers, publisher_id, check = T) %>%
  dm_add_pk(books_series, series_id, check = T) %>%
  dm_add_pk(books_years, year_id, check = T) %>%
  dm_add_fk(book_author, book_id, books, book_id, check = T) %>%
  dm_add_fk(book_author, author_id, books_authors, author_id, check = T) %>%
  dm_add_fk(book_editor, editor_id, books_editors, editor_id, check = T) %>%
  dm_add_fk(book_editor, book_id, books, book_id, check = T) %>%
  dm_add_fk(book_location, location_id, books_locations, location_id, check = T) %>%
  dm_add_fk(book_location, book_id, books, book_id, check = T) %>%
  dm_add_fk(book_publisher, publisher_id, books_publishers, publisher_id, check = T) %>%
  dm_add_fk(book_publisher, book_id, books, book_id, check = T) %>%
  dm_add_fk(book_series, series_id, books_series, series_id, check = T) %>%
  dm_add_fk(book_series, book_id, books, book_id, check = T) %>%
  dm_add_fk(book_year, year_id, books_years, year_id, check = T) %>%
  dm_add_fk(book_year, book_id, books, book_id, check = T)


# Inspect dm object -------------------------------------------------------

dm %>% dm_draw()
dm %>% dm_nrow()
dm %>% dm_get_all_fks()
dm %>% dm_get_all_pks()
dm %>% dm_examine_constraints()

# Write to DB -------------------------------------------------------------

con <- connect_db(credential_name = "db_local")
deployed_dm <- copy_dm_to(con, dm, temporary = FALSE)
deployed_dm
DBI::dbDisconnect(con)
