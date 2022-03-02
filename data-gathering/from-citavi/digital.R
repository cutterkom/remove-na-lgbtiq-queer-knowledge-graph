# title: ETL poster data
# desc: extract poster data from citavi db tables, create clean relations, add pk's and fk's and load in DB
# ref: https://scriptsandstatistics.wordpress.com/2017/05/19/how-to-parse-citavi-files-using-r/

con <- dbConnect(RSQLite::SQLite(), "data-gathering/data/input/citavi-digital.ctt4")
dbListTables(con)

# non-empty tables
df_keyword <- dbGetQuery(con, "select * from Keyword") %>% 
  select(ID, Name) %>% 
  as_tibble() %>% 
  janitor::clean_names() %>% 
  left_join(df_ReferenceKeyword, by = c("id" = "KeywordID"))

df_location <- dbGetQuery(con, "select * from Location") %>% 
  select(ID, ReferenceID, Address) %>% 
  as_tibble() %>% 
  janitor::clean_names()

df_periodical <- dbGetQuery(con, "select * from Periodical") %>% 
  select(ID, Name) %>% 
  as_tibble() %>% 
  janitor::clean_names()

df_person <- dbGetQuery(con, "select * from Person") %>% 
  select(ID, Abbreviation, FirstName, LastName, MiddleName, Sex) %>% 
  as_tibble() %>% 
  janitor::clean_names()

df_publisher <- dbGetQuery(con, "select * from Publisher") %>% 
  select(ID, Name) %>% 
  as_tibble() %>% 
  janitor::clean_names()

df_reference <- dbGetQuery(con, "select * from Reference") %>% 
  select(ID, Abstract, CitationKey, CustomField1, CustomField2, CustomField3, CustomField4, Date, Notes, Number, OnlineAddress, PeriodicalID) %>% 
  as_tibble() %>% 
  janitor::clean_names()

df_ReferenceAuthor <- dbGetQuery(con, "select * from ReferenceAuthor") %>% as_tibble() %>% select(-Index)
df_ReferenceCollaborator <- dbGetQuery(con, "select * from ReferenceCollaborator") %>% as_tibble()  %>% select(-Index)
df_ReferenceEditor <- dbGetQuery(con, "select * from ReferenceEditor") %>% as_tibble() %>% select(-Index)
#df_ReferenceKeyword <- dbGetQuery(con, "select * from ReferenceKeyword") %>% as_tibble() %>% select(-Index)
df_ReferenceOrganization <- dbGetQuery(con, "select * from ReferenceOrganization") %>% as_tibble() %>% select(-Index)
df_ReferenceOthersInvolved <- dbGetQuery(con, "select * from ReferenceOthersInvolved") %>% as_tibble() %>% select(-Index)
df_ReferencePublisher <- dbGetQuery(con, "select * from ReferencePublisher") %>% as_tibble() %>% select(-Index)


# Unklar, was hier drin ist
# allerdings zB Rita Braaz, die sonst noch nicht abgebildet ist
# https://www.abendzeitung-muenchen.de/muenchen/rita-braaz-die-rosa-liste-verliert-ihre-bekannteste-frau-art-478209


df_periodical


df_person %>% 
  left_join(df_ReferenceAuthor, by = c("id" = "PersonID")) %>% 
  left_join(df_reference, by = c("ReferenceID" = "id")) %>% 
  left_join(df_ReferenceOrganization, by = c("id" = "ReferenceID"), suffix = c("", "_organization")) %>% 
  #left_join(df_ReferenceOthersInvolved, by = c("id" = "PersonID")) %>% View
  left_join(df_periodical, by = c("periodical_id" = "id")) %>% 
  left_join(df_ReferenceKeyword, by = c("id" = "ReferenceID")) %>% 
  left_join(df_keyword, by = c("KeywordID" = "id")) %>% 
  View


# Unklar, wie das alles zusammenhängt...
# im Forum nachschauen und zeigen lassen
# Was aber interessant ist: Personen



digitales_persons <- df_person %>% 
  filter(!is.na(first_name)) %>% 
  mutate(
    sex = 
      case_when(sex == 1 ~ "female",
                sex == 2 ~ "male",
                sex == 0 ~ "not stated")
  ) %>% 
  distinct(first_name, middle_name, last_name, sex) %>% 
  mutate(id = row_number()) %>% 
  select(id, everything())


test_that(
  desc = "unique combinations",
  expect_unique(everything(), data = digitales_persons)
)


# Construct dm ------------------------------------------------------------

dm <- dm(digitales_persons) %>% 
  dm_add_pk(digitales_persons, id, check = T)

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

