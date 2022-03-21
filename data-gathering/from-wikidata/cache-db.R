library(tidywikidatar) # tidy queries
library(WikidataR) # SPARQL queries
library(tidyverse)

config <- config::get(file = "~/config.yml", "wikidata")
tw_enable_cache(SQLite = FALSE)
# dd <- tw_set_cache_db(
#                 driver = "RMariaDB::MariaDB()",
#                 host = "config$host",
#                 port = config$port,
#                 database = "wikidata",
#                 user = "config$user",
#                 pwd = "config$password")

con <- connect_db(credential_name = "db_local_wikidata")
con
cc <- tw_connect_to_cache(connection = con, RSQLite = FALSE)
cc
tw_get(id = "Q180099", language = "en", cache = T, cache_connection = cc)