

# 2 kinds of bugfixes -----------------------------------------------------

# 1) female version of descs
query <- '
SELECT ?item ?itemDescription ?Forum_M_nchen_ID ?Geschlecht WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr". }
  ?item wdt:P131 wd:Q400012.
  ?item wdt:P2 wd:Q7.
    OPTIONAL { ?item wdt:P154 ?Geschlecht. }
  
  OPTIONAL { ?item wdt:P728 ?Forum_M_nchen_ID. }
}'

res_fr <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(Geschlecht = str_extract(Geschlecht, "Q[0-9]+"),
         item = str_extract(item, "Q[0-9]+"))

res_es %>% filter(Geschlecht == "Q17")

female_fr <- res_fr %>% 
  filter(Geschlecht == "Q17") %>% 
  #filter(P154 == "Q17") %>% 
  #select(item, Lde, Dde, Den, Dfr, Des) %>% 
  # mutate(Dfr = str_replace(Dfr, "Auteur|auteur", "autrice"),
  #        Dfr = str_replace(Dfr, "écrivain ", "écrivaine ")) %>% 
  mutate(
    itemDescription2 = str_replace(itemDescription, "Auteur|auteur", "autrice"),
    itemDescription2 = str_replace(itemDescription2, "écrivain ", "écrivaine "),
    itemDescription2 = str_replace(itemDescription2, "allemand", "allemande"),
    itemDescription2 = str_remove_all(itemDescription2, '\"|\"|, München|@fr|München, ')
    )
import <- female_fr %>% 
  distinct(item, value = itemDescription2) %>% 
  mutate(property = "Dfr",
         value = trimws(value)) 

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/persons-fr.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_persons",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)



# 2 GND properties --------------------------------------------------------

query <- '
SELECT ?item ?itemLabel ?Deutsche_Biographie__GND__ID ?GND_ID WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
  ?item wdt:P131 wd:Q400012.
  ?item wdt:P2 wd:Q7.
  OPTIONAL { ?item wdt:P622 ?Deutsche_Biographie__GND__ID. }
  OPTIONAL { ?item wdt:P76 ?GND_ID. }
}
'
query_res <- query %>% 
  sparql_to_tibble(endpoint = config$connection$sparql_endpoint) %>% 
  mutate(itemLabel = str_remove_all(itemLabel, '\"|\"|, München|@de|München, '),
         item = str_extract(item, "Q[0-9]+")) %>% 
  rename(P622 = Deutsche_Biographie__GND__ID, P76 = GND_ID)

import <- query_res %>% 
  filter(!is.na(P622)) %>% 
  distinct(item, P622) %>% 
  rename(P76 = P622) %>% 
  pivot_longer(cols = 2:last_col(), names_to = "property", values_to = "value") %>% 
  # fix helper with two instances_of:
  mutate(property = str_remove(property, "_.*")) %>% 
  filter(!is.na(value))

import

import %>% count(item, sort =T)

write_wikibase(
  items = import$item,
  properties = import$property,
  values = import$value,
  format = "csv",
  format.csv.file = "data-publishing/factgrid/data/persons-add-gnd.csv",
  api.username = config$connection$api_username,
  api.token = config$connection$api_token,
  api.format = "v1",
  api.batchname = "entities_persons",
  api.submit = F,
  quickstatements.url = config$connection$quickstatements_url
)


