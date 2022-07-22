
library(tidyverse)
library(tidygraph)
library(ggraph) 

# alle daten bis zu den zwei Nachbarn
all_neighbors_data <- readRDS("analysis/network/data/all_neighbors_data.Rds")


# nur diejenigen Verbindungen aller root mit sich selber
relations_within <- all_neighbors_data %>% 
  filter(item1 %in% all_neighbors_data$root) %>% 
  select(contains("root"), contains("1"))  %>% 
  distinct()

# Welche Items kommen am häufigsten vor?
relations_within %>% count(root, rootLabel, sort = T)

# Welche Verbindungen kommen am häufigsten vor?
relations_within %>% count(property1, property1Label, sort = T)




graph_tbl <- relations_within %>% 
  select(from = root, to = item1, property1) %>% 
  tidygraph::as_tbl_graph()

ggraph(
  graph = graph_tbl,
  layout = "kk") +
  geom_node_point() +
  geom_edge_diagonal(alpha = 0.2)


edges <- relations_within %>% select(from = root, to = item1)

nodes <- bind_rows(relations_within %>% select(item = root),
                   relations_within %>% select(item = item1)) %>% 
  distinct() %>% 
  rowid_to_column("id")

# simple metrics
# but cant access degree this way... 
graph_metrics <- graph_tbl %>% 
  activate(nodes) %>%
  mutate(degree = centrality_degree()) %>%
  activate(edges) %>%
  mutate(centrality = centrality_edge_betweenness()) %>%
  arrange(desc(centrality)) %>% 
  as_tibble() %>% 
  left_join(nodes %>% rename(item_from = item), by = c("from" = "id")) %>% 
  left_join(nodes %>% rename(item_to = item), by = c("to" = "id"))

# with labels
graph_metrics %>% 
  left_join(relations_within, by = c("item_from" = "root", "item_to" = "item1", "property1")) %>% View

# Bei Postern ist aber nicht die direkte Verbindung interessant, sondern die indirekte zu den anderen Orgs. Beispiel:
all_neighbors_data %>% 
  filter(root == "Q404263") %>% 
  filter(property1 == "P174") %>% View

