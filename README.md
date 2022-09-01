# Remove NA

---

This project is finished. Results, an essay and methodology are available on 
* [queerdata.forummuenchen.org/en](http://queerdata.forummuenchen.org/en) in English
* [queerdata.forummuenchen.org](http://queerdata.forummuenchen.org) in German 

![The knowledge graph about queer history.](https://queerdata.forummuenchen.org/img/full-network-big-small.png)

---

This repository is the central hub for everything related to the "Remove NA" project.
The goal is to create a LGBTIQ* knowledge graph based on data of a community archive in Munich called [Forum Queeres Archiv München](https://forummuenchen.org/en/english/). 
We collect cultural heritage on queer history, especially in Munich and Bavaria/Germany.

**More on the project:**

* [Technical Documentation](https://cutterkom.github.io/remove-na-lgbtiq-queer-knowledge-graph/)
* [Prototype Fund Website](https://prototypefund.de/project/remove-na/)
* [Remove NA: Warum ich einen LGBTIQ*-Knowledge Graph bauen werde](https://katharinabrunner.de/2022/03/remove-na-ein-lgbtiq-knowledge-graph/)

## Repository Layout

* `data-gathering`: contains the scripts in the data collection process from various, heterogeneous data sources
* `data-linking`: link entities to external IDs, e.g. Wikidata or GND
* `data-modeling`: create RDF data (linked data)
* `docs`: technical documentation based on `bookdown`
* `apps`: smaller helper apps e.g. the [entity resolver app based on R-Shiny](https://github.com/cutterkom/remove-na-lgbtiq-queer-knowledge-graph/blob/main/apps/entity-resolver/index.Rmd) that helps during de-deduplication


