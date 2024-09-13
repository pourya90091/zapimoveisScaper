# Zapimoveis Scraper

## Setup and Run

### Install Requirements

```bash
pip install -r requirements.txt
playwright install
playwright install-deps
``` 

### Config
You can easily config your desire settings in `.env` file at the root of project.

Variables you can set:

- **CITY**: You can obtain it from zapimoveis.com.br search system. Go to https://www.zapimoveis.com.br and in the search input type a city name then click on one of the results. After that you should be in a URL like below URL (copy the **CITY** part):

    - https://www.zapimoveis.com.br/venda/imoveis/**CITY**/...

    - >Note: Select anything between `imoveis/` and `/...`.

### Run

```bash
scrapy crawl zapimoveis -o export/zapimoveis.csv
```

## Tips

>**Tip** : If you set a value for CITY (in `.env` file), the scraper would scrape all properties related with that city name but if you leave it empty, the scraper would try to scrape all available properties in the target website.
---
