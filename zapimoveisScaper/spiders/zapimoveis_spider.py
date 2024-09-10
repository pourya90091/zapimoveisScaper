import scrapy
from scrapy.http import Response
from scrapy_playwright.page import PageMethod
from pathlib import Path
from zapimoveisScaper import settings
import gzip
import shutil
import re
from datetime import datetime


TEMP_DIR = settings.BASE_DIR / "temp"
Path(TEMP_DIR).mkdir(parents=True, exist_ok=True) # Ensures that TEMP_DIR exists.

def extract_gz(filepath):
    with gzip.open(filepath, 'rb') as f_in:
        xml_file_path = str(filepath).replace(".gz", "")

        with open(xml_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return xml_file_path


def save_file(response: Response):
    # Save the file locally
    file_name = response.url.split("/")[-1]
    path = TEMP_DIR / file_name
    with open(path, "wb") as f:
        f.write(response.body)

    return path


class ZapimoveisSpider(scrapy.Spider):
    name = "zapimoveis"
    namespaces = [
        ("x", "http://www.sitemaps.org/schemas/sitemap/0.9")
    ]
    base_url = "https://www.zapimoveis.com.br"

    def start_requests(self):
        urls = [
            f"{self.base_url}/sitemap_used_resultpage_streets_index.xml",
            # f"{self.base_url}/sitemap_development_resultpage_index.xml"
        ]

        for url in urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response: Response):
        for sm in response.xpath("//x:loc/text()", namespaces=self.namespaces).getall():
            yield scrapy.Request(
                url=sm, 
                callback=self.gz_to_xml
            )

    def gz_to_xml(self, response: Response):
        path = save_file(response)
        sitemap = extract_gz(path)
        local_file_url = f"file:///{sitemap}"

        yield scrapy.Request(url=local_file_url, callback=self.sitemap_handler)

    def sitemap_handler(self, response: Response):
        pages = response.xpath("//x:loc/text()", namespaces=self.namespaces).getall()

        yield from response.follow_all(pages, callback=self.page_handler, meta={
            "playwright": True,
            "playwright_page_methods": [
                PageMethod("evaluate", "document.body.style.zoom = '1%';"), # Zoom out the page (force the page to load without scrolling down)
                PageMethod("wait_for_load_state", "networkidle"), # Wait until the network is idle (all network requests are done)
            ]})

    def page_handler(self, response: Response):
        properties = response.xpath("//div[@class='listing-wrapper__content']/div[@data-position or @data-type]//a[@href]/@href").extract()

        # yield from response.follow_all(properties, callback=self.property_handler, meta={"playwright": True})
        yield from response.follow_all(properties, callback=self.property_handler)

    def property_handler(self, response: Response):
        def remove_whitespaces(text):
            return text.strip() if text else None

        def get_property_type():
            # ------------- RESIDENTIAL -------------
            if "-apartamento-" in response.url:
                property_type = "Apartment"
            elif "-studio-" in response.url:
                property_type = "Studio"
            elif "-quitinete-" in response.url:
                property_type = "Studio apartment"
            elif "-casa-" in response.url:
                property_type = "House"
            elif "-sobrados-" in response.url:
                property_type = "Townhouse"
            elif "-cobertura-" in response.url:
                property_type = "Penthouse"
            elif "-flat-" in response.url:
                property_type = "Flat"
            elif "-loft-" in response.url:
                property_type = "Loft"
            elif "-terreno-" in response.url:
                property_type = "Land"
            elif "-fazenda-" in response.url:
                property_type = "Country House"
            # ------------- RESIDENTIAL -------------

            # ------------- BUSINESS -------------
            elif "-loja-salao-" in response.url:
                property_type = "Salon"
            elif "-conjunto-comercial-sala-" in response.url:
                property_type = "Commercial Unit"
            elif "-andar-laje-corporativa-" in response.url:
                property_type = "Corporate Floor"
            elif "-hotel-" in response.url:
                property_type = "Hotel"
            elif "-predio-" in response.url:
                property_type = "Entire Building"
            elif "-galpao-" in response.url:
                property_type = "Warehouse"
            # TODO: Find a url of Garage property and add it
            # ------------- BUSINESS -------------
            else:
                property_type = None

            return property_type

        def get_listing_type():
            if "aluguel-" in response.url:
                listing_type = "RENTAL"
            elif "venda-" in response.url:
                listing_type = "SALE"
            else:
                listing_type = None
            
            return listing_type

        def get_reference_market():
            RESIDENTIAL = ["-apartamento-", "-studio-", "-quitinete-", "-casa-", "-sobrados-",
                           "-cobertura-", "-flat-", "-loft-", "-terreno-", "-fazenda-"]
            for type in RESIDENTIAL:
                if type in response.url:
                    return "RESIDENTIAL"

            BUSINESS = ["-loja-salao-", "-conjunto-comercial-sala-", "-andar-laje-corporativa-",
                        "-hotel-", "-predio-", "-galpao-"]
            for type in BUSINESS:
                if type in response.url:
                    return "BUSINESS"

            return None # In case that reference_market was unknown


        breadcrumb = response.xpath("//ol[contains(@class, 'breadcrumb')]/li[1]/a/text()").get()

        yield {
            "competence_date": datetime.now().strftime("%Y-%m-%d"),
            "listing_id": re.search(r"id-(\d+)/$", response.url).group(1),
            "listing_title": remove_whitespaces(response.xpath("//h1[contains(@class, 'description__title')]/text()").get()),
            "listing_description": remove_whitespaces(response.xpath("//p[@data-testid='description-content']/text()").get()),
            "property_type": get_property_type(),
            "listing_type": get_listing_type(),
            "reference_market": get_reference_market(),
            "location_description": None,
            "location_region": None,
            "location_province": None,
            "location_city": None,
            "location_zip": None,
            "locaiton_neighborhood": None,
            "location_street": None,
            "location_street_n": None,
            "location_lon": None,
            "location_lat": None,
            "area_unit": None,
            "area_value": None,
            "bedrooms": None,
            "bathrooms": None,
            "floor": None,
            "total_floors": None,
            "amenities_list": None,
            "listing_date": None,
            "listing_status": None,
            "agent_id": None,
            "agent_url": None,
            "price": None,
            "imageurl": None,
            "itemurl": None,
        }
