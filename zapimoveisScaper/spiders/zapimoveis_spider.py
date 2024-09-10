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
            yield scrapy.Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response: Response):
        for sm in response.xpath("//x:loc/text()", namespaces=self.namespaces).getall():
            yield scrapy.Request(
                url=sm, 
                callback=self.gz_to_xml,
                dont_filter=True
            )

    def gz_to_xml(self, response: Response):
        path = save_file(response)
        sitemap = extract_gz(path)
        local_file_url = f"file:///{sitemap}"

        yield scrapy.Request(url=local_file_url, callback=self.sitemap_handler, dont_filter=True)

    def sitemap_handler(self, response: Response):
        pages = response.xpath("//x:loc/text()", namespaces=self.namespaces).getall()

        yield from response.follow_all(pages, callback=self.page_handler, dont_filter=True, meta={
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

        def get_area_unit():
            if "m²" in area:
                area_unit = "SQMT"
            else:
                area_unit = None

            return area_unit

        breadcrumb = response.xpath("//ol[contains(@class, 'breadcrumb')]/li[1]/a/text()").get()
        agent_url = response.xpath("//section[@class='advertiser-info__container']//a[@data-testid='official-store-redirect-link']/@href").get()
        area = response.xpath("normalize-space(//div[@data-testid='amenities-list']/p[@itemprop='floorSize']/span[@class='amenities-item-text'])").get()

        yield {
            "competence_date": datetime.now().strftime("%Y-%m-%d"),
            "listing_id": re.search(r"id-(\d+)/$", response.url).group(1),
            "listing_title": remove_whitespaces(response.xpath("//h1[contains(@class, 'description__title')]/text()").get()),
            "listing_description": remove_whitespaces(response.xpath("//p[@data-testid='description-content']/text()").get()),
            "property_type": get_property_type(),
            "listing_type": get_listing_type(),
            "reference_market": get_reference_market(),
            "location_description": remove_whitespaces(response.xpath("//div[@class='address-info-container']//p[contains(@class, 'address-info-value')]/text()").get()),
            "location_region": None,
            "location_province": None,
            "location_city": None,
            "location_zip": None,
            "locaiton_neighborhood": None,
            "location_street": None,
            "location_street_n": None,
            "location_lon": None,
            "location_lat": None,
            "area_unit": get_area_unit() if area else None,
            "area_value": re.search(r"^(\d+)", area).group(1) if area else None,
            "bedrooms": response.xpath("normalize-space(//div[@data-testid='amenities-list']/p[@itemprop='numberOfRooms']/span[@class='amenities-item-text'])").get(),
            "bathrooms": response.xpath("normalize-space(//div[@data-testid='amenities-list']/p[@itemprop='numberOfBathroomsTotal']/span[@class='amenities-item-text'])").get(),
            "floor": response.xpath("normalize-space(//div[@data-testid='amenities-list']/p[@itemprop='floorLevel']/span[@class='amenities-item-text'])").get(),
            "total_floors": None,
            "amenities_list": ", ".join(response.xpath("//div[@data-testid='amenities-list']/p/span[@class='amenities-item-text']/text()").getall()),
            "listing_date": remove_whitespaces(response.xpath("(//div[@data-testid='info-date']/span[@data-testid='listing-created-date']/text())[2]").get()),
            "listing_status": None,
            "agent_id": re.search(r"/(\d+)/$", agent_url).group(1),
            "agent_url": agent_url,
            "price": re.search(r"([\d,\.]+)$", response.xpath("//p[@data-testid='price-info-value']/text()").get()).group(1).replace(".", ""),
            "imageurl": re.search(r"(https?://[^\s,]+)", response.xpath("//ul[@data-testid='carousel-photos']/li//img[@srcset]/@srcset").get()).group(1),
            "itemurl": response.url,
        }
