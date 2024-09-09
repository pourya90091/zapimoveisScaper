import scrapy
from scrapy.http import Response
from pathlib import Path
from zapimoveisScaper import settings
import gzip
import shutil


TEMP_DIR = settings.BASE_DIR / "temp"

def handle_dir(path: str) -> None:
    """Ensures that the directory exists.

    Parameters:
        path: Path to a directory.
    """

    Path(path).mkdir(parents=True, exist_ok=True)

handle_dir(TEMP_DIR)

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
    base_url = "https://www.zapimoveis.com.br/"

    def start_requests(self):
        urls = [
            "https://www.zapimoveis.com.br/sitemap_development_resultpage_index.xml"
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

        yield scrapy.Request(url=local_file_url, callback=self.sitemap_handler, meta={"playwright": True})

    def sitemap_handler(self, response: Response):
        pages = response.xpath("//x:loc/text()", namespaces=self.namespaces).getall()

        yield from response.follow_all(pages, callback=self.page_handler, meta={"playwright": True})

    def page_handler(self, response: Response):
        property_xpath = "//div[@data-type='STANDARD']"
