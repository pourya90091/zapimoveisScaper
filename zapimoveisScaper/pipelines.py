# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from xlsxwriter import Workbook
from zapimoveisScaper import settings
from pathlib import Path

EXPORT_DIR = settings.BASE_DIR / "export"
Path(EXPORT_DIR).mkdir(parents=True, exist_ok=True) # Ensures that EXPORT_DIR exists.


class ZapimoveisscaperPipeline:
    def __init__(self):
        self.workbook = None
        self.worksheet = None
        self.row = 1

    def process_item(self, item, spider):
        self.worksheet.write(self.row, 0, item.get("competence_date"))
        self.worksheet.write(self.row, 1, item.get("listing_id"))
        self.worksheet.write(self.row, 2, item.get("listing_title"))
        self.worksheet.write(self.row, 3, item.get("listing_description"))
        self.worksheet.write(self.row, 4, item.get("property_type"))

        self.row += 1

        return item

    def open_spider(self, spider):
        file_name = f"export.xlsx"
        self.workbook = Workbook(f"{EXPORT_DIR}/{file_name}")
        self.worksheet = self.workbook.add_worksheet()
        self.worksheet.write("A1", "competence_date")
        self.worksheet.write("B1", "listing_id")
        self.worksheet.write("C1", "listing_title")
        self.worksheet.write("D1", "listing_description")
        self.worksheet.write("E1", "property_type")

    def close_spider(self, spider):
        self.workbook.close()
