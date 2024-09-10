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
        self.worksheet.write(self.row, 5, item.get("listing_type"))
        self.worksheet.write(self.row, 6, item.get("reference_market"))
        self.worksheet.write(self.row, 7, item.get("location_description"))
        self.worksheet.write(self.row, 8, item.get("location_region"))
        self.worksheet.write(self.row, 9, item.get("location_province"))
        self.worksheet.write(self.row, 10, item.get("location_city"))
        self.worksheet.write(self.row, 11, item.get("location_zip"))
        self.worksheet.write(self.row, 12, item.get("locaiton_neighborhood"))
        self.worksheet.write(self.row, 13, item.get("location_street"))
        self.worksheet.write(self.row, 14, item.get("location_street_n"))
        self.worksheet.write(self.row, 15, item.get("location_lon"))
        self.worksheet.write(self.row, 16, item.get("location_lat"))
        self.worksheet.write(self.row, 17, item.get("area_unit"))
        self.worksheet.write(self.row, 18, item.get("area_value"))
        self.worksheet.write(self.row, 19, item.get("bedrooms"))
        self.worksheet.write(self.row, 20, item.get("bathrooms"))
        self.worksheet.write(self.row, 21, item.get("floor"))
        self.worksheet.write(self.row, 22, item.get("total_floors"))
        self.worksheet.write(self.row, 23, item.get("amenities_list"))
        self.worksheet.write(self.row, 24, item.get("listing_date"))
        self.worksheet.write(self.row, 25, item.get("listing_status"))
        self.worksheet.write(self.row, 26, item.get("agent_id"))
        self.worksheet.write(self.row, 27, item.get("agent_url"))
        self.worksheet.write(self.row, 28, item.get("price"))
        self.worksheet.write(self.row, 29, item.get("imageurl"))
        self.worksheet.write(self.row, 30, item.get("itemurl"))

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
        self.worksheet.write("F1", "listing_type")
        self.worksheet.write("G1", "reference_market")
        self.worksheet.write("H1", "location_description")
        self.worksheet.write("I1", "location_region")
        self.worksheet.write("J1", "location_province")
        self.worksheet.write("K1", "location_city")
        self.worksheet.write("L1", "location_zip")
        self.worksheet.write("M1", "locaiton_neighborhood")
        self.worksheet.write("N1", "location_street")
        self.worksheet.write("O1", "location_street_n")
        self.worksheet.write("P1", "location_lon")
        self.worksheet.write("Q1", "location_lat")
        self.worksheet.write("R1", "area_unit")
        self.worksheet.write("S1", "area_value")
        self.worksheet.write("T1", "bedrooms")
        self.worksheet.write("U1", "bathrooms")
        self.worksheet.write("V1", "floor")
        self.worksheet.write("W1", "total_floors")
        self.worksheet.write("X1", "amenities_list")
        self.worksheet.write("Y1", "listing_date")
        self.worksheet.write("Z1", "listing_status")
        self.worksheet.write("AA1", "agent_id")
        self.worksheet.write("AB1", "agent_url")
        self.worksheet.write("AC1", "price")
        self.worksheet.write("AD1", "imageurl")
        self.worksheet.write("AE1", "itemurl")

    def close_spider(self, spider):
        self.workbook.close()
