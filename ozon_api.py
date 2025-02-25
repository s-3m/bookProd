import random

import requests
import os
from concurrent.futures import ThreadPoolExecutor
import time
from loguru import logger


def separate_records_to_client_id(books_records: list[dict]) -> dict[str, list[dict]]:
    result_dict = {}

    for book in books_records:
        if not result_dict.get(book["seller_id"]):
            result_dict[book["seller_id"]] = []
        result_dict[book["seller_id"]].append(book)

    return result_dict


class Ozon:
    def __init__(self, client_id: str, api_key: str):
        self.client_id = client_id
        self.api_key = api_key
        self.host = "https://api-seller.ozon.ru"
        self.errors = {self.client_id: []}

        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def _get_warehouse_id(self):
        response = requests.post(f"{self.host}/v1/warehouse/list", headers=self.headers)
        warehouses_list: list[dict] = response.json().get("result")
        for i in warehouses_list:
            if i["name"] == "Волгоградка":
                return int(i["warehouse_id"])

    def update_stock(self, item_list: list[dict]):
        warehouse_id = self._get_warehouse_id()
        stocks_list = [
            {
                "offer_id": i["article"],
                "stock": int(i["stock"]),
                "warehouse_id": warehouse_id,
            }
            for i in item_list
        ]

        for item in range(0, len(item_list), 100):
            body = {
                "stocks": stocks_list[item : item + 100],
            }

            response = requests.post(
                f"{self.host}/v2/products/stocks",
                headers=self.headers,
                json=body,
            )
            results = response.json().get("result")
            for result in results:
                if result.get("errors"):
                    self.errors[self.client_id].append(result)
            time.sleep(30)
        if self.errors[self.client_id]:
            logger.warning(self.errors)


def start_push_to_ozon(separate_records: dict[str, list[dict]]):
    with ThreadPoolExecutor(max_workers=20) as executor:
        for item in separate_records:
            seller_id = item
            api_key = os.getenv(f"CLIENT_ID_{seller_id}")
            ozon = Ozon(client_id=seller_id, api_key=api_key)
            executor.submit(ozon.update_stock, separate_records[item])


# if __name__ == "__main__":
#     load_dotenv(".env")
#     from utils import give_me_sample
#
#     sample = give_me_sample("bb/source/every_day", "bb")
#     for i in sample:
#         i["stock"] = random.randint(3, 8)
#
#     sep_rec = separate_records_to_client_id(sample)
#     # ozon = Ozon("2173296", "4fdc3d57-5f21-416f-b032-d7fce2332d90")
#     # ozon.update_stock(sep_rec["2173296"])
#
#     #
#     start_push_to_ozon(sep_rec)
