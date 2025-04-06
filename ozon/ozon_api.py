import requests
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time

from dotenv import load_dotenv
from loguru import logger

load_dotenv(Path(__file__).parent.parent / ".env")


def separate_records_to_client_id(books_records: list[dict]) -> dict[str, list[dict]]:
    result_dict = {}

    for book in books_records:
        if not result_dict.get(book["seller_id"]):
            result_dict[book["seller_id"]] = []
        result_dict[book["seller_id"]].append(book)

    return result_dict


class Ozon:
    max_discount = {"mg": 0.68, "msk": 0.72, "mdk": 0.75, "chit_gor": 0.80}

    def __init__(self, client_id: str, api_key: str, prefix: str):
        self.client_id = client_id
        self.api_key = api_key
        self.discount = self.max_discount[prefix]
        self.host = "https://api-seller.ozon.ru"
        self.errors = {self.client_id: []}

        self.headers = {
            "Client-Id": str(self.client_id),
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def get_stocks(self, visible: str, *args) -> list[dict]:
        result = []
        cursor = ""
        while True:
            response = requests.post(
                f"{self.host}/v4/product/info/stocks",
                headers=self.headers,
                json={
                    "cursor": cursor,
                    "filter": {"visibility": visible},
                    "limit": 1000,
                },
            )
            items_list = response.json().get("items")
            if not items_list:
                break
            cursor = response.json()["cursor"]
            for item in items_list:
                item["seller_id"] = self.client_id
                result.append(item)
        return result

    def _get_warehouse_id(self):
        response = requests.post(f"{self.host}/v1/warehouse/list", headers=self.headers)
        warehouses_list: list[dict] = response.json().get("result")
        for i in warehouses_list:
            if i["name"] == "Волгоградка":
                return int(i["warehouse_id"])

    def update_price(self, item_list: list[dict]) -> None:
        list_for_price_update = []
        for i in item_list:
            if i["price"] is not None:
                if not i["price"].isdigit():
                    logger.warning(i)
                    continue
                raw_price = round(float(i["price"].replace(",", ".")))
                price = round(raw_price * 2.75)
                if price < 999:
                    price = 999
                old_price = price * 2
                min_price = price * self.discount

                item_body = {
                    "offer_id": i["article"],
                    "old_price": str(old_price),
                    "price": str(price),
                    "min_price": str(min_price),
                    "min_price_for_auto_actions_enabled": True,
                    "auto_action_enabled": "DISABLED",
                    "price_strategy_enabled": "DISABLED",
                    "vat": "0",
                }
                list_for_price_update.append(item_body)

        for item in range(0, len(list_for_price_update), 1000):
            body = {"prices": list_for_price_update[item : item + 1000]}
            try:
                response = requests.post(
                    "https://api-seller.ozon.ru/v1/product/import/prices",
                    headers=self.headers,
                    json=body,
                )
                results = response.json().get("result")
                for result in results:
                    if result["errors"]:
                        logger.warning(result["errors"])
            except Exception as e:
                logger.error(e)
                continue

    def update_stock(self, item_list: list[dict], update_price=True):
        if update_price:
            # Сначала обновляем цены, чтобы не вывелись товары со старыми ценами
            self.update_price(item_list)
        warehouse_id = self._get_warehouse_id()
        stocks_list = [
            {
                "offer_id": str(i["article"]),
                "stock": int(i["stock"]) if str(i["stock"]).isdigit() else 0,
                "warehouse_id": warehouse_id,
            }
            for i in item_list
        ]

        for item in range(0, len(item_list), 100):
            body = {
                "stocks": stocks_list[item : item + 100],
            }

            try:
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
            except Exception as e:
                logger.exception(e)
                continue
        if self.errors[self.client_id]:
            logger.warning(self.errors)

    def _prepare_for_sample(self, raw_data: list[dict], for_parse_sample: bool = True):
        ready_data = []
        if for_parse_sample:
            for item in raw_data:
                if item["offer_id"].endswith(".0"):
                    ready_data.append(
                        {"Артикул": item["offer_id"], "seller_id": self.client_id}
                    )
        else:
            for item in raw_data:
                ready_data.append(
                    {"Артикул": item["offer_id"], "seller_id": self.client_id}
                )
        return ready_data

    def get_items_list(self, visibility, for_parse_sample=True):
        result = []
        body = {
            "filter": {"visibility": visibility},
            "limit": 1000,
            "last_id": "",
        }
        while True:
            response = requests.post(
                f"{self.host}/v3/product/list", headers=self.headers, json=body
            )
            time.sleep(0.5)
            items_list = response.json().get("result").get("items")
            last_id = response.json().get("result").get("last_id")
            body["last_id"] = last_id
            if not items_list:
                break
            result.extend(items_list)
        ready_data = self._prepare_for_sample(result, for_parse_sample)
        print(len(ready_data))
        return ready_data


def start_push_to_ozon(
    separate_records: dict[str, list[dict]], prefix: str, update_price=True
):
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for item in separate_records:
            seller_id = item
            api_key = os.getenv(f"{prefix.upper()}_CLIENT_ID_{seller_id}")
            ozon = Ozon(client_id=seller_id, api_key=api_key, prefix=prefix)
            future = executor.submit(
                ozon.update_stock, separate_records[item], update_price
            )
            futures.append(future)

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.critical(f"Ошибка в задаче: {e}")


def get_items_list(
    prefix: str, visibility: str = "VISIBLE", for_parse_sample=True, get_stocks=False
):
    shop_list = []
    ready_result = []
    for key, value in os.environ.items():
        if key.startswith(prefix.upper()):
            shop_list.append((key.split("_")[-1], value))

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for item in shop_list:
            ozon = Ozon(client_id=item[0], api_key=item[1], prefix=prefix)
            task = executor.submit(
                (ozon.get_items_list if not get_stocks else ozon.get_stocks),
                visibility,
                for_parse_sample,
            )
            futures.append(task)
        for i in futures:
            try:
                ready_result.extend(i.result())
            except Exception as e:
                logger.critical(e)

    return ready_result


def archive_items_stock_to_zero(prefix):
    logger.info("Start check archived stocks")
    archive_items_list = get_items_list(
        prefix, visibility="ARCHIVED", for_parse_sample=False, get_stocks=True
    )
    ready_items_list = [
        {"seller_id": i["seller_id"], "article": i["offer_id"], "stock": "0"}
        for i in archive_items_list
        if i["stocks"] and i["stocks"][0]["present"] != 0
    ]
    if not ready_items_list:
        logger.success("Didn't find any stocks")
        return
    ready_items_list = separate_records_to_client_id(ready_items_list)
    start_push_to_ozon(ready_items_list, prefix, update_price=False)
    logger.success("Archived stocks set to 0!")


# if __name__ == "__main__":
# load_dotenv(".env")
# from utils import give_me_sample
#
# sample = give_me_sample("bb/source/every_day", "bb")
# for i in sample:
#     i["stock"] = random.randint(3, 8)
#
# sep_rec = separate_records_to_client_id(sample)
# ozon = Ozon("2173296", "4fdc3d57-5f21-416f-b032-d7fce2332d90")
# ozon.update_stock(sep_rec["2173296"])

#
# start_push_to_ozon(sep_rec)
# sample = archive_items_stock_to_zero("msk")
