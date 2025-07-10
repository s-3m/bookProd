from typing import Literal

import pandas as pd
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
    max_discount = {"mg": 0.68, "msk": 0.75, "mdk": 0.77, "chit_gor": 0.80}

    def __init__(self, client_id: str, api_key: str, prefix: str, prx: bool = False):
        self.client_id = client_id
        self.api_key = api_key
        self.discount = self.max_discount[prefix]
        self.host = "https://api-seller.ozon.ru"
        self.errors = {self.client_id: []}
        self.tasks_id = []
        self.PRX = prx
        self.prx_list = self.get_proxies() if prx else None
        self.prefix = prefix

        self.headers = {
            "Client-Id": str(self.client_id),
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def get_proxies(self):
        prx = os.getenv("PRX")
        proxies = {
            "http": f"http://{prx}",
            "https": f"http://{prx}",
        }
        return proxies

    def add_items(self, item_list: list[dict]) -> list[dict] | None:

        ready_data_for_push = [
            {
                "attributes": [
                    {
                        "id": 4180,
                        "values": [{"value": i["Название"]}],
                    },
                    {"id": 9356, "values": [{"value": i["Тип обложки"]}]},
                    {"id": 4052, "values": [{"value": str(i["Тираж"])}]},
                    {
                        "id": 8862,
                        "values": [{"value": i["Возраст"] if i["Возраст"] else "0+"}],
                    },
                    {
                        "id": 9070,
                        "values": [
                            {"value": "true" if "18" in i["Возраст"] else "false"}
                        ],
                    },
                    {"id": 4191, "values": [{"value": i["description"]}]},
                    {"id": 4051, "values": [{"value": str(i["Страниц"])}]},
                    {"id": 4081, "values": [{"value": str(i["Год производства"])}]},
                    {
                        "id": 7,
                        "values": [{"value": p} for p in i["Издательство"].split(";")],
                    },
                    {"id": 8229, "values": [{"value": "Художественная литература"}]},
                    {
                        "id": 4182,
                        "values": [
                            {"value": i["author"] if i["author"] else "Нет автора"}
                        ],
                    },
                    {"id": 4184, "values": [{"value": str(i["ISBN"])}]},
                ],
                "description_category_id": 200001483,  # Печатные книги, журналы, комиксы (с 2011 г.)
                "depth": 80,  # длина
                "dimension_unit": "mm",
                "height": 40,  # высота
                "images": [i["Фото"]],
                "primary_image": "",
                "name": i["Название"],
                "offer_id": str(i["Артикул_OZ"]),
                "old_price": self._price_calculate(i["Цена"])["old_price"],
                "price": self._price_calculate(i["Цена"])["price"],
                "type_id": 971445081,  # проза других жанров
                "vat": "0",
                "weight": 200,  # вес
                "weight_unit": "g",
                "width": 100,  # ширина
                "stock": "4",
            }
            for i in item_list
        ]
        logger.info("Начал добавлять товары")
        items_stock = [
            {"article": str(i["Артикул_OZ"]), "stock": i["Наличие"]} for i in item_list
        ]
        for item in range(0, len(item_list), 100):
            response = requests.post(
                f"{self.host}/v3/product/import",
                headers=self.headers,
                json={
                    "items": ready_data_for_push[item : item + 100],
                },
                proxies=self.prx_list,
            )
            result = response.json().get("result")
            task_id = result.get("task_id")
            if task_id:
                self.tasks_id.append(task_id)
            else:
                print(result)
            time.sleep(2)
        print(self.tasks_id)
        time.sleep(300)
        error_articles = [i["article"] for i in self.check_tasks_status()]
        for item in items_stock:
            if error_articles:
                if item["article"] in error_articles:
                    items_stock.remove(item)

        return items_stock

    def check_tasks_status(self, tasks: list[str] = None) -> list[dict]:
        """Проверяем все таски на добавление товаров, возвращаем только ошибки в формате списка словарей (артикул - ошибка)"""
        logger.info("Начал проверять статусы загрузки")
        errors_list = []
        if tasks is None:
            tasks = self.tasks_id
        for task in tasks:
            response = requests.post(
                f"{self.host}/v1/product/import/info",
                headers=self.headers,
                json={"task_id": task},
                proxies=self.prx_list,
            )
            result = response.json().get("result")
            if result:
                result_items_list = result.get("items")
                for item in result_items_list:
                    article = item.get("offer_id")
                    errors = item.get("errors")
                    if errors:
                        for error in errors:
                            if error.get("level") == "error":
                                errors_list.append(
                                    {
                                        "article": article,
                                        "field": error["field"],
                                        "code": error["code"],
                                        "error": error["description"],
                                    }
                                )
        if errors_list:
            pd.DataFrame(errors_list).to_excel(
                f"error_list_{errors_list[0]["article"]}.xlsx"
            )
        return errors_list

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
                proxies=self.prx_list,
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
        response = requests.post(
            f"{self.host}/v1/warehouse/list",
            headers=self.headers,
            proxies=self.prx_list,
        )
        warehouses_list: list[dict] = response.json().get("result")
        for i in warehouses_list:
            if i["name"] == "Волгоградка":
                return int(i["warehouse_id"])
            elif "скот" in i["name"].lower() and i["status"] == "created":
                return int(i["warehouse_id"])
        return None

    def _price_calculate(self, input_price) -> dict:
        input_price = str(input_price)
        raw_price = round(float(input_price.replace(",", ".").replace("\xa0", "")), 2)
        if self.prefix == "mg":
            raw_price = raw_price - (raw_price * 0.15)
        # profit calculate
        profit = (raw_price * 50) / 100
        # Фиксированная сумма (обработка, доставка - 135р)
        fixed_margin = raw_price + profit + 135
        # Сумма с учетом минимальной комиссии за задержку отправлений (100р)
        additional_coef = 0
        if self.prefix == "chit_gor":
            additional_coef = 100
        elif self.prefix == "mdk":
            additional_coef = 50
        price_with_delay_tax = fixed_margin + additional_coef
        # Сумма с учетом комиссии озон и эквайринга (32,5% и 2%)
        price_with_main_tax = price_with_delay_tax * 34.5 / 65.5 + price_with_delay_tax
        # Конечная сумма с учётом акции 15%
        finish_price = round(price_with_main_tax * 15 / 85 + price_with_main_tax, 0)

        if finish_price < 999:
            finish_price = 999
        old_price = finish_price * 2
        min_price = finish_price * self.discount
        return {
            "price": str(finish_price),
            "old_price": str(old_price),
            "min_price": str(min_price),
        }

    def update_price(self, item_list: list[dict]) -> None:
        list_for_price_update = []
        for i in item_list:
            if i["price"] is not None:
                if not i["price"].isdigit():
                    logger.warning(i)
                    continue

                prices_dict = self._price_calculate(i["price"])

                item_body = {
                    "offer_id": i["article"],
                    "old_price": str(prices_dict["old_price"]),
                    "price": str(prices_dict["price"]),
                    "min_price": str(prices_dict["min_price"]),
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
                    proxies=self.prx_list,
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
                    proxies=self.prx_list,
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

    def _prepare_for_sample(
        self, raw_data: list[dict], for_parse_sample: bool = True
    ) -> tuple[list, list]:
        ready_data = []
        wrong_article = []
        if for_parse_sample:
            for item in raw_data:
                if item["offer_id"].endswith(".0"):
                    ready_data.append(
                        {"Артикул": item["offer_id"], "seller_id": self.client_id}
                    )
                else:
                    wrong_dict = {"article": item["offer_id"], "stock": 0}
                    wrong_article.append(wrong_dict)
        else:
            for item in raw_data:
                ready_data.append(
                    {"Артикул": item["offer_id"], "seller_id": self.client_id}
                )
        return ready_data, wrong_article

    def get_items_list(self, visibility, for_parse_sample=True):
        result = []
        body = {
            "filter": {"visibility": visibility},
            "limit": 1000,
            "last_id": "",
        }
        while True:
            response = requests.post(
                f"{self.host}/v3/product/list",
                headers=self.headers,
                json=body,
                proxies=self.prx_list,
            )
            time.sleep(0.5)
            items_list = response.json().get("result").get("items")
            last_id = response.json().get("result").get("last_id")
            body["last_id"] = last_id
            if not items_list:
                break
            result.extend(items_list)
        ready_data = self._prepare_for_sample(result, for_parse_sample)
        if ready_data[1]:
            self.update_stock(ready_data[1], update_price=False)
        print(len(ready_data[0]))
        return ready_data[0]


def start_push_to_ozon(
    separate_records: dict[str, list[dict]], prefix: str, update_price=True
):
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for item in separate_records:
            seller_id = item
            for key, value in os.environ.items():
                prx = False
                if seller_id in key:
                    api_key = value
                    prx = True if key.split("_")[-2] == "PRX" else False

            ozon = Ozon(client_id=seller_id, api_key=api_key, prefix=prefix, prx=prx)
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
    prefix: str,
    visibility: str = "VISIBLE",
    for_parse_sample=True,
    get_stocks=False,
    shop_category: Literal["new", "old", "all"] = "all",
):
    shop_list = []
    ready_result = []
    for key, value in os.environ.items():
        prx = False
        if key.startswith(prefix.upper()):
            new_shop_flag = True if key.split("_")[-2] == "PRX" else False
            if shop_category == "new":
                if new_shop_flag:
                    shop_list.append((key.split("_")[-1], value, new_shop_flag))
            elif shop_category == "old":
                if not new_shop_flag:
                    shop_list.append((key.split("_")[-1], value, new_shop_flag))
            else:
                shop_list.append((key.split("_")[-1], value, new_shop_flag))

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for item in shop_list:
            ozon = Ozon(client_id=item[0], api_key=item[1], prefix=prefix, prx=item[2])
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
