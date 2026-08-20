import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal
import pickle
import gzip

from loguru import logger

from utils import check_religions_book
from wb.wb_api import Wildberries


def create_local_db(data, shop: Literal["IBRA", "SRG"]):
    save_path = Path(__file__).parent.parent / f"{shop}_wb_db.pkl.gz"
    with gzip.open(save_path, "wb") as f:
        pickle.dump(data, f)


def load_local_db(shop: Literal["IBRA", "SRG"]):
    load_path = Path(__file__).parent.parent / f"{shop}_wb_db.pkl.gz"
    with gzip.open(load_path, "rb") as f:
        data = pickle.load(f)
    return data


def get_all_items_from_wb(wb: Wildberries, item_filter="religions"):
    all_items = wb.get_items_list()
    if item_filter == "religions":
        all_items = [
            item for item in all_items if not check_religions_book(item["title"])
        ]
    return all_items


def separate_items_to_store(
    items_list: list[dict], prefix: Literal["mg", "chit_gor", "msk", "mdk", "b24"]
) -> list[tuple[str, str]]:
    result = []
    article_prefix = {
        "mg": "",
        "chit_gor": "",
        "msk": "m",
        "mdk": "a",
        "b24": "k",
    }
    start_symbol = article_prefix[prefix]

    for item in items_list:
        if prefix == "chit_gor":
            if item["vendorCode"][0].isdigit():
                result.append((item["vendorCode"], item["sizes"][0]["chrtID"]))
        else:
            if item["vendorCode"].startswith(start_symbol):
                result.append((item["vendorCode"], item["sizes"][0]["chrtID"]))

    return result


def prepare_to_daily_parse(
    prefix: Literal["mg", "chit_gor", "msk", "mdk", "b24"]
) -> list[dict]:
    shop_list = [
        (k.split("_")[-1], v) for k, v in os.environ.items() if "WB_TOKEN" in k
    ]
    ready_data = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        if prefix == "chit_gor":
            # Запускаем ВСЕ задачи параллельно, не дожидаясь результата в цикле
            futures = {
                shop: executor.submit(get_all_items_from_wb, Wildberries(token), item_filter="religions")
                for shop, token in shop_list
            }
            # Теперь собираем результаты — задачи уже выполняются одновременно
            shop_items_map = {}
            for shop, future in futures.items():
                items = future.result()
                create_local_db(items, shop)
                shop_items_map[shop] = items
        else:
            # load_local_db тоже можно выполнять параллельно
            futures = {
                shop: executor.submit(load_local_db, shop=shop)
                for shop, _ in shop_list
            }
            shop_items_map = {shop: future.result() for shop, future in futures.items()}

    # Формирование ready_data — без потоков, чистая обработка данных
    for shop, items in shop_items_map.items():
        separated_items = separate_items_to_store(items_list=items, prefix=prefix)
        shop_data = [
            {
                "article": article,
                "stock": "",
                "price": "",
                "seller_id": f"wb_{shop}",
                "marketplace": "wb",
                "chrtID": chrt_id,
                "link": None,
                "shop": shop,
            }
            for article, chrt_id in separated_items
        ]
        ready_data.extend(shop_data)

    return ready_data

def separate_to_wb_cabinet(books_data) -> dict[str, list[dict]]:
    separated_data = {}
    for book in books_data:
        separated_data.setdefault(book["shop"], []).append(book)
    return separated_data

def push_stock_to_wb(items_list: list[dict]):
    separated_data = separate_to_wb_cabinet(items_list)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for shop_name, data in separated_data.items():
            wb_api = os.getenv(f"WB_TOKEN_{shop_name.upper()}")
            if not wb_api:
                logger.error(f"WB_TOKEN not found for shop {shop_name}")
                continue
            wb = Wildberries(wb_api)
            logger.info(f"Start pushing items to WB --- {shop_name.upper()} ---")
            futures[shop_name] = executor.submit(wb.update_stocks, data)

        for shop_name, future in futures.items():
            try:
                future.result()
            except Exception:
                logger.exception(
                    f"Failed to push stocks to WB --- {shop_name.upper()} ---"
                )


def reset_stocks_to_zero(
    prefix: Literal["mg", "chit_gor", "msk", "mdk", "b24"],
    all_books=True,
    religions=False,
    shop: Literal["IBRA", "SRG"] = "ibra",
):
    """
    Скинуть остатки конкретного магазина на 0 (вывести из продажи). Можно вывести все книги конкретного магазина,
    либо религиозные книги во всех магазинах, в соответствии с переданными параметрами.
    :param prefix:
    :param all_books:
    :param religions:
    :param shop: Ибрагим или Сергей
    :return:
    """
    all_items_from_db = load_local_db(shop=shop)

    if all_books:
        shop_items = separate_items_to_store(
            items_list=all_items_from_db, prefix=prefix
        )
        zero_stocks_list = [
            {
                "article": item[0],
                "stock": "0",
                "price": "",
                "seller_id": "",
                "marketplace": "wb",
                "chrtID": item[1],
                "link": None,
                "shop": shop
            }
            for item in shop_items
        ]
        push_stock_to_wb(items_list=zero_stocks_list)

    if religions:
        religin_books = []
        for item in all_items_from_db:
            if check_religions_book(item["title"]):
                religin_books.append(
                    {
                        "article": item["vendorCode"],
                        "stock": "0",
                        "price": "",
                        "seller_id": "",
                        "marketplace": "wb",
                        "chrtID": item["sizes"][0]["chrtID"],
                        "link": None,
                    }
                )
        return religin_books
    return None


if __name__ == "__main__":
    reset_stocks_to_zero(prefix="chit_gor")
