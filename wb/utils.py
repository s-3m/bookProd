import os
from pathlib import Path
from typing import Literal
import pickle
import gzip

from loguru import logger
from wb.wb_api import Wildberries


def create_local_db(data):
    save_path = Path(__file__).parent.parent / "wb_db.pkl.gz"
    with gzip.open(save_path, "wb") as f:
        pickle.dump(data, f)


def load_local_db():
    load_path = Path(__file__).parent.parent / "wb_db.pkl.gz"
    with gzip.open(load_path, "rb") as f:
        data = pickle.load(f)
    return data


def get_all_items_from_wb(wb: Wildberries):
    all_items = wb.get_items_list()
    return all_items


def separate_items_to_store(
    items_list: list[dict], prefix: Literal["mg", "chit_gor", "msk", "mdk"]
) -> list[tuple[str, str]]:
    result = []
    article_prefix = {
        "mg": "",
        "chit_gor": "",
        "msk": "m",
        "mdk": "a",
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
    prefix: Literal["mg", "chit_gor", "msk", "mdk"]
) -> list[dict]:
    if prefix == "chit_gor":
        wb_api = os.getenv("WB_TOKEN")
        wb = Wildberries(wb_api)
        all_items = get_all_items_from_wb(wb)
        create_local_db(all_items)
    else:
        all_items = load_local_db()

    separated_items = separate_items_to_store(items_list=all_items, prefix=prefix)
    ready_data = [
        {
            "article": i[0],
            "stock": "",
            "price": "",
            "seller_id": "",
            "marketplace": "wb",
            "chrtID": i[1],
            "link": None,
        }
        for i in separated_items
    ]
    return ready_data


def push_stock_to_wb(items_list: list[dict]):
    wb_api = os.getenv("WB_TOKEN")
    wb = Wildberries(wb_api)
    logger.info(f"Start pushing items to WB")
    wb.update_stocks(items_list)
    logger.info(f"End pushing items to WB")
