import os
from typing import Literal

from dotenv import load_dotenv
from loguru import logger

from wb.wb_api import Wildberries


def separate_items_to_store(
    wb: Wildberries, prefix: Literal["mg", "chit_gor", "msk", "mdk"]
) -> list[tuple[str, str]]:
    result = []
    article_prefix = {
        "mg": "",
        "chit_gor": "",
        "msk": "m",
        "mdk": "a",
    }
    start_symbol = article_prefix[prefix]
    all_items = wb.get_items_list()

    for item in all_items:
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
    wb_api = os.getenv("WB_TOKEN")
    wb = Wildberries(wb_api)
    items_articles = separate_items_to_store(wb=wb, prefix=prefix)
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
        for i in items_articles
    ]
    return ready_data


def push_stock_to_wb(items_list: list[dict]):
    wb_api = os.getenv("WB_TOKEN")
    wb = Wildberries(wb_api)
    logger.info(f"Start pushing items to WB")
    wb.update_stocks(items_list)
    logger.info(f"End pushing items to WB")
