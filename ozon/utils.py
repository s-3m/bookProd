import os

from ozon_api import get_items_list, separate_records_to_client_id, start_push_to_ozon
from loguru import logger


def logger_filter(record):
    return record["module"] == "ozon_api"


def all_stocks_to_zero(prefix):
    print(os.getenv("MSK_CLIENT_ID_2146613"))
    logger.info("Start to search a stocks")
    items = get_items_list(
        prefix=prefix, visibility="VISIBLE", for_parse_sample=False, get_stocks=True
    )
    ready_items_list = [
        {"seller_id": i["seller_id"], "article": i["offer_id"], "stock": "0"}
        for i in items
        if i["stocks"] and i["stocks"][0]["present"] != 0
    ]

    ready_items_list = separate_records_to_client_id(ready_items_list)
    start_push_to_ozon(ready_items_list, prefix, update_price=False)
    logger.success("Stocks set to 0!")


if __name__ == "__main__":
    for i in ["mg", "mdk", "chit_gor", "msk"]:
        all_stocks_to_zero(prefix=i)
