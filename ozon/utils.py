import os
from concurrent.futures import ThreadPoolExecutor

from .ozon_api import (
    get_items_list,
    separate_records_to_client_id,
    start_push_to_ozon,
    Ozon,
)
from loguru import logger


def logger_filter(record):
    return record["module"] == "ozon_api"


def all_stocks_to_zero(prefix):
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


def change_warehouse(ozon: Ozon):
    stocks_in_old_warehouse = ozon.get_info_stock()
    logger.info(f"start {ozon.client_id}")
    warehouses_list = ozon.get_warehouse_list()
    new_warehouse_id = None
    old_warehouse_id = None
    for i in warehouses_list:
        if (
            "набережный проезд" in i["name"].lower()
        ):  # Тут нужно заменить значение переменных new_warehouse_id и old_warehouse_id на требуемые
            new_warehouse_id = i["warehouse_id"]
        else:
            old_warehouse_id = i["warehouse_id"]

    prepare_data = []
    for i in stocks_in_old_warehouse:
        prepare_data.append(
            {
                "article": i["article"],
                "stock": i["stock"],
                "warehouse_id": new_warehouse_id,
            }
        )
        prepare_data.append(
            {
                "article": i["article"],
                "stock": 0,
                "warehouse_id": old_warehouse_id,
            }
        )

    ozon.update_stock(prepare_data, update_price=False, to_change_warehouse=True)


def start_changes_warehouses(prefix) -> None:
    """
    Достаточно передать сюда префикс магазина
    :param prefix: префикс магазина
    :return: None
    """
    shop_list = []
    ready_result = []
    for key, value in os.environ.items():
        if key.startswith(prefix.upper()):
            new_shop_flag = True if key.split("_")[-2] == "PRX" else False
            shop_list.append((key.split("_")[-1], value, new_shop_flag))

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for item in shop_list:
            ozon = Ozon(client_id=item[0], api_key=item[1], prefix=prefix, prx=item[2])
            task = executor.submit(change_warehouse, ozon)
            futures.append(task)
        for i in futures:
            try:
                ready_result.extend(i.result())
            except Exception as e:
                logger.critical(e)


if __name__ == "__main__":
    for i in ["mg", "mdk", "chit_gor", "msk"]:
        logger.info(f"Start {i}")
        all_stocks_to_zero(prefix=i)
