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


def skip_archive_process(ozon: Ozon):
    archived_list = ozon.get_items_list("ARCHIVED")
    items_info = ozon.get_items_info(archived_list)
    for i in items_info:
        i["stock"] = "0"
    ready_data_for_push = [
        {
            "attributes": [
                {
                    "id": 4180,
                    "values": [{"value": f"archive_{i["offer_id"][:-2]}"}],
                },
                {
                    "id": 7,
                    "values": [{"value": "АСТ"}],
                },
                {
                    "id": 23273,
                    "values": [{"dictionary_value_id": "972842193", "value": "Проза"}],
                },
                {
                    "id": 4182,
                    "values": [{"value": "Нет автора"}],
                },
                {"id": 4184, "values": [{"value": "1234567890"}]},
                {"id": 4191, "values": [{"value": "Автор рекомендует книгу."}]},
            ],
            "description_category_id": 200001483,  # Печатные книги, журналы, комиксы (с 2011 г.)
            "new_description_category_id": 200001483,
            "depth": 80,  # длина
            "dimension_unit": "mm",
            "height": 40,  # высота
            "images": [
                "https://disk.yandex.ru/i/SjGVhWbFaEp6Kw",
            ],
            "primary_image": "https://disk.yandex.ru/i/SjGVhWbFaEp6Kw",
            "old_price": i["price"],
            "price": i["price"],
            "name": f"archive_{i["offer_id"][:-2]}",
            "offer_id": f"{i["offer_id"]}",
            "type_id": 971445087,  # Печатная книга
            "vat": "0",
            "weight": 200,  # вес
            "weight_unit": "g",
            "width": 100,  # ширина
        }
        for i in items_info
    ]
    ozon.add_items(ready_data_for_push)
    ozon.update_stock(items_info, update_price=False)
    ozon.change_articles([i["offer_id"] for i in items_info])


def skip_all_archive_items(prefix):
    shop_list = []
    for key, value in os.environ.items():
        if key.startswith(prefix.upper()):
            new_shop_flag = True if key.split("_")[-2] == "PRX" else False
            shop_list.append((key.split("_")[-1], value, new_shop_flag))
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for item in shop_list:
            ozon = Ozon(client_id=item[0], api_key=item[1], prefix=prefix, prx=item[2])
            task = executor.submit(skip_archive_process, ozon)
            futures.append(task)


def get_period_ozon_real_fees(ozon: Ozon, start_period, end_period):
    response = ozon.get_finance_statement(
        period_from=start_period, period_to=end_period
    )
    order_amount = 0
    return_amount = 0
    commission_amount = 0
    delivery_amount = 0
    agent_amount = 0
    services_amount = 0
    for period in response["cash_flows"]:
        order_amount += period["orders_amount"]
        return_amount += period["returns_amount"]
        commission_amount += period["commission_amount"]

    for period in response["details"]:
        for i in period["delivery"]["delivery_services"]["items"]:
            if i.get("name") == "MarketplaceServiceItemDropoff":
                delivery_amount += i["price"]
            elif i.get("name") == "MarketplaceServiceItemDirectFlowLogisticSum":
                delivery_amount += i["price"]
            elif i.get("name") == "MarketplaceServiceItemRedistributionDropoff":
                agent_amount += i["price"]
            elif i.get("name") == "MarketplaceServiceItemRedistributionLastMileCourier":
                agent_amount += i["price"]

        if period.get("return"):
            delivery_amount += period.get("return").get("return_services").get("total")

        for i in period["others"]["items"]:
            if i.get("name") == "MarketplaceRedistributionOfAcquiringOperation":
                agent_amount += i["price"]

        services_amount += period["services"]["total"]

    tax_summary = (
        return_amount
        + commission_amount
        + delivery_amount
        + agent_amount
        + services_amount
    )

    fees_percent = round(abs(tax_summary * 100 / order_amount))
    return fees_percent


def start_monthly_fees_calculate(prefix, start_period, end_period) -> None:
    """
    Достаточно передать сюда префикс магазина
    :param prefix: префикс магазина
    :param start_period: начало периода
    :param end_period: окончание периода
    :return: None
    """
    shop_list = []
    ready_result = []
    for key, value in os.environ.items():
        if key.startswith(prefix.upper()):
            new_shop_flag = True if key.split("_")[-2] == "PRX" else False
            shop_list.append((key.split("_")[-1], value, new_shop_flag))

    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = []
        for item in shop_list:
            ozon = Ozon(client_id=item[0], api_key=item[1], prefix=prefix, prx=item[2])
            task = executor.submit(
                get_period_ozon_real_fees, ozon, start_period, end_period
            )
            futures.append(task)
        for i in futures:
            try:
                ready_result.append(i.result())
            except Exception as e:
                logger.critical(e)
        print(f"{prefix} - {round(sum(ready_result)/len(ready_result))}")


# if __name__ == "__main__":
#     start_monthly_fees_calculate("chit_gor", "01-09-2025", "30-09-2025")
#     start_monthly_fees_calculate("mdk", "01-09-2025", "30-09-2025")
#     start_monthly_fees_calculate("msk", "01-09-2025", "30-09-2025")
#     start_monthly_fees_calculate("mg", "01-09-2025", "30-09-2025")
