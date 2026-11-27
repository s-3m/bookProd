import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Literal
import pickle
import gzip
import gc
from dotenv import load_dotenv
import schedule
from loguru import logger

from utils import check_religions_book
from wb.wb_api import Wildberries


def create_local_db(data, shop: Literal["IBRA", "SRG"]):
    """Атомарно сохраняет архив: читатели увидят либо старую, либо новую версию."""
    save_path = Path(__file__).parent.parent / f"{shop}_wb_db.pkl.gz"
    temp_path = save_path.with_name(f"{save_path.name}.{os.getpid()}.tmp")

    try:
        with gzip.open(temp_path, "wb", compresslevel=1) as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(temp_path, save_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def load_local_db(shop: Literal["IBRA", "SRG"]):
    """Загружает один архив целиком. Вызывать последовательно, не пачкой потоков."""
    load_path = Path(__file__).parent.parent / f"{shop}_wb_db.pkl.gz"
    if not load_path.exists():
        raise FileNotFoundError(f"Не найден архив WB: {load_path}")

    with gzip.open(load_path, "rb") as f:
        return pickle.load(f)


def get_all_items_from_wb(wb: Wildberries, item_filter="religions"):
    """Загружает карточки, удаляет дубли nmID и фильтрует каталог."""
    raw_items = wb.get_items_list()

    # Сохраняем прежнее поведение: при дублях nmID остаётся последняя карточка.
    items_by_id = {}
    for item in raw_items:
        items_by_id[item["nmID"]] = item

    # После построения индекса исходный список больше не нужен.
    del raw_items

    # Удаляем неподходящие записи прямо из индекса, не создавая второй
    # промежуточный список карточек.
    rejected_ids = []
    for nm_id, item in items_by_id.items():
        if item["subjectName"] != "Книги":
            rejected_ids.append(nm_id)
        elif item_filter == "religions" and check_religions_book(item["title"]):
            rejected_ids.append(nm_id)

    for nm_id in rejected_ids:
        del items_by_id[nm_id]

    result = list(items_by_id.values())
    del items_by_id, rejected_ids
    return result


def separate_items_to_store(
    items_list: list[dict], prefix: Literal["mg", "chit_gor", "msk", "mdk", "b24"]
) -> list[tuple[str, str, str]]:
    result = []
    article_prefix = {
        "mg": "g",
        "chit_gor": "",
        "msk": "m",
        "mdk": "a",
        "b24": "k",
    }
    start_symbol = article_prefix[prefix]

    for item in items_list:
        if prefix == "chit_gor":
            if item["vendorCode"][0].isdigit():
                result.append(
                    (item["vendorCode"], item["sizes"][0]["chrtID"], item["nmID"])
                )
        else:
            if item["vendorCode"].startswith(start_symbol):
                result.append(
                    (item["vendorCode"], item["sizes"][0]["chrtID"], item["nmID"])
                )

    return result


def _append_daily_records(
    ready_data: list[dict], items: list[dict], prefix: str, shop: str
):
    """Добавляет записи нужного магазина сразу в итоговый список, без списка-кортежей."""
    article_prefix = {
        "mg": "g",
        "chit_gor": "",
        "msk": "m",
        "mdk": "a",
        "b24": "k",
    }
    start_symbol = article_prefix[prefix]
    added = 0

    for item in items:
        vendor_code = item["vendorCode"]
        if prefix == "chit_gor":
            matches = vendor_code[0].isdigit()
        else:
            matches = vendor_code.startswith(start_symbol)

        if not matches:
            continue

        ready_data.append(
            {
                "article": vendor_code,
                "stock": "0",
                "price": "",
                "seller_id": f"wb_{shop}",
                "marketplace": "wb",
                "chrtID": item["sizes"][0]["chrtID"],
                "nmID": item["nmID"],
                "link": None,
                "shop": shop,
            }
        )
        added += 1

    return added


def prepare_to_daily_parse(
    prefix: Literal["mg", "chit_gor", "msk", "mdk", "b24"]
) -> list[dict]:
    """Локальные архивы читает последовательно, каталоги WB для ЧГ скачивает параллельно."""
    shop_list = [
        (k.split("_")[-1], v) for k, v in os.environ.items() if "WB_TOKEN" in k
    ]

    ready_data = []

    if prefix == "chit_gor":
        # Сетевые запросы к WB выполняем параллельно: они самые медленные.
        # Но не складываем все каталоги в общий shop_items_map. Каждый worker
        # скачивает каталог, атомарно сохраняет архив и превращает карточки
        # в компактные записи. В основной поток возвращаются только эти записи.
        def download_save_and_prepare(shop_token):
            shop, token = shop_token
            logger.info(f"WB: загрузка каталога кабинета {shop}")
            items = get_all_items_from_wb(Wildberries(token), item_filter="religions")
            items_count = len(items)
            create_local_db(items, shop)

            shop_records = []
            added = _append_daily_records(shop_records, items, prefix, shop)
            del items
            gc.collect()
            return shop, items_count, added, shop_records

        # Ограничиваем число одновременных полных каталогов, чтобы не вернуть
        # прежний пик RAM. Три параллельных кабинета сохраняют выигрыш по сети.
        max_workers = min(3, len(shop_list)) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(download_save_and_prepare, shop_token): shop_token[0]
                for shop_token in shop_list
            }
            for future in as_completed(futures):
                shop = futures.pop(future)
                try:
                    result_shop, items_count, added, shop_records = future.result()
                    ready_data.extend(shop_records)
                    logger.info(
                        f"WB: кабинет {result_shop}: карточек в архиве {items_count}, "
                        f"позиций для парсинга {added}"
                    )
                    del shop_records
                except Exception:
                    logger.exception(f"WB: ошибка загрузки кабинета {shop}")
                    raise

    else:
        # Ключевое изменение: не распаковываем все gzip/pickle архивы параллельно.
        # Один архив загружается, преобразуется в компактные записи и освобождается.
        for shop, _ in shop_list:
            logger.info(f"WB: чтение локального архива кабинета {shop}")
            items = load_local_db(shop=shop)
            added = _append_daily_records(ready_data, items, prefix, shop)
            logger.info(
                f"WB: кабинет {shop}: карточек в архиве {len(items)}, "
                f"позиций для парсинга {added}"
            )
            del items
            gc.collect()

    logger.info(f"WB: всего подготовлено позиций для {prefix}: {len(ready_data)}")
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

            # Выставляем остаток 3 шт если книг в магазине больше чем 3
            for book in data:
                try:
                    if int(book["stock"]) > 3:
                        book["stock"] = 3
                except ValueError:
                    book["stock"] = 0

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


# def calculate_price(price):
#     price = int(price)
#     if price < 100:
#         return 1099
#     elif price < 250:
#         return 1299
#     elif price < 350:
#         return 1599
#     else:
#         return max(1599, price * 4)


def calculate_prices(items_list: list[dict]):
    """
    Обновляем цены по какой-либо логике. Логику прописываем прям тут
    :param items_list: список из словарей, полученных методом wb api - get_items_list_with_prices()
    :return: список из словарей [{"nmID": "123456789", "price": "123456"}]
    """
    result = []
    for item in items_list:
        result.append(
            {"nmID": item["nmID"], "price": round(item["sizes"][0]["price"], 1)}
        )
    return result


def push_price_to_wb(items_list: list[dict]):
    item_with_stock = []
    for i in items_list:
        try:
            if i["price"] not in ("", " ", None):
                item_with_stock.append(i)
            else:
                logger.warning(i)
        except Exception as e:
            logger.error(f"{i} --- {e}")
    logger.info(f"TOTAL ITEMS FOR PRICE UPDATE - {len(item_with_stock)}")
    separated_data = separate_to_wb_cabinet(item_with_stock)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for shop_name, data in separated_data.items():
            body_for_api = [
                {"nmID": i["nmID"], "price": round(i["price"] * 1.10, 1)} for i in data
            ]
            wb_api = os.getenv(f"WB_TOKEN_{shop_name.upper()}")
            if not wb_api:
                logger.error(f"WB_TOKEN not found for shop {shop_name}")
                continue
            wb = Wildberries(wb_api)
            logger.info(f"Start set prices to WB --- {shop_name.upper()} ---")
            futures[shop_name] = executor.submit(wb.update_price, body_for_api)

        for shop_name, future in futures.items():
            try:
                future.result()
            except Exception:
                logger.exception(
                    f"Failed to set prices to WB --- {shop_name.upper()} ---"
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
                "nmID": item[2],
                "link": None,
                "shop": shop,
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


def add_characteristics(env_name, characteristics_list: list[dict]):
    """
    :param env_name: имя переменной окружения
    :param characteristics_list: список словарей характеристик формата [{id: int, value: [str]}]
    :return:
    """
    api_key = os.getenv(env_name)
    wb = Wildberries(api_key)
    items = get_all_items_from_wb(wb, item_filter="none")
    print(len(items))
    need_items = []
    for item in items:
        try:
            if not item.get("characteristics"):
                item["characteristics"] = []
                item["characteristics"].extend(characteristics_list)
                need_items.append(item)
            else:
                chars_id = []
                for i in item["characteristics"]:
                    chars_id.append(i["id"])
                for i in characteristics_list:
                    if i["id"] not in chars_id:
                        item["characteristics"].append(i)
                        need_items.append(item)

        except Exception as e:
            logger.warning(f"{e} ---- {item}")
    print(len(need_items))
    wb.update_cards(need_items)


if __name__ == "__main__":
    load_dotenv("../.env")
    wb = Wildberries(os.getenv("WB_TOKEN_IBRA"))
    data = wb.get_items_list_with_prices()
    wb.update_price(
        [
            {"nmID": i["nmID"], "price": round(i["sizes"][0]["price"] * 1.1, 0)}
            for i in data
        ]
    )

    # Если остановлен ЧГ, тогда запускаем это в отдельном потоке для обновления БД по магазинам
    # def main():
    #     prepare_to_daily_parse(prefix="chit_gor")
    #
    # schedule.every().day.at("20:00").do(main)
    # while True:
    #     schedule.run_pending()

    # add_characteristics(
    #     env_name="WB_TOKEN_IBRA2",
    #     characteristics_list=[{"id": 15000001, "value": ["4901990000"]}],
    # )
