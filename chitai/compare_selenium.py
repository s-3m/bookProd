import os
import sys

import requests
from dotenv import load_dotenv
import schedule
from loguru import logger
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import asyncio
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tg_sender import tg_send_files, tg_send_msg
from utils import give_me_sample, quantity_checker
from concurrent.futures import ThreadPoolExecutor
from ozon.ozon_api import (
    get_items_list,
    start_push_to_ozon,
    separate_records_to_client_id,
    archive_items_stock_to_zero,
)
from ozon.utils import logger_filter

pandas.io.formats.excel.ExcelFormatter.header_style = None

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru,en;q=0.9",
    "cache-control": "no-cache",
    # 'cookie': '__ddg1_=pvduRZr2PWRQfgBSlf6h; refresh-token=; tmr_lvid=03935c7450b807684f9dcd65334067b0; tmr_lvidTS=1719430836103; _ym_uid=1719430836919836282; _ym_d=1719430836; gdeslon.ru.__arc_domain=gdeslon.ru; gdeslon.ru.user_id=15254033-97f3-4a90-ae4b-740f08cf987d; _bge_ci=BA1.1.1122769529.1719430836; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; flocktory-uuid=0241fd6a-c187-4d15-b067-6914fd7ea4c9-9; adrcid=A6I49NSU-aGn_FnD2kzLfwA; adrcid=A6I49NSU-aGn_FnD2kzLfwA; stDeIdU=c130f130-3401-4ce5-9052-788e665fbea5; _ymab_param=NOyyliya_BgJ0VOb3JL1PA1-1gyIOZRewTIGnSgxe-t2ci28PM-AMDZfHAuZzH4TsvmxnZPeYOHvYxXI9RgNC-VeR8Q; chg_visitor_id=470bb97a-014e-404d-9c3d-a67245b92f38; adid=173169335285853; analytic_id=1731693382619506; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; origem=cityads; deduplication_cookie=cityads; deduplication_cookie=cityads; _ga_YVB4ZXMWPL=GS1.2.1731764757.1.1.1731765315.60.0.0; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MzIwODY5NzcsImlhdCI6MTczMTkxODk3NywiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjA0ODlmNzEzNzQ4NjRiNjYxMGNiOGJmZTlmNTY1M2UxOWRkYzQ3NTAwMmZlNTc1MzNlYWViMzk0MGJhOGZlZTkiLCJ0eXBlIjoxMH0.Tq3QOvFYkYoTpnAT4MDH51OIaSENrWDhAYCpNpXbFPg; _ga=GA1.1.1903252348.1719430836; _ym_isad=2; acs_3=%7B%22hash%22%3A%22768a608b20ce960ff29026da95a81203ec583ad1%22%2C%22nextSyncTime%22%3A1732005380055%2C%22syncLog%22%3A%7B%22224%22%3A1731918980055%2C%221228%22%3A1731918980055%2C%221230%22%3A1731918980055%7D%7D; acs_3=%7B%22hash%22%3A%22768a608b20ce960ff29026da95a81203ec583ad1%22%2C%22nextSyncTime%22%3A1732005380055%2C%22syncLog%22%3A%7B%22224%22%3A1731918980055%2C%221228%22%3A1731918980055%2C%221230%22%3A1731918980055%7D%7D; adrdel=1731918980080; adrdel=1731918980080; domain_sid=L3BsIrNkQonH7entRBvC0%3A1731918980423; clickCityAdsID=7MRZ235eurZf323; epn_click_id=7MRZ235eurZf323; tmr_detect=0%7C1731918987403; __ddg9_=85.198.105.3; partner_name=cityads; mindboxDeviceUUID=0660d298-673e-43fc-8993-474c6e6cd4c8; directCrm-session=%7B%22deviceGuid%22%3A%220660d298-673e-43fc-8993-474c6e6cd4c8%22%7D; __ddg10_=1731930396; __ddg8_=Kj0vHcaaytcn7CsU; _ga_W0V3RXZCPY=GS1.1.1731930384.6.1.1731930398.0.0.0; _ga_6JJPBGS8QY=GS1.1.1731930384.6.1.1731930398.0.0.0; _ga_LN4Z31QGF4=GS1.1.1731930376.7.1.1731930404.32.0.1425492556',
    "priority": "u=0, i",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "YaBrowser";v="24.10", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36",
}

DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.chitai-gorod.ru"
BASE_LINUX_DIR = "/media/source/chitai/every_day" if not DEBUG else "source/every_day"
logger.add(
    f"{BASE_LINUX_DIR}/log/error.log",
    format="{time} {level} {message}",
    level="ERROR",
)
logger.add(
    f"{BASE_LINUX_DIR}/log.json",
    level="WARNING",
    serialize=True,
    filter=logger_filter,
)
count = 1


def get_link_from_ajax(article):

    params = {
        "phrase": article[:-2],
        "customerCityId": "213",
    }
    request_count = 0
    while request_count < 4:
        try:
            resp = requests.get(
                "https://web-gate.chitai-gorod.ru/api/v2/search/product",
                headers=headers,
                params=params,
                timeout=15,
            )
            response = resp.json()
            link = response["included"][0]["attributes"].get("url")
            print()
            print(f"Нашёл ссылку {link}")
            print()
            return link
        except KeyError:
            continue
    return None


def get_main_data(book_item):
    try:
        if not book_item["link"]:
            i_link = get_link_from_ajax(book_item["article"])
            if not i_link:
                book_item["stock"] = "0"
                book_item["price"] = None
                return
            book_item["link"] = f"{BASE_URL}/{i_link}"

        response_text = ""
        for _ in range(5):
            response = requests.get(book_item["link"], headers=headers, timeout=30)
            if response.status_code == 404:
                book_item["stock"] = "0"
                book_item["price"] = None
                return
            if response.status_code == 200:
                response_text = response.text
                break

        soup = bs(response_text, "lxml")

        online_option = soup.find("div", class_="product-offer-price")

        if not online_option:
            book_item["stock"] = "0"
            book_item["price"] = None
            return

        stock = soup.find("link", attrs={"itemprop": "availability"})

        price = soup.find("span", attrs={"itemprop": "price"}).get("content")

        book_item["price"] = price

        if stock:
            stock = stock.next.strip()
            book_item["stock"] = stock
        else:
            book_item["stock"] = "0"

    except Exception as e:
        book_item["stock"] = "error"
        logger.exception(f"ERROR - {book_item['link']}")
    finally:
        global count
        print(f"\rDone - {count}", end="")
        count += 1


async def get_auth_token():
    resp = requests.get("https://www.chitai-gorod.ru", headers=headers)
    ddd = resp.cookies
    acc_token = (
        str(ddd["access-token"])
        .split("access-token=")[0]
        .split(";")[0]
        .replace("%20", " ")
    )
    print(acc_token)
    headers["Authorization"] = acc_token


async def get_gather_data(sample):
    logger.info("Start collect data")
    print()
    await get_auth_token()

    # Main loop
    with ThreadPoolExecutor(max_workers=10) as executor:
        threads = [executor.submit(get_main_data, i) for i in sample]

    # Reparse item
    with ThreadPoolExecutor(max_workers=10) as executor:
        threads_repars = [
            executor.submit(get_main_data, i) for i in sample if i["stock"] == "error"
        ]

    for i in sample:
        if i["stock"] == "error":
            i["stock"] = "0"

    print()
    global count
    count = 1
    logger.success("Finish collect data")


def main():
    try:
        # load_dotenv("../.env")
        books_in_sale = get_items_list("chit_gor")
        sample = give_me_sample(
            base_dir=BASE_LINUX_DIR, prefix="chit_gor", ozon_in_sale=books_in_sale
        )
        print(len(sample))
        asyncio.run(get_gather_data(sample))

        checker = quantity_checker(sample)
        if checker:
            # Push to OZON with API
            separate_records = separate_records_to_client_id(sample)
            logger.info("Start push to ozon")
            start_push_to_ozon(separate_records, prefix="chit_gor")
            logger.success("Data was pushed to ozon")
        else:
            logger.warning("Detected too many ZERO items")
            asyncio.run(tg_send_msg("'Читай-Город'"))

        logger.info("Start write to excel")
        df_result = pd.DataFrame(sample)

        # TG send
        df_del = df_result.loc[df_result["stock"] == "0"][["article"]]
        del_path = f"{BASE_LINUX_DIR}/chit_gor_del.xlsx"
        df_del.to_excel(del_path, index=False)

        df_without_del = df_result.loc[df_result["stock"] != "0"]
        new_stock_path = f"{BASE_LINUX_DIR}/chit_gor_new_stock.xlsx"
        df_without_del.to_excel(new_stock_path, index=False)

        logger.success("Finish write to excel")

        asyncio.run(tg_send_files([new_stock_path, del_path], "Chit_gor"))

        logger.success("Script was finished successfully")
        print(
            "\n---------------------------------------------------------------------------------------------\n"
        )
    except Exception as e:
        logger.exception(e)


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("13:00").do(archive_items_stock_to_zero, "chit_gor")
    schedule.every().day.at("16:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    super_main()
