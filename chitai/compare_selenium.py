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
import time

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
    "cache-control": "max-age=0",
    # 'cookie': '__ddg9_=185.112.249.83; __ddg1_=R992fleg5s9uUqtWE49T; _pk_id.1.f5fe=b58316b62597714f.1743516792.; _pk_ses.1.f5fe=1; __ddgid_=QHi2XPU2bjRmnYcr; __ddgmark_=orfy5PTHwhmAnmj3; __ddg5_=rFtFVC5ujLXNhx3M; __ddg2_=EjiE0k6vT4io83Sc; __ddg8_=x2wIvvgyqOS3QoFQ; __ddg10_=1743516830',
    "priority": "u=0, i",
    "referer": "https://www.chitai-gorod.ru/",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
}
cookies = {
    "__ddg9_": "185.112.249.83",
    "__ddg1_": "R992fleg5s9uUqtWE49T",
    "_pk_id.1.f5fe": "b58316b62597714f.1743516792.",
    "_pk_ses.1.f5fe": "1",
    "__ddgid_": "QHi2XPU2bjRmnYcr",
    "__ddgmark_": "orfy5PTHwhmAnmj3",
    "__ddg5_": "rFtFVC5ujLXNhx3M",
    "__ddg2_": "EjiE0k6vT4io83Sc",
    "__ddg8_": "x2wIvvgyqOS3QoFQ",
    "__ddg10_": "1743516830",
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
                cookies=cookies,
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
            response = requests.get(
                book_item["link"], headers=headers, timeout=30, cookies=cookies
            )
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
    resp = requests.get("https://www.chitai-gorod.ru", headers=headers, cookies=cookies)
    time.sleep(15)
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
        archive_items_stock_to_zero(prefix="chit_gor")
        print(
            "\n---------------------------------------------------------------------------------------------\n"
        )
    except Exception as e:
        logger.exception(e)


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("16:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    # main()
    super_main()
