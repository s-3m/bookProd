import asyncio
import sys
import os

import schedule
from loguru import logger
import aiohttp
import pandas as pd
from pandas.io.formats import excel
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import time

from selenium_data import get_book_data

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tg_sender import tg_send_files, tg_send_msg
from utils import give_me_sample, fetch_request, quantity_checker
from ozon.ozon_api import separate_records_to_client_id, start_push_to_ozon, get_in_sale
from ozon.utils import logger_filter


DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.moscowbooks.ru/"
USER_AGENT = UserAgent()
PATH_TO_FILES = "/media/source/msk/every_day" if not DEBUG else "source/every_day"
logger.add(
    f"{PATH_TO_FILES}/error.log", format="{time} {level} {message}", level="ERROR"
)
logger.add(
    f"{PATH_TO_FILES}/log.json",
    level="WARNING",
    serialize=True,
    filter=logger_filter,
)

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}

excel.ExcelFormatter.header_style = None

count = 1
error_count = 0


async def to_check_item(item, session):
    global count
    global error_count
    link = f"{BASE_URL}/book/{item["article"][:-2]}"
    try:
        response = await fetch_request(session, link, headers=headers, sleep=None)
        soup = bs(response, "lxml")
        age_control = soup.find("input", id="age_verification_form_mode")
        script_index = 1
        if age_control:
            closed_page = get_book_data(link)
            soup = bs(closed_page, "lxml")
            script_index = 5

        if not soup.find("div", class_="book__buy"):
            item["stock"] = "0"
            item["price"] = None
            return

        else:
            need_element = soup.find_all("script")
            a = (
                need_element[script_index]
                .text.split("MbPageInfo = ")[1]
                .replace("false", "False")
                .replace("true", "True")
            )
            need_data_dict = eval(a[:-1])["Products"][0]
            stock = need_data_dict["Stock"]
            item["stock"] = stock if stock != 9999999 else "1"

        price = soup.find("div", class_="book__price")
        if price:
            price = price.text.strip().replace("\xa0", "")
            item["price"] = price
        else:
            item["price"] = None

        print(f"\r{count} | Error book - {error_count}", end="")
        count += 1
    except Exception as e:
        item["stock"] = "error"
        error_count += 1
        logger.exception(f"ERROR with {link}")
        with open(
            f"{PATH_TO_FILES}/error.txt",
            "a+",
        ) as f:
            f.write(f"{link} ------ {e}\n")


async def get_compare():
    books_in_sale = get_in_sale("msk")
    sample = give_me_sample(
        base_dir=PATH_TO_FILES,
        prefix="msk",
        without_merge=True,
        ozon_in_sale=books_in_sale,
    )

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False), trust_env=True
    ) as session:
        for item in sample:
            await to_check_item(item, session)

        print()  # empty print for clear info visualization
        logger.warning("Start reparse error")

        for item in sample:
            if item["stock"] == "error":
                await to_check_item(item, session)
    global count
    count = 1

    for item in sample:
        if item["stock"] == "error":
            item["stock"] = "0"
            item["price"] = None

    checker = quantity_checker(sample)
    if checker:
        # Push to OZON with API
        separate_records = separate_records_to_client_id(sample)
        logger.info("Start push to ozon")
        start_push_to_ozon(separate_records, prefix="msk")
        logger.success("Data was pushed to ozon")
    else:
        logger.warning("Detected too many ZERO items")
        await tg_send_msg("'Москва'")

    # TG send
    logger.info("Preparing files for sending")
    df_result = pd.DataFrame(sample)
    df_without_del = df_result.loc[df_result["stock"] != "0"]
    without_del_path = f"{PATH_TO_FILES}/msk_new_stock.xlsx"
    df_without_del.to_excel(without_del_path, index=False)

    df_del = df_result.loc[df_result["stock"] == "0"][["article"]]
    del_path = f"{PATH_TO_FILES}/msk_del.xlsx"
    df_del.to_excel(del_path, index=False)

    logger.info("Start sending files")
    await tg_send_files([without_del_path, del_path], subject="Москва")
    print(f"\n{"----------" * 5}\n")


def main():
    logger.info("Start parsing Moscow")
    asyncio.run(get_compare())


def super_main():
    schedule.every().day.at("23:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    super_main()
