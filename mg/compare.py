import sys, os
import time
import schedule
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import aiohttp
import asyncio
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tg_sender import tg_send_files, tg_send_msg
from utils import fetch_request, give_me_sample, quantity_checker
from ozon.ozon_api import (
    separate_records_to_client_id,
    start_push_to_ozon,
    get_items_list,
    archive_items_stock_to_zero,
)
from ozon.utils import logger_filter

pandas.io.formats.excel.ExcelFormatter.header_style = None

BASE_URL = "https://www.dkmg.ru"
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}
ajax_headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ru,en;q=0.9",
    "Connection": "keep-alive",
    # 'Cookie': '_ym_uid=1724136084781768402; _ym_d=1724136084; BITRIX_SM_mguser=972aa2e5-c315-416b-88b3-92f79b453510; BX_USER_ID=ed697227b63725fef1d378c8253f2c14; PHPSESSID=vrf57rm4u3ioipl9cgotmjgec6; _ym_isad=2; _ym_visorc=w',
    "Referer": "https://www.dkmg.ru/catalog/search/?Catalog_ID=0&search_word=978-5-9951-4898-2.0&Series_ID=&Publisher_ID=&Year_Biblio=",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 YaBrowser/24.12.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Chromium";v="130", "YaBrowser";v="24.12", "Not?A_Brand";v="99", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}


count = 1
DEBUG = True if sys.platform.startswith("win") else False
BASE_LINUX_DIR = "/media/source/mg/every_day" if not DEBUG else "source/every_day"
logger.add(
    f"{BASE_LINUX_DIR}/error.log", format="{time} {level} {message}", level="ERROR"
)
logger.add(
    f"{BASE_LINUX_DIR}/log.json",
    level="WARNING",
    serialize=True,
    filter=logger_filter,
)
error_items_count = 0


async def get_id_from_ajax(session, item):
    ajax_url = "https://www.dkmg.ru/ajax/ajax_search.php"
    params = {"term": item["article"][:-2]}
    async with session.get(ajax_url, params=params, headers=ajax_headers) as resp:
        ajax_result = await resp.json(content_type=None)
        item_id = ajax_result[0].get("value")
        if item_id and item_id != "#":
            item["id"] = item_id.split("/")[-1].strip()


async def get_item_data(session, item):
    global count
    global error_items_count
    try:
        if not item["id"]:
            await get_id_from_ajax(session, item)

        if not item["id"]:
            item["stock"] = "0"
            item["price"] = None
            return

        full_url = f"{BASE_URL}/tovar/{item["id"]}"
        response = await fetch_request(session, full_url, headers)
        if response == "404":
            item["stock"] = "0"
            item["price"] = None
            return
        elif not response:
            item["stock"] = "error"
            item["price"] = None
            return
        soup = bs(response, "lxml")
        buy_btn = soup.find("a", class_="btn_red wish_list_btn add_to_cart")
        if not buy_btn:
            item["stock"] = "0"
        else:
            item["stock"] = "2"

        price = (
            soup.find_all("div", class_="product_item_price")[1]
            .text.strip()
            .split(".")[0]
            .replace(" ", "")
        )

        item["price"] = price

        print(f"\rDone - {count} | Error - {error_items_count}", end="")
        count += 1
    except Exception as e:
        item["stock"] = "error"
        error_items_count += 1
        logger.exception(item)
        with open(f"{BASE_LINUX_DIR}/error.txt", "a+") as file:
            file.write(f"{item['article']} --- {e}\n")


async def get_gather_data(sample):
    global error_items_count
    tasks = []
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit_per_host=10, limit=10),
        timeout=aiohttp.ClientTimeout(total=1200),
    ) as session:
        for item in sample:
            task = asyncio.create_task(get_item_data(session, item))
            tasks.append(task)

        await asyncio.gather(*tasks)

        if error_items_count > 0:
            logger.info("\nStart reparse error")
            error_items_count = 0
            reparse_tasks = [
                asyncio.create_task(get_item_data(session, item))
                for item in sample
                if item["stock"] == "error"
            ]
            await asyncio.gather(*reparse_tasks)
            print()
            logger.info(
                f"Success parse - {count} | Not reparse - {error_items_count} errors"
            )

        for i in sample:
            if item["stock"] == "error":
                i["stock"] = "0"


def main():
    logger.info("Start MG parsing")
    books_in_sale = get_items_list("mg")
    sample = give_me_sample(
        BASE_LINUX_DIR, prefix="mg", merge_obj="id", ozon_in_sale=books_in_sale
    )

    asyncio.run(get_gather_data(sample))

    checker = quantity_checker(sample)

    if checker:
        # Push to OZON with API
        separate_records = separate_records_to_client_id(sample)
        print()
        logger.info("Start push to ozon")
        start_push_to_ozon(separate_records, prefix="mg")
        logger.success("Data was pushed to ozon")
    else:
        print()
        logger.warning("Detected too many ZERO items")
        asyncio.run(tg_send_msg("'Гвардия'"))

    df_result = pd.DataFrame(sample)
    df_del = df_result.loc[df_result["stock"] == "0"][["article"]]
    del_file = f"{BASE_LINUX_DIR}/mg_del.xlsx"
    df_del.to_excel(del_file, index=False)

    df_without_del = df_result.loc[df_result["stock"] != "0"]
    stock_file = f"{BASE_LINUX_DIR}/mg_new_stock.xlsx"
    df_without_del.to_excel(stock_file, index=False)

    global count
    global error_items_count
    error_items_count = 0
    count = 1

    time.sleep(5)
    asyncio.run(tg_send_files([stock_file, del_file], subject="Гвардия"))
    print(f"\n{"----------" * 5}\n")

    archive_items_stock_to_zero(prefix="mg")


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("20:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    start_time = time.time()
    # main()
    super_main()
    print()
    print(time.time() - start_time)
