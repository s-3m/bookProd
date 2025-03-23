import os
import sys
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
import schedule
from loguru import logger
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import asyncio
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tg_sender import tg_send_files, tg_send_msg
from utils import give_me_sample, sync_fetch_request, quantity_checker
from ozon.ozon_api import get_in_sale, start_push_to_ozon, separate_records_to_client_id
from ozon.utils import logger_filter

pandas.io.formats.excel.ExcelFormatter.header_style = None

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    # 'Cookie': 'mdk_session=u258615rb5b6de9s23usk0k6oq; ab=423e39551c447a02edec1afdbad60a8fa3ba3871%7ES; city_zip=3a63c8786b135cd844d7071e992508e361acac14%7E101000; _ym_uid=1731307959487657928; _ym_d=1731307959; _gid=GA1.2.1239579180.1731307960; _ym_visorc=w; _ym_isad=2; _ga_V7RS373QY7=GS1.1.1731307959.1.1.1731309048.0.0.0; _ga=GA1.1.2130916544.1731307960',
    "Referer": "https://mdk-arbat.ru/catalog/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "YaBrowser";v="24.10", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://mdk-arbat.ru"
BASE_LINUX_DIR = "/media/source/mdk/every_day" if not DEBUG else "source/every_day"

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

error_book = []
count = 1


def get_main_data(book):
    book_url = f"{BASE_URL}/book/{book['article'][:-2]}"
    try:
        # async with semaphore:
        response = sync_fetch_request(book_url, headers)
        if response == "404":
            book["stock"] = "0"
            book["price"] = None
            return
        elif response == 503:
            book["stock"] = "error"
            return
        else:
            soup = bs(response, "lxml")
            quantity_area = soup.find("div", {"class": "tg-quantityholder"})
            if not quantity_area:
                book["stock"] = "0"
                book["price"] = None
                return
            stock = quantity_area.get("data-maxqty")
            book["stock"] = stock if stock else "0"

            price = (
                soup.find("span", {"class": "itempage-price_inet"})
                .text[:-1]
                .strip()
                .replace("\xa0", "")
            )
            book["price"] = price

    except Exception as e:
        book["stock"] = "error"
        error_book.append(book)
        logger.exception(f"ERROR with {book['article'][:-2]}")
        with open(f"{BASE_LINUX_DIR}/error.txt", "a") as f:
            f.write(f"{book['article'][:-2]} --- {e}\n")
    finally:
        global count
        print(f"\rDone - {count} | Error books - {len(error_book)}", end="")
        count += 1


async def get_gather_data(sample):
    logger.info("Start collect data")

    with ThreadPoolExecutor(max_workers=10) as executor:
        for book in sample:
            executor.submit(get_main_data, book)

    # Reparse errors
    logger.warning(f"Errors detected: {len(error_book)}")
    error_book.clear()

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in sample:
            if i["stock"] == "error":
                executor.submit(get_main_data, i)

    # Note all not reparse item to del
    for book in sample:
        if book["stock"] == "error":
            book["stock"] = "0"

    logger.warning(f"Error not reparse: {len(error_book)}")

    print()
    logger.info("Finish collect data")


def main():
    logger.info("Start script")
    books_in_sale = get_in_sale("mdk")
    sample = give_me_sample(
        base_dir=BASE_LINUX_DIR,
        prefix="mdk",
        without_merge=True,
        ozon_in_sale=books_in_sale,
    )
    asyncio.run(get_gather_data(sample))

    checker = quantity_checker(sample)
    if checker:
        # Push to OZON with API
        separate_records = separate_records_to_client_id(sample)
        logger.info("Start push to ozon")
        start_push_to_ozon(separate_records, prefix="mdk")
        logger.success("Data was pushed to ozon")
    else:
        logger.warning("Detected too many ZERO items")
        asyncio.run(tg_send_msg("'МДК'"))

    logger.info("Start write files")

    df = pd.DataFrame(sample)
    df.drop_duplicates(inplace=True, subset="article", keep="last")

    df_del = df.loc[df["stock"] == "0"][["article"]]
    del_path = f"{BASE_LINUX_DIR}/mdk_del.xlsx"
    df_del.to_excel(del_path, index=False)

    df_without_del = df.loc[df["stock"] != "0"]
    new_stock_path = f"{BASE_LINUX_DIR}/mdk_new_stock.xlsx"
    df_without_del.to_excel(new_stock_path, index=False)

    logger.success("Finish write to excel")

    asyncio.run(tg_send_files([new_stock_path, del_path], "mdk"))

    logger.success("Script was finished successfully")
    global count
    count = 1
    print("-----------" * 5)


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("18:25").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    super_main()
