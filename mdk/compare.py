import os
import sys
from dotenv import load_dotenv
import schedule
from loguru import logger
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tg_sender import tg_send_files
from utils import filesdata_to_dict, fetch_request, give_me_sample

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
# semaphore = asyncio.Semaphore(15)
error_book = []
count = 1


async def get_main_data(session, book, proxy):
    book_url = f"{BASE_URL}/book/{book['article'][:-2]}"
    try:
        # async with semaphore:
        response = await fetch_request(session, book_url, headers, proxy=proxy)
        if response == "404":
            book["stock"] = "del"
        else:
            soup = bs(response, "lxml")
            try:
                stock = soup.find("div", {"class": "tg-quantityholder"}).get(
                    "data-maxqty"
                )
            except:
                stock = "del"

            book["stock"] = stock
    except Exception as e:
        error_book.append(book)
        logger.exception(f"ERROR with {book['article'][:-2]}")
        with open(f"{BASE_LINUX_DIR}/error.txt", "a") as f:
            f.write(f"{book['article'][:-2]} --- {e}\n")
    finally:
        global count
        print(f"\rDone - {count}", end="")
        count += 1


async def get_gather_data(sample, proxy):
    logger.info("Start collect data")
    timeout = aiohttp.ClientTimeout(total=800)
    async with aiohttp.ClientSession(
        headers=headers,
        connector=aiohttp.TCPConnector(ssl=False, limit_per_host=5),
        timeout=timeout,
        trust_env=True,
    ) as session:
        tasks = [
            asyncio.create_task(get_main_data(session, book, proxy)) for book in sample
        ]
        await asyncio.gather(*tasks)

        # Reparse errors
        logger.warning(f"Errors detected: {len(error_book)}")
        error_book.clear()
        error_tasks = [
            asyncio.create_task(get_main_data(session, book, proxy))
            for book in sample
            if book["stock"] == "error"
        ]
        await asyncio.gather(*error_tasks)

        for book in sample:
            if book["stock"] == "error":
                book["stock"] = "del"

        logger.warning(f"Error not reparse: {len(error_book)}")

        print()
        logger.info("Finish collect data")


def main():
    proxy = os.getenv("PROXY")
    logger.info("Start script")
    sample = give_me_sample(base_dir=BASE_LINUX_DIR, prefix="mdk", without_merge=True)
    asyncio.run(get_gather_data(sample, proxy))
    logger.info("Start write files")

    df = pd.DataFrame(sample)
    df.drop_duplicates(inplace=True, subset="article", keep="last")

    df_del = df.loc[df["stock"] == "del"][["article"]]
    del_path = f"{BASE_LINUX_DIR}/mdk_del.xlsx"
    df_del.to_excel(del_path, index=False)

    df_without_del = df.loc[df["stock"] != "del"]
    new_stock_path = f"{BASE_LINUX_DIR}/mdk_new_stock.xlsx"
    df_without_del.to_excel(new_stock_path, index=False)

    logger.success("Finish write to excel")

    asyncio.run(tg_send_files([new_stock_path, del_path], "mdk"))

    logger.success("Script was finished successfully")


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("22:50").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    super_main()
