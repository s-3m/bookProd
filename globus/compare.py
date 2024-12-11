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
from utils import fetch_request, give_me_sample

pandas.io.formats.excel.ExcelFormatter.header_style = None

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Connection": "keep-alive",
    # 'Cookie': '_ga=GA1.1.616419859.1731410744; _ym_uid=1731410745831810371; _ym_d=1731410745; RecentlyViewedProductsCookie=10979497%2c10902386; _ym_isad=2; _ym_visorc=w; _ga_XQBB831D6S=GS1.1.1731485055.4.0.1731485055.60.0.0',
    "Referer": "https://www.biblio-globus.ru/catalog/index/101",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36",
    "cache-control": "no-cache",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "YaBrowser";v="24.10", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.biblio-globus.ru"
BASE_LINUX_DIR = "/media/source/globus/every_day" if not DEBUG else "source/every_day"
logger.add(
    f"{BASE_LINUX_DIR}/log/error.log",
    format="{time} {level} {message}",
    level="ERROR",
)
logger.add(
    f"{BASE_LINUX_DIR}/log/error_serialize.json",
    format="{time} {level} {message}",
    level="ERROR",
    serialize=True,
)

count = 1


async def get_main_data(session, item, semaphore):
    async with semaphore:
        try:
            response = await fetch_request(session, item["link"], headers)
            soup = bs(response, "lxml")

            try:
                stock = (
                    soup.find("div", class_="qtyInStock")
                    .find("span")
                    .text.strip()
                    .split(" ")[1]
                )
            except:
                stock = "del"

            item["stock"] = stock
        except Exception as e:
            logger.exception(f"ERROR - {item['link']}")
            with open(f"{BASE_LINUX_DIR}/error.txt", "a") as f:
                f.write(f"{item['link']} --- {e}\n")
        finally:
            global count
            print(f"\rDone - {count}", end="")
            count += 1


async def get_gather_data(sample):
    logger.info("Start collect data")
    print()
    tasks = []
    semaphore = asyncio.Semaphore(20)
    async with aiohttp.ClientSession(headers=headers) as session:
        for i in sample:
            if not i["link"]:
                i["stock"] = "del"
            else:
                task = asyncio.create_task(get_main_data(session, i, semaphore))
                tasks.append(task)
        await asyncio.gather(*tasks)
    print()
    global count
    count = 1
    logger.success("Finish collect data")


def main():
    sample = give_me_sample(base_dir=BASE_LINUX_DIR, prefix="globus")
    asyncio.run(get_gather_data(sample))

    logger.info("Start write to excel")
    df_result = pd.DataFrame(sample)

    df_del = df_result.loc[df_result["stock"] == "del"][["article"]]
    del_path = f"{BASE_LINUX_DIR}/globus_del.xlsx"
    df_del.to_excel(del_path, index=False)

    df_without_del = df_result.loc[df_result["stock"] != "del"]
    new_stock_path = f"{BASE_LINUX_DIR}/globus_new_stock.xlsx"
    df_without_del.to_excel(new_stock_path, index=False)

    logger.success("Finish write to excel")

    asyncio.run(tg_send_files([new_stock_path, del_path], "Biblio-globus"))

    logger.success("Script was finished successfully")


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("21:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    main()
    # super_main()
