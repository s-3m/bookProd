import os
import sys
import time
import datetime
from dotenv import load_dotenv
import schedule
from loguru import logger
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import aiohttp
import asyncio
import pandas as pd
from tg_sender import tg_send_files

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import check_danger_string, filesdata_to_dict, fetch_request


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

DEBUG = False
BASE_URL = "https://www.biblio-globus.ru"
BASE_LINUX_DIR = "/media/source/globus/every_day" if not DEBUG else "source/every_day"
logger.add(
    f"{BASE_LINUX_DIR}/log/globus_error.log",
    format="{time} {level} {message}",
    level="ERROR",
)
logger.add(
    f"{BASE_LINUX_DIR}/log/globus_error_serialize.json",
    format="{time} {level} {message}",
    level="ERROR",
    serialize=True,
)
semaphore = asyncio.Semaphore(20)
sample = pd.read_excel(
    f"{BASE_LINUX_DIR}/globus_new_stock.xlsx", converters={"article": str, "link": str}
)
sample["stock"] = ""
sample = sample.where(sample.notnull(), None)
sample = sample.to_dict("records")

count = 1


async def get_main_data(session, item):
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
            with open(f"{BASE_LINUX_DIR}/globus_error.txt", "a") as f:
                f.write(f"{item['link']} --- {e}\n")
        finally:
            global count
            print(f"\rDone - {count}", end="")
            count += 1


async def get_gather_data():
    logger.info("Start collect data")
    print()
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as session:
        for i in sample:
            if not i["link"]:
                i["stock"] = "del"
            else:
                task = asyncio.create_task(get_main_data(session, i))
                tasks.append(task)
        await asyncio.gather(*tasks)
    print()
    logger.success("Finish collect data")


def main():
    asyncio.run(get_gather_data())

    logger.info("Start write to excel")
    df_result = pd.DataFrame(sample)

    df_del = df_result.loc[df_result["stock"] == "del"][["article"]]
    del_path = f"{BASE_LINUX_DIR}/globus_del.xlsx"
    df_del.to_excel(del_path, index=False)

    df_without_del = df_result.loc[df_result["stock"] != "del"]
    new_stock_path = f"{BASE_LINUX_DIR}/__globus_new_stock.xlsx"
    df_without_del.to_excel(new_stock_path, index=False)
    logger.success("Finish art write to excel")
    logger.success("Script was finished successfully")


if __name__ == "__main__":
    main()
