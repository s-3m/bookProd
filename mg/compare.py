import os.path
import time
import schedule
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import aiohttp
import asyncio
import pandas as pd
from tg_sender import tg_send_files
from loguru import logger
from utils import fetch_request, give_me_sample

pandas.io.formats.excel.ExcelFormatter.header_style = None

BASE_URL = "https://www.dkmg.ru"
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}

count = 1
DEBUG = True
BASE_LINUX_DIR = "/media/source/mg/every_day" if not DEBUG else "source/every_day"
logger.add(
    f"{BASE_LINUX_DIR}/error.log", format="{time} {level} {message}", level="ERROR"
)
error_items = []


async def get_item_data(session, item, semaphore, sample, reparse=False):
    global count

    url = f"https://www.dkmg.ru/catalog/search/?search_word={item["article"][:-2]}"
    try:
        async with semaphore:
            search_response = await fetch_request(session, url, headers)
            soup = bs(search_response, "lxml")
            content = soup.find("div", {"id": "content"}).find("div", class_="item")
            if content is None:
                item["stock"] = "del"
                return

            link = content.find("a").get("href")
            full_url = f"{BASE_URL}{link}"

            response = await fetch_request(session, full_url, headers)
            soup = bs(response, "lxml")
            buy_btn = soup.find("a", class_="btn_red wish_list_btn add_to_cart")
            if not buy_btn:
                item["stock"] = "del"
            else:
                item["stock"] = "2"

            if reparse:
                sample.append(item)

            print(f"\rDone - {count}", end="")
            count += 1
    except Exception as e:
        error_items.append(item)
        logger.exception(e)
        with open(f"{BASE_LINUX_DIR}/error.txt", "a+") as file:
            file.write(f"{item['article']} --- {e}\n")


async def get_gather_data(semaphore, sample):
    tasks = []
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit_per_host=10, limit=50),
        timeout=aiohttp.ClientTimeout(total=1200),
    ) as session:
        for item in sample:
            task = asyncio.create_task(get_item_data(session, item, semaphore, sample))
            tasks.append(task)

        await asyncio.gather(*tasks)

        if error_items:
            print()
            logger.info("\nStart reparse error")
            print(f"--- Quantity error - {len(error_items)}")
            errors_copy = error_items.copy()
            error_items.clear()
            reparse_tasks = [
                asyncio.create_task(
                    get_item_data(session, item, semaphore, sample, reparse=True)
                )
                for item in errors_copy
            ]

            await asyncio.gather(*reparse_tasks)
            sample.extend(errors_copy)

        for i in sample:
            if not i["stock"]:
                i["stock"] = "del"


def main():
    logger.info("Start MG parsing")
    sample = give_me_sample(BASE_LINUX_DIR, prefix="mg", without_merge=True)

    semaphore = asyncio.Semaphore(5)
    asyncio.run(get_gather_data(semaphore, sample))

    df_result = pd.DataFrame(sample)
    df_result.drop_duplicates(inplace=True, keep="last", subset="article")

    result_file = f"{BASE_LINUX_DIR}/mg_all_result.xlsx"
    df_result.to_excel(result_file, index=False)

    df_del = df_result.loc[df_result["stock"] == "del"][["article"]]
    del_file = f"{BASE_LINUX_DIR}/mg_del.xlsx"
    df_del.to_excel(del_file, index=False)

    df_without_del = df_result.loc[df_result["stock"] != "del"]
    stock_file = f"{BASE_LINUX_DIR}/mg_new_stock.xlsx"
    df_without_del.to_excel(stock_file, index=False)
    global count
    count = 1
    time.sleep(10)
    print()
    logger.success("Parse end successfully")
    asyncio.run(tg_send_files([stock_file, del_file], subject="Гвардия"))


def super_main():
    schedule.every().day.at("03:30").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    start_time = time.time()
    super_main()
    print()
    print(time.time() - start_time)
