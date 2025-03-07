import json
import os
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
from .tg_sender import tg_send_files, tg_send_msg
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import give_me_sample, quantity_checker
from ozon.ozon_api import separate_records_to_client_id, start_push_to_ozon, get_in_sale
from ozon.utils import logger_filter

pandas.io.formats.excel.ExcelFormatter.header_style = None

BASE_URL = "https://bookbridge.ru"
USER_AGENT = UserAgent()

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Connection": "keep-alive",
    # 'Cookie': 'prefers-color-scheme=dark; prefers-color-scheme=dark; prefers-color-scheme=dark; _ym_uid=1717183212832820790; BX_USER_ID=a0af244758793d3dc0af39826552039d; BITRIX_SM_LAST_ADV=7; BITRIX_SM_SALE_UID=78be4594d53d6b19bf10305a8e1d4a6e; searchbooster_v2_user_id=n05uIFy3UTrz7mcUOGX3B_8cAbP0_BBEfGtwSyhfWyi%7C9.26.23.36; ageCheckPopupRedirectUrl=%2Fv2-mount-input; BITRIX_SM_AG_SMSE_H=9780230452732_U1%7C9780230438002%7C9780545231398%7C9781380013514%7C9781903128770%7C9780521713214%7C9780521538053%7C9781408216965%7C9780521608794%7C9780333665749; _ym_d=1733082642; BITRIX_SM_GUEST_ID=2134331; PHPSESSID=W3azKyeB5MqsmYbqkAXpxjHI9qQBQr5N; ASPRO_MAX_USE_MODIFIER=Y; MAX_VIEWED_ITEMS_s1=%7B%2245576%22%3A%5B%221734210161880%22%2C%222761648%22%5D%7D; prefers-color-scheme=dark; BITRIX_CONVERSION_CONTEXT_s1=%7B%22ID%22%3A2%2C%22EXPIRE%22%3A1734555540%2C%22UNIQUE%22%3A%5B%22conversion_visit_day%22%5D%7D; _ym_debug=null; _ym_isad=2; BITRIX_SM_LAST_VISIT=18.12.2024%2021%3A46%3A29',
    "Referer": "https://bookbridge.ru/catalog/angliyskiy/uchebnaya_literatura/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 YaBrowser/24.12.0.0 Safari/537.36",
    "cache-control": "no-cache",
    "sec-ch-ua": '"Chromium";v="130", "YaBrowser";v="24.12", "Not?A_Brand";v="99", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

count = 1
DEBUG = True if sys.platform.startswith("win") else False
PATH_TO_FILES = "/media/source/bb/every_day" if not DEBUG else "source/every_day"
errors = []


@logger.catch
async def fetch_request(session, url):
    for _ in range(20):
        async with session.get(url, headers=headers) as response:
            await asyncio.sleep(5)
            if response.status == 200:
                return await response.text()
            elif response.status == 404:
                return "del"
    return None


@logger.catch
async def get_item_data(session, item, semaphore, item_index=None):
    async with semaphore:
        try:
            response = await fetch_request(session, item["link"])
            if response == "del":
                item["stock"] = "0"
                return
            soup = bs(response, "lxml")
            quantity_element = soup.find("span", class_="plus dark-color")
            stock_quantity = "0"
            if quantity_element:
                stock_quantity = quantity_element.get("data-max")
                if not stock_quantity:
                    stock_quantity = soup.find("div", class_="quantity_block")
                    if stock_quantity:
                        stock_quantity = (
                            stock_quantity.find("span").text.strip().split("\n")[0]
                        )
                    else:
                        stock_quantity = "2"
            global count
            print(f"\rDone - {count} | error - {len(errors)}", end="")
            count += 1
            item["stock"] = stock_quantity
        except Exception as e:
            logger.exception(item["link"])
            item["stock"] = "0"
            errors.append(item_index)
            today = datetime.date.today().strftime("%d-%m-%Y")
            with open(f"{PATH_TO_FILES}/error.txt", "a+") as f:
                f.write(f"{today} --- {item['link']} --- {e}\n")


async def get_link_from_ajax(session, id):
    params = {
        "query": id[:-2],
        "locale": "ru",
        "client": "bookbridge.ru",
    }
    try:
        async with session.get(
            "https://api.searchbooster.net/api/6097963d-ae4c-4620-ae3d-0ae0fc8387f8/completions",
            params=params,
            headers=headers,
        ) as response:
            data = await response.json()
            for i in data["searchBox"]:
                if i.get("offer_code") == id[:-2]:
                    if i.get("url"):
                        print(i["url"])
                        return i["url"]
                    return
            return
    except Exception as e:
        logger.exception(id)


async def get_gather_data():
    books_in_sale = get_in_sale("bb")
    sample = give_me_sample(
        base_dir=PATH_TO_FILES, prefix="bb", ozon_in_sale=books_in_sale
    )

    semaphore = asyncio.Semaphore(10)
    tasks = []
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit=10, limit_per_host=10),
        trust_env=True,
    ) as session:
        for item in sample:
            if not item["link"]:
                item["link"] = await get_link_from_ajax(session, item["article"])

        for item in sample:
            if not item["link"]:
                item["stock"] = "0"
                continue
            item_index = sample.index(item)
            task = asyncio.create_task(
                get_item_data(
                    session=session,
                    item=item,
                    item_index=item_index,
                    semaphore=semaphore,
                )
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

        # Start reparse error
        if len(errors) > 0:
            logger.warning("Start reparse error")
            try:
                tasks = [
                    asyncio.create_task(
                        get_item_data(
                            session=session,
                            item=sample[error_index],
                            semaphore=semaphore,
                        )
                    )
                    for error_index in errors
                ]
                await asyncio.gather(*tasks)
            except Exception as e:
                logger.exception(e)

    print()
    logger.success("Finished parser successfully")
    global count
    count = 1

    logger.info("Preparing file")

    checker = quantity_checker(sample)
    if checker:
        # Push to OZON with API
        separate_records = separate_records_to_client_id(sample)
        logger.info("Start push to ozon")
        start_push_to_ozon(separate_records, prefix="bb")
        logger.success("Data was pushed to ozon")
    else:
        logger.warning("Detected too many ZERO items")
        await tg_send_msg("'Букбридж'")

    df_result = pd.DataFrame(sample)
    # df_result = df_result.loc[df_result["stock"] != "0"]
    # df_result.to_excel(f"{PATH_TO_FILES}/bb_new_stock.xlsx", index=False)

    df_without_del = df_result.loc[df_result["stock"] != "0"]
    df_del = df_result.loc[df_result["stock"] == "0"][["article"]]
    del_path = f"{PATH_TO_FILES}/bb_del.xlsx"
    without_del_path = f"{PATH_TO_FILES}/bb_new_stock.xlsx"
    df_without_del.to_excel(without_del_path, index=False)
    df_del.to_excel(del_path, index=False)

    await asyncio.sleep(10)
    logger.info("Start sending files")
    await tg_send_files([without_del_path, del_path], subject="бб")


def main():
    logger.add(
        f"{PATH_TO_FILES}/error.log", format="{time} {level} {message}", level="ERROR"
    )
    logger.add(
        f"{PATH_TO_FILES}/log.json",
        level="WARNING",
        serialize=True,
        filter=logger_filter,
    )
    logger.info("Start parsing BookBridge.ru")
    asyncio.run(get_gather_data())


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("15:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    start_time = time.time()
    super_main()
    print(f"\n{time.time() - start_time}")
