import os
import sys

import requests
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
from utils import give_me_sample
from concurrent.futures import ThreadPoolExecutor

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
error_book = []
count = 1


def get_link_from_ajax(article):

    params = {
        "phrase": article[:-2],
        "customerCityId": "213",
    }
    resp = requests.get(
        "https://web-gate.chitai-gorod.ru/api/v2/search/product",
        headers=headers,
        params=params,
    )
    response = resp.json()
    link = response["included"][0]["attributes"]["url"]
    return link


def get_main_data(book_item):
    try:
        if not book_item["link"]:
            logger.info("Нет ссылки")
            i_link = get_link_from_ajax(book_item["article"])
            book_item["link"] = f"{BASE_URL}/{i_link}"
            logger.info(f"Нашёл ссылку - {book_item["link"]}")

        response_text = ""
        for _ in range(5):
            response = requests.get(book_item["link"], headers=headers)
            if response.status_code == 404:
                book_item["stock"] = "del"
                return
            if response.status_code == 200:
                response_text = response.text
                break

        soup = bs(response_text, "lxml")

        online_option = soup.find("div", class_="product-offer-price")
        online_option_2 = soup.find("span", class_="offer-availability-status--green")
        in_shop_option = soup.find("p", class_="product-offer-header__title")
        not_in_option = soup.find("div", class_="detail-product__unavailable")
        if in_shop_option:
            moscow_shop_check = soup.find(
                "div", class_="product-offer-shops__title"
            ).text
            if "В наличии в" in moscow_shop_check:
                in_shop_option = True
            else:
                in_shop_option = False
                book_item["stock"] = "del"

        stock = soup.find("link", attrs={"itemprop": "availability", "href": "InStock"})

        if online_option or in_shop_option:
            if stock:
                stock = stock.next.strip()
                book_item["stock"] = stock
            else:
                book_item["stock"] = "Только в магазине"
        elif not_in_option and online_option_2 is None:
            book_item["stock"] = "del"

    except Exception as e:
        book_item["stock"] = "error"
        error_book.append(book_item["article"])
        logger.exception(f"ERROR - {book_item['link']}")
        with open(f"{BASE_LINUX_DIR}/error.txt", "a") as f:
            f.write(f"{book_item['link']} --- {e}\n")
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
    with ThreadPoolExecutor(max_workers=5) as executor:
        threads = [executor.submit(get_main_data, i) for i in sample]

        # Reparse item
    with ThreadPoolExecutor(max_workers=5) as executor:
        threads_repars = [
            executor.submit(get_main_data, i) for i in sample if i["stock"] == "error"
        ]

    for i in sample:
        if i["stock"] == "error":
            i["stock"] = "del"

    print()
    global count
    count = 1
    logger.success("Finish collect data")


def main():
    # load_dotenv("../.env")
    sample = give_me_sample(base_dir=BASE_LINUX_DIR, prefix="chit-gor")
    print(len(sample))
    asyncio.run(get_gather_data(sample))

    logger.info("Start write to excel")
    df_result = pd.DataFrame(sample)

    df_del = df_result.loc[df_result["stock"] == "del"][["article"]]
    del_path = f"{BASE_LINUX_DIR}/chit-gor_del.xlsx"
    df_del.to_excel(del_path, index=False)

    df_without_del = df_result.loc[df_result["stock"] != "del"]
    new_stock_path = f"{BASE_LINUX_DIR}/chit-gor_new_stock.xlsx"
    df_without_del.to_excel(new_stock_path, index=False)

    logger.success("Finish write to excel")

    asyncio.run(tg_send_files([new_stock_path, del_path], "Chit-gor"))

    logger.success("Script was finished successfully")


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("16:00").do(main)

    while True:
        schedule.run_pending()


if __name__ == "__main__":
    super_main()
