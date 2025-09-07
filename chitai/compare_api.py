import os
import random
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
from chit_utils import parse_with_playwright_proxy
from utils import give_me_sample, quantity_checker, sync_fetch_request
from utils import PROXIES
from concurrent.futures import ThreadPoolExecutor
from ozon.ozon_api import (
    get_items_list,
    start_push_to_ozon,
    separate_records_to_client_id,
    archive_items_stock_to_zero,
)
from ozon.utils import logger_filter

pandas.io.formats.excel.ExcelFormatter.header_style = None

cookies = {
    "tmr_lvid": "03935c7450b807684f9dcd65334067b0",
    "tmr_lvidTS": "1719430836103",
    "_ym_uid": "1719430836919836282",
    "popmechanic_sbjs_migrations": "popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1",
    "adrcid": "A6I49NSU-aGn_FnD2kzLfwA",
    "adrcid": "A6I49NSU-aGn_FnD2kzLfwA",
    "_ymab_param": "NOyyliya_BgJ0VOb3JL1PA1-1gyIOZRewTIGnSgxe-t2ci28PM-AMDZfHAuZzH4TsvmxnZPeYOHvYxXI9RgNC-VeR8Q",
    "adid": "173169335285853",
    "analytic_id": "1731693382619506",
    "_ga_YVB4ZXMWPL": "GS1.2.1731764757.1.1.1731765315.60.0.0",
    "_ga": "GA1.1.1903252348.1719430836",
    "_pk_id.1.f5fe": "739be507b513d787.1734287298.",
    "__USER_ID_COOKIE_NAME__": "173477373250003",
    "__P__wuid": "e9e879e0b9184ea1d0f3cef59ae944f3",
    "stDeIdU": "e9e879e0b9184ea1d0f3cef59ae944f3",
    "access-token": "Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTczMTk1MTYsImlhdCI6MTc1NzE1MTUxNiwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6ImI3NTdjNWQ5NWQ2ZDNmOTQyNjYxMDVlOGM4NDlhMTU1YTE5YTgwYjU0OGYxNzJiYzhjNDQyNjhlMTM2ZjliN2EiLCJ0eXBlIjoxMH0.JkEd3M1DCeQ5hcqNGPfbW8J7-fQwGllfSmDDOh3nnvM",
    "__ddgid_": "kqef4ZJd3nmCz7F5",
    "flocktory-uuid": "0241fd6a-c187-4d15-b067-6914fd7ea4c9-9",
    "_ga_W0V3RXZCPY": "GS2.1.s1748254899$o69$g0$t1748254899$j0$l0$h0",
    "_ga_LN4Z31QGF4": "GS2.1.s1748254899$o68$g0$t1748254899$j60$l0$h1954308688$ddGf82hx6-g98Aa9NAsjY4lKdL5u-b15wng",
    "_ga_6JJPBGS8QY": "GS2.1.s1748254899$o69$g0$t1748254899$j0$l0$h0",
    "__ddg1_": "NccemJ5EAdt5mQJHUWzi",
    "_ym_d": "1752911287",
    "gdeslon.ru.__arc_domain": "gdeslon.ru",
    "gdeslon.ru.user_id": "75110470-37f1-4bb5-ad12-750d369722c2",
    "__ddg9_": "89.169.48.165",
    "__ddgmark_": "AwUpOznqJrt8960s",
    "__ddg5_": "IpYxxNqs0IQiLV5J",
    "__ddg2_": "h1ncZ4olo86x2ewA",
    "ddg_last_challenge": "1757151513546",
    "tid-back-to": "%7B%22fullPath%22%3A%22%2F%22%2C%22hash%22%3A%22%22%2C%22query%22%3A%7B%7D%2C%22name%22%3A%22index%22%2C%22path%22%3A%22%2F%22%2C%22params%22%3A%7B%7D%2C%22meta%22%3A%7B%7D%7D",
    "utm_custom_source": "default",
    "tid-state": "7c1f48ba-6f7d-4017-90c6-22f1c22fcd68",
    "tid-redirect-uri": "https%3A%2F%2Fwww.chitai-gorod.ru%2Fauth%2Ft-id-next",
    "chg_visitor_id": "48d8b642-4794-4d55-a13b-434fb38d0c9b",
    "_pk_ses.1.f5fe": "1",
    "_ym_isad": "2",
    "vIdUid": "33e05e06-7617-4ea1-857a-3198862c74a2",
    "stLaEvTi": "1757151517626",
    "stSeStTi": "1757151517625",
    "_ym_visorc": "w",
    "adrdel": "1757151517749",
    "adrdel": "1757151517749",
    "acs_3": "%7B%22hash%22%3A%221aa3f9523ee6c2690cb34fc702d4143056487c0d%22%2C%22nst%22%3A1757237917780%2C%22sl%22%3A%7B%22224%22%3A1757151517780%2C%221228%22%3A1757151517780%7D%7D",
    "acs_3": "%7B%22hash%22%3A%221aa3f9523ee6c2690cb34fc702d4143056487c0d%22%2C%22nst%22%3A1757237917780%2C%22sl%22%3A%7B%22224%22%3A1757151517780%2C%221228%22%3A1757151517780%7D%7D",
    "domain_sid": "L3BsIrNkQonH7entRBvC0%3A1757151518623",
    "mindboxDeviceUUID": "0660d298-673e-43fc-8993-474c6e6cd4c8",
    "directCrm-session": "%7B%22deviceGuid%22%3A%220660d298-673e-43fc-8993-474c6e6cd4c8%22%7D",
    "tmr_detect": "0%7C1757151520135",
    "__ddg8_": "R3UUqMEb4nerDrP0",
    "__ddg10_": "1757151532",
}

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "referer": "https://www.ya.ru/",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "YaBrowser";v="25.8", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 YaBrowser/25.8.0.0 Safari/537.36",
    "Connection": "keep-alive",
    # 'cookie': 'tmr_lvid=03935c7450b807684f9dcd65334067b0; tmr_lvidTS=1719430836103; _ym_uid=1719430836919836282; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; adrcid=A6I49NSU-aGn_FnD2kzLfwA; adrcid=A6I49NSU-aGn_FnD2kzLfwA; _ymab_param=NOyyliya_BgJ0VOb3JL1PA1-1gyIOZRewTIGnSgxe-t2ci28PM-AMDZfHAuZzH4TsvmxnZPeYOHvYxXI9RgNC-VeR8Q; adid=173169335285853; analytic_id=1731693382619506; _ga_YVB4ZXMWPL=GS1.2.1731764757.1.1.1731765315.60.0.0; _ga=GA1.1.1903252348.1719430836; _pk_id.1.f5fe=739be507b513d787.1734287298.; __USER_ID_COOKIE_NAME__=173477373250003; __P__wuid=e9e879e0b9184ea1d0f3cef59ae944f3; stDeIdU=e9e879e0b9184ea1d0f3cef59ae944f3; __ddgid_=kqef4ZJd3nmCz7F5; flocktory-uuid=0241fd6a-c187-4d15-b067-6914fd7ea4c9-9; _ga_W0V3RXZCPY=GS2.1.s1748254899$o69$g0$t1748254899$j0$l0$h0; _ga_LN4Z31QGF4=GS2.1.s1748254899$o68$g0$t1748254899$j60$l0$h1954308688$ddGf82hx6-g98Aa9NAsjY4lKdL5u-b15wng; _ga_6JJPBGS8QY=GS2.1.s1748254899$o69$g0$t1748254899$j0$l0$h0; __ddg1_=NccemJ5EAdt5mQJHUWzi; _ym_d=1752911287; gdeslon.ru.__arc_domain=gdeslon.ru; gdeslon.ru.user_id=75110470-37f1-4bb5-ad12-750d369722c2; __ddg9_=89.169.48.165; __ddgmark_=AwUpOznqJrt8960s; __ddg5_=IpYxxNqs0IQiLV5J; __ddg2_=h1ncZ4olo86x2ewA; ddg_last_challenge=1757151513546; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTczMTk1MTYsImlhdCI6MTc1NzE1MTUxNiwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6ImI3NTdjNWQ5NWQ2ZDNmOTQyNjYxMDVlOGM4NDlhMTU1YTE5YTgwYjU0OGYxNzJiYzhjNDQyNjhlMTM2ZjliN2EiLCJ0eXBlIjoxMH0.JkEd3M1DCeQ5hcqNGPfbW8J7-fQwGllfSmDDOh3nnvM; tid-back-to=%7B%22fullPath%22%3A%22%2F%22%2C%22hash%22%3A%22%22%2C%22query%22%3A%7B%7D%2C%22name%22%3A%22index%22%2C%22path%22%3A%22%2F%22%2C%22params%22%3A%7B%7D%2C%22meta%22%3A%7B%7D%7D; utm_custom_source=default; tid-state=7c1f48ba-6f7d-4017-90c6-22f1c22fcd68; tid-redirect-uri=https%3A%2F%2Fwww.chitai-gorod.ru%2Fauth%2Ft-id-next; chg_visitor_id=48d8b642-4794-4d55-a13b-434fb38d0c9b; _pk_ses.1.f5fe=1; _ym_isad=2; vIdUid=33e05e06-7617-4ea1-857a-3198862c74a2; stLaEvTi=1757151517626; stSeStTi=1757151517625; _ym_visorc=w; adrdel=1757151517749; adrdel=1757151517749; acs_3=%7B%22hash%22%3A%221aa3f9523ee6c2690cb34fc702d4143056487c0d%22%2C%22nst%22%3A1757237917780%2C%22sl%22%3A%7B%22224%22%3A1757151517780%2C%221228%22%3A1757151517780%7D%7D; acs_3=%7B%22hash%22%3A%221aa3f9523ee6c2690cb34fc702d4143056487c0d%22%2C%22nst%22%3A1757237917780%2C%22sl%22%3A%7B%22224%22%3A1757151517780%2C%221228%22%3A1757151517780%7D%7D; domain_sid=L3BsIrNkQonH7entRBvC0%3A1757151518623; mindboxDeviceUUID=0660d298-673e-43fc-8993-474c6e6cd4c8; directCrm-session=%7B%22deviceGuid%22%3A%220660d298-673e-43fc-8993-474c6e6cd4c8%22%7D; tmr_detect=0%7C1757151520135; __ddg8_=R3UUqMEb4nerDrP0; __ddg10_=1757151532',
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
unique_article: dict[str, tuple] = {}  # article: (stock, price)


def get_link_from_ajax(article):
    selected_proxy = random.choice(PROXIES).strip()
    proxy = {
        "http": selected_proxy,
        "https": selected_proxy,
    }
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
                proxies=proxy,
            )
            response = resp.json()
            link = response["included"][0]["attributes"].get("url")
            return link
        except KeyError:
            continue
    return None


def get_book_data_from_ajax(book_url):
    selected_proxy = random.choice(PROXIES).strip()
    proxy = {
        "http": selected_proxy,
        "https": selected_proxy,
    }
    book_slug = book_url.split("/")[-1]
    response = requests.get(
        f"https://web-agr.chitai-gorod.ru/web/api/v1/products/slug/{book_slug}",
        headers=headers,
        cookies=cookies,
        proxies=proxy,
        timeout=15,
    )
    time.sleep(1)
    if response.status_code == 200:
        book_data = response.json().get("data")
        if book_data and book_data.get("status") == "canBuy":
            stock = book_data.get("availability")
            price = book_data.get("price")
            return stock, price
        return None
    else:
        logger.error(
            f"Error in ajax request - {response.status_code} | {response.text}"
        )
        raise Exception


def get_main_data(book_item):
    global unique_article
    if book_item["article"] in unique_article:  # check on parse was
        book_item["stock"] = unique_article[book_item["article"]][0]
        book_item["price"] = unique_article[book_item["article"]][1]
        return

    try:
        if not book_item["link"]:
            i_link = get_link_from_ajax(book_item["article"])
            if not i_link:
                book_item["stock"] = "0"
                book_item["price"] = None
                return
            book_item["link"] = f"{BASE_URL}/{i_link}"

        book_data = get_book_data_from_ajax(book_item["link"])
        if book_data:
            stock = book_data[0]
            price = book_data[1]
            if stock and price:
                book_item["stock"] = stock
                book_item["price"] = price
        else:
            book_item["stock"] = "0"

        unique_article[book_item["article"]] = (book_item["stock"], book_item["price"])

    except Exception as e:
        book_item["stock"] = "error"
        logger.exception(f"ERROR - {book_item['link']}")
    finally:
        global count
        print(f"\rDone - {count}", end="")
        count += 1


def get_auth_token():
    selected_proxy = random.choice(PROXIES).strip()
    target_url = "https://www.chitai-gorod.ru"
    proxy = {
        "http": selected_proxy,
        "https": selected_proxy,
    }
    resp = requests.get(
        target_url,
        headers=headers,
        cookies=cookies,
        timeout=15,
        allow_redirects=True,
        proxies=proxy,
    )
    time.sleep(5)
    if resp.status_code == 200:
        response_cookies = resp.cookies
        acc_token = (
            str(response_cookies["access-token"])
            .split("access-token=")[0]
            .split(";")[0]
            .replace("%20", " ")
        )
    elif resp.status_code == 403:
        response_cookies = parse_with_playwright_proxy(
            target_url=target_url, proxy_url=selected_proxy
        )
        acc_token = response_cookies.replace("%20", " ")
    print(acc_token)
    headers["Authorization"] = acc_token


def get_gather_data(sample):
    logger.info("Start collect data")
    print()
    get_auth_token()

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
            i["stock"] = "0"

    print()
    global count
    global unique_article
    count = 1
    unique_article.clear()
    logger.success("Finish collect data")


def main():
    try:
        # load_dotenv("../.env")
        books_in_sale = get_items_list("chit_gor")
        sample = give_me_sample(
            base_dir=BASE_LINUX_DIR, prefix="chit_gor", ozon_in_sale=books_in_sale
        )
        print(len(sample))
        get_gather_data(sample)

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
        df_del.to_excel(del_path, index=False, engine="openpyxl")

        df_without_del = df_result.loc[df_result["stock"] != "0"]
        new_stock_path = f"{BASE_LINUX_DIR}/chit_gor_new_stock.xlsx"
        df_without_del.to_excel(new_stock_path, index=False, engine="openpyxl")

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
