import gzip
import os
import pickle
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv
import schedule
from loguru import logger
import pandas.io.formats.excel
import asyncio
import pandas as pd
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from wb.wb_utils import prepare_to_daily_parse, push_stock_to_wb
from tg_sender import tg_send_files, tg_send_msg
from chit_utils import get_auth_token
from utils import give_me_sample, quantity_checker
from utils import PROXIES
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


def get_main_data(sample):
    body = {
        "include": "isbns",
        "forceFilters[categories]": "18030",
        "forceFilters[onlyNotOnSale]": "1",
        "product[status]": "canBuy",
        "customerCityId": "213",
        "products[page]": "1",
        "products[per-page]": "1000",
    }
    page_api_url = "https://web-agr.chitai-gorod.ru/web/api/v2/products"
    response = requests.get(
        page_api_url,
        params=body,
        headers=headers,
        # proxies=proxy,
    )
    page_count = response.json()["meta"]["pagination"]["total_pages"]
    for page in range(1, page_count + 1):
        time.sleep(0.3)
        try:
            page_response = requests.get(
                page_api_url,
                params=body,
                headers=headers,
                # proxies=proxy,
                timeout=(15, 60),
            )
            items_list = page_response.json()["data"]
            print(f"page - {page}")
            for shop in sample:
                for item in items_list:
                    if (
                        item["id"] in sample[shop]
                        and item["attributes"].get("status") == "canBuy"
                    ):
                        sample[shop][item["id"]]["stock"] = item["attributes"][
                            "quantity"
                        ]
                        sample[shop][item["id"]]["price"] = item["attributes"]["price"]
            body["products[page]"] = str(page + 1)
        except Exception as e:
            logger.exception(e)
            continue


def get_gather_data(sample):
    global error_count
    logger.info("Start collect data")
    print()
    acc_token = get_auth_token()
    headers["Authorization"] = acc_token

    dict_sample = {}

    for item in sample:
        article_for_key = (
            item["article"][:-2] if item["article"].endswith(".0") else item["article"]
        )
        dict_sample.setdefault(item["seller_id"], {})[article_for_key] = item

    get_main_data(dict_sample)

    sample_after_pars = []
    for item in dict_sample:
        for book_data in dict_sample[item]:
            try:
                dict_sample[item][book_data]["stock"] = int(
                    dict_sample[item][book_data]["stock"]
                )
            except (TypeError, ValueError):
                dict_sample[item][book_data]["stock"] = 0
            sample_after_pars.append(dict_sample[item][book_data])

    return sample_after_pars


def main():
    try:
        # load_dotenv("../.env")
        # ozon sample
        books_in_sale = get_items_list("chit_gor", ibra="all")
        sample = give_me_sample(
            base_dir=BASE_LINUX_DIR, prefix="chit_gor", ozon_in_sale=books_in_sale
        )

        # Создаем архив с книгами МСК для парса в
        msk_book = [i for i in sample if i["article"].startswith("m")]
        with gzip.open(
            f"{Path(__file__).parent.parent / "msk_books.pkl.gz"}", "wb"
        ) as f:
            pickle.dump(msk_book, f)

        # wb sample
        wb_sample = prepare_to_daily_parse(prefix="chit_gor")

        sample.extend(wb_sample)
        print(len(sample))
        new_daily_data = get_gather_data(sample)

        checker = quantity_checker(new_daily_data)
        if checker:
            wb_items = []
            ozon_items = []
            for i in sample:
                if i.get("marketplace") == "wb":
                    wb_items.append(i)
                else:
                    ozon_items.append(i)

            # Push to OZON with API
            ozon_separate_records = separate_records_to_client_id(ozon_items)
            logger.info("Start push to ozon")
            start_push_to_ozon(ozon_separate_records, prefix="chit_gor")
            logger.success("Data was pushed to ozon")

            # Push to WB with API
            for i in wb_items:
                if i["stock"] < 2:
                    i["stock"] = 0
            push_stock_to_wb(wb_items)

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
        # archive_items_stock_to_zero(prefix="chit_gor")
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
    super_main()
