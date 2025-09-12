import random
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor
import pandas.io.formats.excel
import requests
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
from loguru import logger
import pandas as pd

from chitai.main import sample

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from chitai.chit_utils import get_auth_token
from ozon.ozon_api import get_items_list
from utils import (
    check_danger_string,
    sync_fetch_request,
    write_result_files,
    exclude_else_shops_books,
)
from filter import filtering_cover

pandas.io.formats.excel.ExcelFormatter.header_style = None
logger.add("chitai_error.log", format="{time} {level} {message}", level="ERROR")
DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.chitai-gorod.ru"
BASE_LINUX_DIR = "/media/source/chitai" if not DEBUG else "source"

sample_raw = get_items_list("chit_gor", visibility="ALL")
sample = {i["Артикул"] for i in sample_raw}

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru,en;q=0.9",
    "cache-control": "max-age=0",
    # 'cookie': '__ddg1_=yWrh69GuCxAUjXSCodCf; refresh-token=; _ym_uid=172527752299912427; _ym_d=1733736779; _ga=GA1.1.11893149.1733736780; tmr_lvid=cd762e6000c9037a100c4ac95d961051; tmr_lvidTS=1725277523213; _ymab_param=PPhFvankcmpKDwmn7qscmms5XlJXLzkfnYP7_GsmlVspAc3CHvQJlRI9Xwv6ZSY733RwoAiNp3bWflRxg15sYwXo75c; gdeslon.ru.__arc_domain=gdeslon.ru; gdeslon.ru.user_id=db16c5ef-b01d-4aea-9cbd-88ad11af62f6; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; adrcid=AoR2yjfMnkaj8U98-uI46GA; adrcid=AoR2yjfMnkaj8U98-uI46GA; flocktory-uuid=1183ce97-9eef-4b5c-b717-2109aa9c8049-3; analytic_id=1733736783431262; _pk_id.1.f5fe=d968ec26cb54d349.1734072966.; adid=173683801139491; __P__wuid=7ced6ae9c151dda37671045d4e0bf2e9; stDeIdU=7ced6ae9c151dda37671045d4e0bf2e9; chg_visitor_id=9dd3ba69-35f3-4fc3-872a-a3f08a99dbd6; adrdel=1741588150140; adrdel=1741588150140; acs_3=%7B%22hash%22%3A%22be483547539f1e5fb43aa6ae1ea56ef0a5c5be24%22%2C%22nst%22%3A1741674550175%2C%22sl%22%3A%7B%22224%22%3A1741588150175%2C%221228%22%3A1741588150175%7D%7D; acs_3=%7B%22hash%22%3A%22be483547539f1e5fb43aa6ae1ea56ef0a5c5be24%22%2C%22nst%22%3A1741674550175%2C%22sl%22%3A%7B%22224%22%3A1741588150175%2C%221228%22%3A1741588150175%7D%7D; mindboxDeviceUUID=c6cfc9ac-0349-46cc-8b11-e2362a153d96; directCrm-session=%7B%22deviceGuid%22%3A%22c6cfc9ac-0349-46cc-8b11-e2362a153d96%22%7D; _ga_W0V3RXZCPY=GS1.1.1741588149.45.1.1741588166.0.0.0; _ga_LN4Z31QGF4=GS1.1.1741588149.47.1.1741588166.43.0.1146694035; _ga_6JJPBGS8QY=GS1.1.1741588150.45.1.1741588166.0.0.0; __ddgid_=tGw2PWBoCFqMPRO7; __ddg9_=185.112.249.83; __ddgmark_=fypt6YQQKSKH0JC8; __ddg5_=xlSx15KpL0isZOg6; __ddg2_=c6X5H58ic0WGzmtM; _ymab_param=PPhFvankcmpKDwmn7qscmms5XlJXLzkfnYP7_GsmlVspAc3CHvQJlRI9Xwv6ZSY733RwoAiNp3bWflRxg15sYwXo75c; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDM2NjY3MjAsImlhdCI6MTc0MzQ5ODcyMCwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjkzNmU0ZmQ2OTRmYmYyNjM4NDAyNWE4NGQ2MTgxY2FkNmNhZDdhOWJlOGJlOTk4OTkwODYyOTRjZDdhMjdkY2IiLCJ0eXBlIjoxMH0.ka53VDN2bKEHDGdbg2Wed4_toTQ7JVbBrGSav9yh450; vIdUid=a0457a4d-de09-40ec-8813-b45667dff8a3; stSeStTi=1743498723704; _pk_ref.1.f5fe=%5B%22%22%2C%22%22%2C1743498724%2C%22https%3A%2F%2Fwww.yandex.ru%2Fclck%2Fjsredir%3Ffrom%3Dyandex.ru%3Bsuggest%3Bbrowser%26text%3D%22%5D; _pk_ses.1.f5fe=1; tid-back-to=%7B%22fullPath%22%3A%22%2Fcatalog%2Fbooks-18030%22%2C%22hash%22%3A%22%22%2C%22query%22%3A%7B%7D%2C%22name%22%3A%22catalog-page%22%2C%22path%22%3A%22%2Fcatalog%2Fbooks-18030%22%2C%22params%22%3A%7B%22category%22%3A%22%22%2C%22slug%22%3A%22books%22%2C%22id%22%3A%2218030%22%7D%2C%22meta%22%3A%7B%7D%7D; tid-strategy=tinkoffWhiteLabel; tid-state=bb87dfff-bcd8-4558-b2a8-ca0a12303fe9; tid-redirect-uri=https%3A%2F%2Fwww.chitai-gorod.ru%2Fauth%2Ft-id-next; stLaEvTi=1743498731662; __ddg8_=s1NKwwEHse8c7IHU; __ddg10_=1743498736',
    "priority": "u=0, i",
    "referer": "https://www.chitai-gorod.ru/",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
}
cookies = {
    "__ddg1_": "yWrh69GuCxAUjXSCodCf",
    "refresh-token": "",
    "_ym_uid": "172527752299912427",
    "_ym_d": "1733736779",
    "_ga": "GA1.1.11893149.1733736780",
    "tmr_lvid": "cd762e6000c9037a100c4ac95d961051",
    "tmr_lvidTS": "1725277523213",
    "_ymab_param": "PPhFvankcmpKDwmn7qscmms5XlJXLzkfnYP7_GsmlVspAc3CHvQJlRI9Xwv6ZSY733RwoAiNp3bWflRxg15sYwXo75c",
    "gdeslon.ru.__arc_domain": "gdeslon.ru",
    "gdeslon.ru.user_id": "db16c5ef-b01d-4aea-9cbd-88ad11af62f6",
    "popmechanic_sbjs_migrations": "popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1",
    "adrcid": "AoR2yjfMnkaj8U98-uI46GA",
    "adrcid": "AoR2yjfMnkaj8U98-uI46GA",
    "flocktory-uuid": "1183ce97-9eef-4b5c-b717-2109aa9c8049-3",
    "analytic_id": "1733736783431262",
    "_pk_id.1.f5fe": "d968ec26cb54d349.1734072966.",
    "adid": "173683801139491",
    "__P__wuid": "7ced6ae9c151dda37671045d4e0bf2e9",
    "stDeIdU": "7ced6ae9c151dda37671045d4e0bf2e9",
    "chg_visitor_id": "9dd3ba69-35f3-4fc3-872a-a3f08a99dbd6",
    "adrdel": "1741588150140",
    "adrdel": "1741588150140",
    "acs_3": "%7B%22hash%22%3A%22be483547539f1e5fb43aa6ae1ea56ef0a5c5be24%22%2C%22nst%22%3A1741674550175%2C%22sl%22%3A%7B%22224%22%3A1741588150175%2C%221228%22%3A1741588150175%7D%7D",
    "acs_3": "%7B%22hash%22%3A%22be483547539f1e5fb43aa6ae1ea56ef0a5c5be24%22%2C%22nst%22%3A1741674550175%2C%22sl%22%3A%7B%22224%22%3A1741588150175%2C%221228%22%3A1741588150175%7D%7D",
    "mindboxDeviceUUID": "c6cfc9ac-0349-46cc-8b11-e2362a153d96",
    "directCrm-session": "%7B%22deviceGuid%22%3A%22c6cfc9ac-0349-46cc-8b11-e2362a153d96%22%7D",
    "_ga_W0V3RXZCPY": "GS1.1.1741588149.45.1.1741588166.0.0.0",
    "_ga_LN4Z31QGF4": "GS1.1.1741588149.47.1.1741588166.43.0.1146694035",
    "_ga_6JJPBGS8QY": "GS1.1.1741588150.45.1.1741588166.0.0.0",
    "__ddgid_": "tGw2PWBoCFqMPRO7",
    "__ddg9_": "185.112.249.83",
    "__ddgmark_": "fypt6YQQKSKH0JC8",
    "__ddg5_": "xlSx15KpL0isZOg6",
    "__ddg2_": "c6X5H58ic0WGzmtM",
    "_ymab_param": "PPhFvankcmpKDwmn7qscmms5XlJXLzkfnYP7_GsmlVspAc3CHvQJlRI9Xwv6ZSY733RwoAiNp3bWflRxg15sYwXo75c",
    "access-token": "Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDM2NjY3MjAsImlhdCI6MTc0MzQ5ODcyMCwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjkzNmU0ZmQ2OTRmYmYyNjM4NDAyNWE4NGQ2MTgxY2FkNmNhZDdhOWJlOGJlOTk4OTkwODYyOTRjZDdhMjdkY2IiLCJ0eXBlIjoxMH0.ka53VDN2bKEHDGdbg2Wed4_toTQ7JVbBrGSav9yh450",
    "vIdUid": "a0457a4d-de09-40ec-8813-b45667dff8a3",
    "stSeStTi": "1743498723704",
    "_pk_ref.1.f5fe": "%5B%22%22%2C%22%22%2C1743498724%2C%22https%3A%2F%2Fwww.yandex.ru%2Fclck%2Fjsredir%3Ffrom%3Dyandex.ru%3Bsuggest%3Bbrowser%26text%3D%22%5D",
    "_pk_ses.1.f5fe": "1",
    "tid-back-to": "%7B%22fullPath%22%3A%22%2Fcatalog%2Fbooks-18030%22%2C%22hash%22%3A%22%22%2C%22query%22%3A%7B%7D%2C%22name%22%3A%22catalog-page%22%2C%22path%22%3A%22%2Fcatalog%2Fbooks-18030%22%2C%22params%22%3A%7B%22category%22%3A%22%22%2C%22slug%22%3A%22books%22%2C%22id%22%3A%2218030%22%7D%2C%22meta%22%3A%7B%7D%7D",
    "tid-strategy": "tinkoffWhiteLabel",
    "tid-state": "bb87dfff-bcd8-4558-b2a8-ca0a12303fe9",
    "tid-redirect-uri": "https%3A%2F%2Fwww.chitai-gorod.ru%2Fauth%2Ft-id-next",
    "stLaEvTi": "1743498731662",
    "__ddg8_": "s1NKwwEHse8c7IHU",
    "__ddg10_": "1743498736",
}

all_books_result = []
id_to_add = []

done_count = 0
item_error = []
page_error = []
last_isbn = None


def get_book_data(book_url: str):
    link = book_url if book_url.startswith("http") else f"{BASE_URL}{book_url}"


def get_gather_data():
    logger.info("Начинаю сбор данных")
    jwt = get_auth_token()
    headers["Authorization"] = jwt

    body = {
        "include": "translators,illustrators,publisher,publisherBrand,manufacturer,manufacturerBrand,publisherSeries,productTexts,dates,isbns,binding,ageRestriction,printing,dimensions,files,rating,purchaseStats,seo,schoolProperties,tbks,ekns,literatureWorkCycle,literatureWorkCycleVolume,tags,commonProperties,bookProperties,stationeryProperties,comicProperties,gameProperties,constructorProperties,souvenirProperties,toyProperties,giftProperties,attendedForeignAgents",
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
    )
    page_count = response.json()["meta"]["total_pages"]
    for page in range(page_count + 1):
        new_books = []
        page_response = requests.get(page_api_url, params=body, headers=headers)
        items_list = page_response.json()["data"]
        for item in items_list:
            if item["id"] + ".0" not in sample:
                new_books.append(item["attributes"]["url"])

        if new_books:
            with ThreadPoolExecutor(max_workers=5) as executor:
                for book in new_books:
                    executor.submit(get_book_data, book)

    print()
    logger.success("Main data was collected")

    # Reparse item errors
    if item_error:
        logger.warning(f"Start reparse {len(item_error)} errors")
        new_item_list = item_error.copy()
        item_error.clear()
        with ThreadPoolExecutor(max_workers=5) as executor:
            for item in new_item_list:
                executor.submit(get_book_data, item)

    # Reparse page errors
    if page_error:
        logger.warning(f"Start reparse {len(page_error)} pages errors")
        new_page_list = page_error.copy()
        page_error.clear()
        with ThreadPoolExecutor(max_workers=2) as executor:
            for url in new_page_list:
                executor.submit(get_page_data, False, 1, url)

    logger.warning(
        f"Datas was collected. Not reparse: item errors - {len(item_error)} --- page errors - {len(page_error)}"
    )
    logger.info("Start write files")

    pure_add = exclude_else_shops_books(id_to_add, exclude_shop="chit")
    write_result_files(
        base_dir=BASE_LINUX_DIR,
        prefix="chit_gor",
        all_books_result=all_books_result,
        id_to_add=pure_add,
    )
    logger.info("Finished write files")

    # Тут исключаем книжки у МДК, т.к. ЧГ заканчивает парситься последним
    logger.info("Start to exclude MDK books")
    mdk_old = pd.read_excel("/media/source/mdk/result/mdk_add_old.xlsx").to_dict(
        orient="records"
    )
    mdk_new = pd.read_excel("/media/source/mdk/result/mdk_add_new.xlsx").to_dict(
        orient="records"
    )
    old_after_exclude = exclude_else_shops_books(mdk_old, exclude_shop="mdk")
    new_after_exclude = exclude_else_shops_books(mdk_new, exclude_shop="mdk")
    mdk_path = "/media/source/mdk/result"
    pd.DataFrame(old_after_exclude).to_excel(
        f"{mdk_path}/mdk_add_old.xlsx", engine="openpyxl", index=False
    )
    pd.DataFrame(new_after_exclude).to_excel(
        f"{mdk_path}/mdk_add_new.xlsx", engine="openpyxl", index=False
    )
    logger.info("MDK was excluded")


@logger.catch
def main():
    get_gather_data()
    logger.success("Script finished")


if __name__ == "__main__":
    main()
