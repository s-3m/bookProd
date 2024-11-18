import re
import time
import sys
import os
import traceback
from urllib import parse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger

pandas.io.formats.excel.ExcelFormatter.header_style = None
logger.add("chitai_error.log", format="{time} {level} {message}", level="ERROR")
DEBUG = True
BASE_URL = "https://www.chitai-gorod.ru"
BASE_LINUX_DIR = "/media/source/chitai"

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en;q=0.9',
    'cache-control': 'no-cache',
    # 'cookie': '__ddg1_=pvduRZr2PWRQfgBSlf6h; refresh-token=; tmr_lvid=03935c7450b807684f9dcd65334067b0; tmr_lvidTS=1719430836103; _ym_uid=1719430836919836282; _ym_d=1719430836; gdeslon.ru.__arc_domain=gdeslon.ru; gdeslon.ru.user_id=15254033-97f3-4a90-ae4b-740f08cf987d; _bge_ci=BA1.1.1122769529.1719430836; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; flocktory-uuid=0241fd6a-c187-4d15-b067-6914fd7ea4c9-9; adrcid=A6I49NSU-aGn_FnD2kzLfwA; adrcid=A6I49NSU-aGn_FnD2kzLfwA; stDeIdU=c130f130-3401-4ce5-9052-788e665fbea5; _ymab_param=NOyyliya_BgJ0VOb3JL1PA1-1gyIOZRewTIGnSgxe-t2ci28PM-AMDZfHAuZzH4TsvmxnZPeYOHvYxXI9RgNC-VeR8Q; chg_visitor_id=470bb97a-014e-404d-9c3d-a67245b92f38; adid=173169335285853; analytic_id=1731693382619506; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; origem=cityads; deduplication_cookie=cityads; deduplication_cookie=cityads; _ga_YVB4ZXMWPL=GS1.2.1731764757.1.1.1731765315.60.0.0; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MzIwODY5NzcsImlhdCI6MTczMTkxODk3NywiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjA0ODlmNzEzNzQ4NjRiNjYxMGNiOGJmZTlmNTY1M2UxOWRkYzQ3NTAwMmZlNTc1MzNlYWViMzk0MGJhOGZlZTkiLCJ0eXBlIjoxMH0.Tq3QOvFYkYoTpnAT4MDH51OIaSENrWDhAYCpNpXbFPg; _ga=GA1.1.1903252348.1719430836; _ym_isad=2; acs_3=%7B%22hash%22%3A%22768a608b20ce960ff29026da95a81203ec583ad1%22%2C%22nextSyncTime%22%3A1732005380055%2C%22syncLog%22%3A%7B%22224%22%3A1731918980055%2C%221228%22%3A1731918980055%2C%221230%22%3A1731918980055%7D%7D; acs_3=%7B%22hash%22%3A%22768a608b20ce960ff29026da95a81203ec583ad1%22%2C%22nextSyncTime%22%3A1732005380055%2C%22syncLog%22%3A%7B%22224%22%3A1731918980055%2C%221228%22%3A1731918980055%2C%221230%22%3A1731918980055%7D%7D; adrdel=1731918980080; adrdel=1731918980080; domain_sid=L3BsIrNkQonH7entRBvC0%3A1731918980423; clickCityAdsID=7MRZ235eurZf323; epn_click_id=7MRZ235eurZf323; tmr_detect=0%7C1731918987403; __ddg9_=85.198.105.3; partner_name=cityads; mindboxDeviceUUID=0660d298-673e-43fc-8993-474c6e6cd4c8; directCrm-session=%7B%22deviceGuid%22%3A%220660d298-673e-43fc-8993-474c6e6cd4c8%22%7D; __ddg10_=1731930396; __ddg8_=Kj0vHcaaytcn7CsU; _ga_W0V3RXZCPY=GS1.1.1731930384.6.1.1731930398.0.0.0; _ga_6JJPBGS8QY=GS1.1.1731930384.6.1.1731930398.0.0.0; _ga_LN4Z31QGF4=GS1.1.1731930376.7.1.1731930404.32.0.1425492556',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "YaBrowser";v="24.10", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36',
}

all_books_result = []

done_count = 0


async def get_book_data(session, book_url):
    link = f"{BASE_URL}{book_url}"
    await asyncio.sleep(3)
    async with session.get(link, headers=headers) as resp:
        soup = bs(await resp.text(), 'lxml')

        try:
            title = soup.find("h1").text.strip()
        except:
            title = "Нет названия"

        try:
            author = soup.find("a", class_="product-info-authors__author").text.strip()
        except:
            author = "Автор не указан"

        try:
            category = soup.find_all("li", class_="product-breadcrumbs__item")[-1].text.strip()
        except:
            category = "Категория не указана"

        try:
            description = soup.find("article", class_="detail-description__text").text.strip()
        except:
            description = "Нет описания"

        try:
            photo = soup.find("img", class_="product-info-gallery__poster").get("src")
        except:
            photo = "Нет фото"

        try:
            price = soup.find("span", attrs={"itemprop": "price"}).get("content")
        except:
            price = "Цена не указана"

        try:
            stock = soup.find("div", class_="offer-availability-status").text.strip()
        except:
            stock = "Не указано"

        try:
            detail_section = soup.find("section", class_="detail-product__description-wrapper")
            details = detail_section.find_all("div", class_="product-detail-features__item")
            detail_dict = {
                i.find_all()[0].text.strip(): i.find_all()[1].text.strip()
                for i in details
            }
        except:
            detail_dict = None

        book_result = {
            "link": link,
            "title": title,
            "author": author,
            "category": category,
            "description": description,
            "photo": photo,
            "price": price,
            "stock": stock,
        }

        if detail_dict:
            book_result.update(detail_dict)

        all_books_result.append(book_result)

        global done_count
        done_count += 1
        print(f"\rDone - {done_count}", end="")


async def get_page_data(session, semaphore, page_number):
    async with semaphore:
        async with session.get(f"{BASE_URL}/catalog/books-18030?page={page_number}&sortPreset=newness",
                               headers=headers) as response:
            soup = bs(await response.text(), "lxml")
            product_list = soup.find("div", class_="products-list")
            all_articles = product_list.find_all("article", class_="product-card product-card product")
            for article in all_articles:
                buy_possibility = article.find("span", class_="action-button__text").text.strip()
                book_link = article.find("a", class_="product-card__title")["href"].strip()
                if buy_possibility == "Купить":
                    await get_book_data(session, book_link)


async def get_gather_data():
    logger.info("Начинаю сбор данных БИБЛИО-ГЛОБУС")
    semaphore = asyncio.Semaphore(10)
    timeout = aiohttp.ClientTimeout(total=800)
    async with aiohttp.ClientSession(
            headers=headers, connector=aiohttp.TCPConnector(ssl=False), timeout=timeout
    ) as session:
        async with session.get(f"{BASE_URL}/catalog/books-18030?page=1&sortPreset=newness", headers=headers) as resp:
            soup = bs(await resp.text(), "lxml")
            parse_city = soup.find("span", class_="header-city__title").text.strip()
            logger.info(f"City - {parse_city}")
            max_pages = int(soup.find_all("a", class_="pagination__button")[-2].text)
            tasks = []
            max_pages = 10    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            for page in range(1, max_pages + 1):
                await asyncio.sleep(3)
                task = asyncio.create_task(get_page_data(session, semaphore, page))
                tasks.append(task)
            await asyncio.gather(*tasks)
            print()
            logger.success("All data was collected")

            pd.DataFrame(all_books_result).to_excel("chitai_gorod.xlsx", index=False)


@logger.catch
def main():
    asyncio.run(get_gather_data())


if __name__ == '__main__':
    main()
