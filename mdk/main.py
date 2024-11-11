import time
import sys
import os

from unicodedata import category

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from pprint import pprint
from fake_useragent import UserAgent
import aiohttp
import asyncio
import pandas as pd
from loguru import logger
from utils import filesdata_to_dict


DEBUG = True
BASE_URL = "https://mdk-arbat.ru"


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

all_books = []


async def get_item_data(session, semaphore, book):
    async with semaphore:
        async with session.get(f"{BASE_URL}{book}", headers=headers) as resp:
            soup = bs(await resp.text(), "lxml")
            # Название книги
            try:
                title = soup.find("h1").text
            except:
                title = "Нет названия"

            # Фото
            try:
                photo = soup.find("figure").get("href")
            except:
                photo = "Нет фото"

            # Авторы
            try:
                author_list = [
                    i.text for i in soup.find_all("a", {"class": "itempage-author"})
                ]
                author = " ".join(author_list)
            except:
                author = "Нет автора"

            # Цена
            try:
                price = soup.find("span", {"class": "itempage-price_inet"}).text[:-1]
            except:
                price = "Нет цены"

            # Описание
            try:
                description = soup.find("p", {"class": "itempage-text"}).text
            except:
                description = "Нет описания"

            # Наличие
            try:
                stock = soup.find("div", {"class": "tg-quantityholder"}).get(
                    "data-maxqty"
                )
            except:
                stock = "Кол-во не указано"

            book_data = {
                "link": f"{BASE_URL}{book}",
                "title": title,
                "photo": photo,
                "author": author,
                "price": price,
                "description": description,
                "stock": stock,
            }
            # Характеристики
            try:
                all_char = soup.find("ul", class_="tg-productinfo").find_all("li")
                for i in all_char:
                    row = i.find_all("span")
                    book_data[row[0].text] = row[1].text
            except:
                pass

        all_books.append(book_data)


async def get_category_data(session, semaphore, category):

    async with session.get(f"{BASE_URL}{category}", headers=headers) as resp:
        soup = bs(await resp.text(), "lxml")
        pagination = soup.find("nav", {"class": "tg-pagination"})
        if pagination:
            pagination = int(pagination.find_all("li")[-2].text)
        else:
            pagination = 1
        for page in range(1, pagination + 1):
            async with session.get(
                f"{BASE_URL}{category}&pid={page}", headers=headers
            ) as resp:
                soup = bs(await resp.text(), "lxml")
                all_books_on_page = [
                    i.find("a").get("href")
                    for i in soup.find_all("div", {"class": "tg-postbook"})
                ]
            for book in all_books_on_page:
                await get_item_data(session, semaphore, book)


async def take_all_category(
    session, all_category, cat_list: list | None = None, base_padding=15
):
    if cat_list is None:
        cat_list = []
    for category in all_category:
        async with session.get(f"{BASE_URL}{category}", headers=headers) as resp:
            soup = bs(await resp.text(), "lxml")
            categories_li = [
                i
                for i in soup.find("div", class_="tg-widgetcontent").find_all("li")
                if int(i.get("style").split(":")[1][:2]) > base_padding
            ]
            if categories_li:
                new_categories = [li.find("a").get("href") for li in categories_li]
                cat_list.extend(new_categories)
                base_padding += 15
                await take_all_category(session, new_categories, cat_list, base_padding)
            else:
                continue

    return cat_list


async def get_gather_data():
    semaphore = asyncio.Semaphore(10)
    tasks = []
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(f"{BASE_URL}/catalog?subj_id=51") as response:
            soup = bs(await response.text(), "lxml")
            all_categories = [
                i.get("href")
                for i in soup.find("div", class_="tg-widgetcontent").find_all("a")
            ]

            # additionally_category = await take_all_category(session, all_categories)

            for main_category in all_categories:
                task = asyncio.create_task(
                    get_category_data(session, semaphore, main_category)
                )
                tasks.append(task)
            await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(get_gather_data())
