import sys
import os
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import check_danger_string


pandas.io.formats.excel.ExcelFormatter.header_style = None
DEBUG = True
BASE_URL = "https://mdk-arbat.ru"
BASE_LINUX_DIR = "/media/source/mdk"

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
count = 1

book_error = 0
category_error = 0


async def get_item_data(session, semaphore, book):
    global count
    link = f"{BASE_URL}{book}"
    try:
        async with semaphore:
            async with session.get(f"{BASE_URL}{book}", headers=headers) as resp:
                await asyncio.sleep(3)
                soup = bs(await resp.text(), "lxml")
                # Название книги
                try:
                    title = soup.find("h1").text
                    title = await check_danger_string(title, "title")
                    if not title:
                        return
                except:
                    title = "Нет названия"

                # Артикул
                try:
                    article = link.split("/")[-1]
                except:
                    article = "Нет артикла"

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
                    description = soup.find("p", {"class": "itempage-text"}).text.strip()
                    description = await check_danger_string(description, "description")
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
                    "link": link,
                    "title": title,
                    "article": article,
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

            print(f"\rDone - {count}", end="")
            count += 1
            all_books.append(book_data)
    except (BaseException, Exception) as e:
        with open(f"{BASE_LINUX_DIR}/full_pars/error.txt", "a+", encoding="utf-8") as f:
            f.write(f"{link} ----- {e}\n")
        global book_error
        book_error += 1


async def get_category_data(session, semaphore, category):
    try:
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
    except (BaseException, Exception) as e:
        with open(f"{BASE_LINUX_DIR}/category_error.txt", "a+", encoding="utf-8") as f:
            f.write(f"{BASE_URL}{category} ----- {e}\n")
        global category_error
        category_error += 1


async def get_gather_data():
    logger.info("Начинаю сбор данных МДК")
    semaphore = asyncio.Semaphore(10)
    tasks = []
    async with aiohttp.ClientSession(
        headers=headers, connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        logger.info("Формирование списка категорий")
        async with session.get(f"{BASE_URL}/catalog?subj_id=51") as response:
            soup = bs(await response.text(), "lxml")
            all_categories = [
                i.get("href")
                for i in soup.find("div", class_="tg-widgetcontent").find_all("a")
            ]

            logger.info(f"Найдено {len(all_categories)} категорий")
            logger.info(f"Начался сбор данных по категориям")

            for main_category in all_categories:
                task = asyncio.create_task(
                    get_category_data(session, semaphore, main_category)
                )
                tasks.append(task)
            await asyncio.gather(*tasks)
            pd.DataFrame(all_books).to_excel("mdk_all.xlsx", index=False)
    global category_error
    global book_error
    await tg_send_msg("МДК", [category_error, book_error])


if __name__ == "__main__":
    asyncio.run(get_gather_data())
