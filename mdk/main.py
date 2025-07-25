import json
import sys
import os

import pandas as pd
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
from loguru import logger

from filter import filtering_cover
from photo_utils import replace_photo

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import (
    check_danger_string,
    fetch_request,
    write_result_files,
    forming_add_files,
)

pandas.io.formats.excel.ExcelFormatter.header_style = None
DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://mdk-arbat.ru"
BASE_LINUX_DIR = "/media/source/mdk" if not DEBUG else "source"

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

all_books_result = []

done_count = 0
item_error = []
page_error = []
category_error = []

count = 1

logger.add("mdk_error.log", format="{time} {level} {message}", level="ERROR")
semaphore = asyncio.Semaphore(50)
unique_book_links = set()
last_isbn = None


async def get_item_data(session, book: str):
    global count
    if "?utm" in book:
        book = book.split("?")[0]
    link = book if book.startswith("http") else f"{BASE_URL}{book}"

    if link in unique_book_links:
        return

    try:
        response = await fetch_request(session, link, headers)
        if response == "503":
            return
        soup = bs(response, "lxml")

        in_arbat = soup.find("div", {"class": "shop_on_map", "data-id": "1"})
        if not in_arbat:
            return

        # Название книги
        try:
            title = soup.find("h1").text
            title = await check_danger_string(title, "title")
            if not title:
                return
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
            description = soup.find("p", {"class": "itempage-text"}).text.strip()
            description = await check_danger_string(description, "description")
        except:
            description = "Нет описания"

        # Наличие
        try:
            stock = soup.find("div", {"class": "tg-quantityholder"}).get("data-maxqty")
            stock = int(stock)
        except:
            stock = 0

        # Характеристики
        try:
            all_char = soup.find("ul", class_="tg-productinfo").find_all("li")
            char_data = {}
            for i in all_char:
                row = i.find_all("span")
                char_data[row[0].text.strip().replace(":", "")] = row[1].text.strip()
        except:
            char_data = {}

        article = char_data["Код товара"] + ".0"

        book_data = {
            "Ссылка": link,
            "Название": title,
            "Артикул_OZ": article,
            "Фото": photo,
            "Автор": author,
            "Цена": price,
            "Описание": description,
            "Наличие": str(stock),
        }
        book_data.update(char_data)

        count_edition: str = book_data.get("Тираж")
        quantity_page: str = book_data.get("Количество страниц")

        if not quantity_page:
            book_data["Количество страниц"] = "100"
        elif not quantity_page.isdigit():
            book_data["Количество страниц"] = count_edition.split(" ")[0]

        if not count_edition:
            book_data["Тираж"] = "1000"
        elif not count_edition.isdigit():
            book_data["Тираж"] = count_edition.split(" ")[0]

        # Cover filter
        cover = book_data.get("Переплет")
        if cover:
            cover = filtering_cover(cover)
            book_data["Переплет"] = cover
        else:
            book_data["Переплет"] = "Мягкая обложка"

        # ISBN filter
        global last_isbn
        isbn = book_data.get("ISBN")
        if isbn:
            last_isbn = isbn
        else:
            book_data["ISBN"] = last_isbn

        # Year filter
        publish_year = book_data.get("Год издания")
        if publish_year:
            if (
                "<2018" in publish_year
                or "< 2018" in publish_year
                or ">2024" in publish_year
                or "> 2024" in publish_year
                or len(publish_year) < 4
            ):
                book_data["Год издания"] = "2018"

        # Publisher filter
        publisher = book_data.get("Издательство")
        if not publisher:
            book_data["Издательство"] = "АСТ"

        if stock > 0:
            check_book_type = char_data.get("Вид товара")
            if check_book_type == "Книги":
                all_books_result.append(book_data)

        print(
            f"\rDone - {count} | Item error - {len(item_error)} | Page errors - {len(page_error)} | Category errors - {len(category_error)}",
            end="",
        )
        count += 1

    except (BaseException, Exception) as e:
        logger.exception(f"Error - {link}")
        item_error.append(link)


async def get_page_data(session, page_url):
    try:
        response = await fetch_request(session, page_url, headers)
        if response == "503":
            return
        soup = bs(response, "lxml")
        all_books_on_page = [
            i.find("a").get("href")
            for i in soup.find_all("div", {"class": "tg-postbook"})
        ]

        tasks = [
            asyncio.create_task(get_item_data(session, book))
            for book in all_books_on_page
        ]
        await asyncio.gather(*tasks)

    except Exception as e:
        page_error.append(page_url)
        logger.exception(f"Error on page - {page_url}")


async def get_category_data(session, category: str):
    cat_url = category if category.startswith("http") else f"{BASE_URL}{category}"
    try:
        response = await fetch_request(session, cat_url, headers)
        soup = bs(response, "lxml")
        is_data = soup.find("div", class_="tg-productgrid").find(
            "div", class_="tg-postbook"
        )
        if not is_data:
            return
        pagination = soup.find("nav", {"class": "tg-pagination"})
        if pagination:
            pagination = int(pagination.find_all("li")[-2].text)
        else:
            pagination = 1
        page_tasks = [
            asyncio.create_task(
                get_page_data(session, f"{BASE_URL}{category}&pid={page}")
            )
            for page in range(1, pagination + 1)
        ]
        await asyncio.gather(*page_tasks)
        # for page in range(1, pagination + 1):
        #     page_url = f"{BASE_URL}{category}&pid={page}"
        #     await get_page_data(session, page_url)

    except (BaseException, Exception) as e:
        logger.exception(f"Category Error with --- {cat_url}")
        category_error.append(cat_url)


def get_all_catalogs():
    with open("all_catalog.json") as file:
        cat_list = json.load(file)
        cat_list = [i for i in cat_list if int(i.split("=")[-1]) < 3259]
    return cat_list


@logger.catch
async def get_gather_data():
    logger.info("Начинаю сбор данных МДК")
    async with aiohttp.ClientSession(
        headers=headers, connector=aiohttp.TCPConnector(ssl=False, limit=10)
    ) as session:

        logger.info("Формирование списка категорий")
        all_categories = get_all_catalogs()
        logger.info(f"Найдено {len(all_categories)} категорий")
        logger.info(f"Начался сбор данных по категориям")

        for main_category in all_categories:
            await get_category_data(session, main_category)

        logger.info(f"Main data was collected")
        logger.warning(
            f"Find:\nCategory errors - {len(category_error)}\nPage errors - {len(page_error)}\nItem errors - {len(item_error)}"
        )

        if category_error:
            logger.warning(
                f"Start reparse categories errors. Find ---> {len(category_error)} error category"
            )
            cat_error_copy = category_error.copy()
            category_error.clear()
            for cat in cat_error_copy:
                await get_category_data(session, cat)

        if page_error:
            logger.warning(
                f"Start reparse page errors. Find ---> {len(page_error)} error page"
            )
            page_error_copy = page_error.copy()
            page_error.clear()
            for page in page_error_copy:
                await get_page_data(session, page)

        if item_error:
            logger.warning(
                f"Start reparse item errors. Find ---> {len(item_error)} errors"
            )
            item_error_copy = item_error.copy()
            item_error.clear()
            item_err = [
                asyncio.create_task(get_item_data(session, book_url))
                for book_url in item_error_copy
            ]
            await asyncio.gather(*item_err)

        # Replace photo

        all_books_result_df = pd.DataFrame(all_books_result)
        new_shops_df, old_shops_df = forming_add_files(
            result_df=all_books_result_df, prefix="mdk"
        )

        combined_df = pd.concat([new_shops_df, old_shops_df]).drop_duplicates()
        # Сброс индекса
        combined_df = combined_df.reset_index(drop=True)
        id_to_add = combined_df.to_dict("records")

        logger.info("Start write files")
        try:
            new_id_to_add_df = await replace_photo(id_to_add)
            new_id_to_add_df.set_index("Артикул_OZ", inplace=True)
            new_shops_df.set_index("Артикул_OZ", inplace=True)
            old_shops_df.set_index("Артикул_OZ", inplace=True)

            new_shops_df.update(new_id_to_add_df[["Фото"]])
            old_shops_df.update(new_id_to_add_df[["Фото"]])
            new_shops_df.reset_index(inplace=True)
            old_shops_df.reset_index(inplace=True)

            write_result_files(
                base_dir=BASE_LINUX_DIR,
                prefix="mdk",
                all_books_result=all_books_result,
                id_to_add=(new_shops_df, old_shops_df),
                replace_photo=True,
            )
        except Exception as e:
            logger.exception(e)
            write_result_files(
                base_dir=BASE_LINUX_DIR,
                prefix="mdk",
                all_books_result=all_books_result,
                id_to_add=[{}],
                replace_photo=False,
            )

        logger.success("Script finished successfully")


if __name__ == "__main__":
    asyncio.run(get_gather_data())
