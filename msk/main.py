import os
import sys
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio

from filter import filtering_cover
from selenium_data import get_book_data
from loguru import logger
import pandas.io.formats.excel

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import (
    check_danger_string,
    filesdata_to_dict,
    fetch_request,
    write_result_files,
)

pandas.io.formats.excel.ExcelFormatter.header_style = None

DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.moscowbooks.ru"
USER_AGENT = UserAgent()
BASE_LINUX_DIR = "/media/source/msk" if not DEBUG else "source"
semaphore = asyncio.Semaphore(10)
logger.add(
    f"{BASE_LINUX_DIR}/error.log",
    format="{time} {level} {message}",
    level="ERROR",
)
logger.add(
    f"{BASE_LINUX_DIR}/error_serialize.json",
    format="{time} {level} {message}",
    level="ERROR",
    serialize=True,
)
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}

count = 1
result = []
item_error = []
page_error = []
id_to_add = []

prices = filesdata_to_dict(f"{BASE_LINUX_DIR}/prices")
sample = filesdata_to_dict(f"{BASE_LINUX_DIR}/sale", combined=True)
not_in_sale = filesdata_to_dict(f"{BASE_LINUX_DIR}/not_in_sale", combined=True)

id_to_del = set(sample.keys())
last_isbn = None


async def check_empty_element(session, item, check_price=False):
    link = f"{BASE_URL}/book/{item["Артикул"][:-2]}"
    resp = await fetch_request(session, link, headers)
    soup = bs(resp, "lxml")

    if not check_price:
        item_status = soup.find("div", class_="book__buy")
        under_order = soup.find("div", class_="underorder")

        if item_status and not under_order:
            item["Статус"] = "В продаже"
        else:
            item["Статус"] = "Не в продаже"

    if check_price:
        price = soup.find("div", class_="book__price")
        if price:
            price = price.text.strip()
            item["price"] = price


async def get_item_data(session, item: str):
    global count
    link = item if item.startswith("https") else BASE_URL + item

    # Article
    try:
        article = link.split("/")[-2]
    except:
        article = "Нет артикула"

    try:
        resp = await fetch_request(session, link, headers)
        if resp == "404":
            return
        soup = bs(resp, "lxml")
        age_control = soup.find("input", id="age_verification_form_mode")
        script_index = 1
        if age_control:
            logger.warning("Age Control found")
            closed_page = get_book_data(link)
            soup = bs(closed_page, "lxml")
            script_index = 5

        # Book title
        try:
            title = soup.find("h1").text.strip()
            title = await check_danger_string(title, "title")
            if not title:
                return
        except:
            title = "Название не указано"

        # Book author
        try:
            author_div = soup.find("div", class_="page-header__author")
            author_list = [i.text.strip() for i in author_div.find_all("a")]
            author = ", ".join(author_list)
        except:
            author = "Нет автора"

        # Photo
        try:
            img = soup.find("div", class_="book__cover").find("a").get("href")
            img = f"{BASE_URL}{img}"
        except:
            img = "Нет фото"

        # Description
        try:
            description_list = [
                i.next_element.text.strip()
                for i in soup.find("div", class_="book__description").find_all("br")
                if i.next_element.text != ""
            ]
            description = "\n".join(description_list)
            description = await check_danger_string(description, "description")
        except:
            description = "Нет описания"

        # Category
        try:
            category_area = soup.find_all("li", class_="breadcrumbs__item")
            category = category_area[-3].text.strip()
            sub_category = category_area[-2].text.strip()
        except:
            category = "Без категории"

        # Price
        try:
            price = soup.find("div", class_="book__price")
            if price:
                price = price.text.strip()
        except:
            price = "Цена не указана"

        # Stock
        try:
            need_element = soup.find_all("script")
            a = (
                need_element[script_index]
                .text.split("MbPageInfo = ")[1]
                .replace("false", "False")
                .replace("true", "True")
            )
            need_data_dict = eval(a[:-1])["Products"][0]
            stock = need_data_dict["Stock"]
        except:
            stock = "Количество не указано"

        # Details
        try:
            all_details = soup.find_all("dl", class_="book__details-item")
            details_dict = {
                i.find_all()[0].text.strip().split(":")[0]: i.find_all()[1].text.strip()
                for i in all_details
            }
            isbn = details_dict.get("ISBN")
            publish_year = details_dict.get("Год издания")
            age = details_dict.get("Возраст")
            publisher = details_dict.get("Издательство")
            cover_type = details_dict.get("Тип обложки")
            if not cover_type:
                details_dict["Тип обложки"] = "Мягкая обложка"
            else:
                details_dict["Тип обложки"] = filtering_cover(cover_type)

            if not publisher:
                details_dict["Издательство"] = "АСТ"

            if age:
                if age[0] == "0":
                    details_dict["Возраст"] = "1+"
                elif "-" in age:
                    age = age.split("-")[0].strip() + "+"
                    details_dict["Возраст"] = age

            if publish_year:
                if (
                    "<2018" in publish_year
                    or "< 2018" in publish_year
                    or ">2024" in publish_year
                    or "> 2024" in publish_year
                    or len(publish_year) < 4
                ):
                    details_dict["Год издания"] = "2018"

            global last_isbn
            if isbn:
                last_isbn = isbn
            else:
                details_dict["ISBN"] = last_isbn
        except:
            details_dict = {}

        book_dict = {
            "Ссылка": link,
            "Артикул_OZ": article + ".0",
            "title": title,
            "author": author,
            "description": description,
            "Категория": category,
            "Подкатегория": category,
            "Цена": price,
            "Наличие": stock,
            "Фото": img,
        }
        book_dict.update(details_dict)

        article_for_check = article + ".0"
        item_status = soup.find("div", class_="book__shop-details")
        item_status = (
            item_status.find("span").text.lower().strip() if item_status else None
        )

        for d in prices:
            if article_for_check in prices[d] and price is not None:
                prices[d][article_for_check]["price"] = price

        if article_for_check in not_in_sale and item_status == "в наличии":
            not_in_sale[article_for_check]["on sale"] = "да"
        elif article_for_check not in sample and item_status == "в наличии":
            id_to_add.append(book_dict)
        if article_for_check in id_to_del and item_status == "в наличии":
            id_to_del.remove(article_for_check)

        result.append(book_dict)
        print(
            f"\rDone - {count} | Book errors - {len(item_error)} | Page errors - {len(page_error)}",
            end="",
        )
        count += 1
    except Exception as e:
        item_error.append(link)
        logger.exception(f"Exception in book - {link}")
        with open(f"{BASE_LINUX_DIR}/error.txt", "a+") as file:
            file.write(f"{link} ---> {e}\n")


async def get_page_data(session, page_link):
    async with semaphore:
        try:
            page_html = await fetch_request(session, page_link, headers)
            # page_html = await page_response.text()
            soup = bs(page_html, "lxml")
            all_books_on_page = soup.find_all("div", class_="catalog__item")
            all_items = [book.find("a")["href"] for book in all_books_on_page]
            page_tasks = [
                asyncio.create_task(get_item_data(session, item)) for item in all_items
            ]
            await asyncio.gather(*page_tasks)
        except Exception as e:
            page_error.append(page_link)
            logger.exception(f"Exception on page {page_link} - {e}")
            with open(f"{BASE_LINUX_DIR}/page_error.txt", "a+") as file:
                file.write(f"{page_link} + - + {e}")


async def get_gather_data():
    # semaphore = asyncio.Semaphore(10)
    logger.info("Start to collect data")
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit_per_host=10), trust_env=True
    ) as session:
        # response = await session.get(f"{BASE_URL}/books/", headers=headers)
        all_categories = [
            "https://www.moscowbooks.ru/books/",
            "https://www.moscowbooks.ru/books/exclusive-and-collective-editions/",
        ]
        for cat in all_categories:
            response = await fetch_request(session, cat, headers)
            soup = bs(response, "lxml")
            max_pagination = (
                soup.find("ul", class_="pager__list").find_all("li")[-2].text
            )
            tasks = []
            logger.info(f"Page find - {max_pagination}")
            for page in range(1, int(max_pagination) + 1):
                page_link = f"{cat}?page={page}"
                task = asyncio.create_task(get_page_data(session, page_link))
                tasks.append(task)
            await asyncio.gather(*tasks)

        # Reparse page errors
        if page_error:
            logger.warning("Reparse page errors")
            page_error_tasks = [
                asyncio.create_task(get_page_data(session, i)) for i in page_error
            ]
            await asyncio.gather(*page_error_tasks)

        # Reparse item errors
        if item_error:
            logger.warning("Reparse item errors")
            items_error_tasks = [
                asyncio.create_task(get_item_data(session, i)) for i in item_error
            ]
            await asyncio.gather(*items_error_tasks)

        # Reparse empty string
        # del file
        logger.info("Reparse del file")
        del_list = [{"Артикул": i} for i in id_to_del]
        reparse_del_tasks = [
            asyncio.create_task(check_empty_element(session, item)) for item in del_list
        ]
        await asyncio.gather(*reparse_del_tasks)

        # price files
        try:
            logger.info("Reparse empty price")
            for i_dict in prices:
                prices_tasks = [
                    asyncio.create_task(
                        check_empty_element(session, item, check_price=True)
                    )
                    for item in prices[i_dict]
                    if item["price"] == ""
                ]
                await asyncio.gather(*prices_tasks)
        except Exception as e:
            logger.exception(e)
            pass

    print()
    logger.info("Start to write data in file")
    write_result_files(
        base_dir=BASE_LINUX_DIR,
        prefix="msk",
        all_books_result=result,
        id_to_add=id_to_add,
        id_to_del=[i["Артикул"] for i in del_list if i["Статус"] == "Не в продаже"],
        not_in_sale=not_in_sale,
        prices=prices,
    )
    logger.success("Data was wrote in file successfully")


@logger.catch
def main():
    asyncio.run(get_gather_data())


if __name__ == "__main__":
    main()
