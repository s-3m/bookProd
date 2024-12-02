import os
import sys
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
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

DEBUG = True
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
id_to_del = []

prices = filesdata_to_dict(f"{BASE_LINUX_DIR}/prices")
df_price_one = prices["1"]
df_price_two = prices["2"]
df_price_three = prices["3"]
sample = filesdata_to_dict(f"{BASE_LINUX_DIR}/sale", combined=True)
not_in_sale = filesdata_to_dict(f"{BASE_LINUX_DIR}/not_in_sale", combined=True)


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
            if article + ".0" in sample:
                id_to_del.append({"article": article + ".0"})
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
            author = "Автор не указан"

        # Photo
        try:
            img = soup.find("div", class_="book__cover").find("a").get("href")
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
            category = soup.find_all("li", class_="breadcrumbs__item")[-2].text.strip()
        except:
            category = "Без категории"

        # Price
        try:
            price = soup.find("div", class_="book__price").text.strip()
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
        except:
            details_dict = {}

        book_dict = {
            "link": link,
            "article": article + ".0",
            "title": title,
            "author": author,
            "description": description,
            "category": category,
            "price": price,
            "stock": stock,
            "image": img,
        }
        book_dict.update(details_dict)

        article_for_check = article + ".0"
        item_status = soup.find("div", class_="book__buy")
        for d in [df_price_one, df_price_two, df_price_three]:
            if article_for_check in d and item_status is not None:
                d[article_for_check]["price"] = price

        if article_for_check in not_in_sale and item_status is not None:
            not_in_sale[article_for_check]["on sale"] = "да"
        elif article_for_check not in sample and item_status is not None:
            id_to_add.append(book_dict)
        elif article_for_check in sample and item_status is None:
            id_to_del.append({"article": article_for_check})

        result.append(book_dict)
        print(f"\rDone - {count}", end="")
        count += 1
    except Exception as e:
        item_error.append(link)
        logger.exception(f"Exception in book - {link}")
        with open(f"{BASE_LINUX_DIR}/error.txt", "a+") as file:
            file.write(f"{link} ---> {e}\n")


async def get_page_data(session, page_link, tasks):
    async with semaphore:
        try:
            page_html = await fetch_request(session, page_link, headers)
            # page_html = await page_response.text()
            soup = bs(page_html, "lxml")
            all_books_on_page = soup.find_all("div", class_="catalog__item")
            all_items = [book.find("a")["href"] for book in all_books_on_page]
            for item in all_items:
                task = asyncio.create_task(get_item_data(session, item))
                tasks.append(task)
        except Exception as e:
            page_error.append(page_link)
            logger.exception(f"Exception on page {page_link} - {e}")
            with open(f"{BASE_LINUX_DIR}/page_error.txt", "a+") as file:
                file.write(f"{page_link} + - + {e}")


async def get_gather_data():
    # semaphore = asyncio.Semaphore(10)
    logger.info("Start to collect data")
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False), trust_env=True
    ) as session:
        response = await session.get(f"{BASE_URL}/books/", headers=headers)
        response_text = await response.text()
        soup = bs(response_text, "lxml")
        max_pagination = soup.find("ul", class_="pager__list").find_all("li")[-2].text
        tasks = []
        logger.info(f"Page find - {max_pagination}")
        # max_pagination = 30
        for page in range(1, int(max_pagination) + 1):
            page_link = f"{BASE_URL}/books/?page={page}"
            await get_page_data(session, page_link, tasks)
        await asyncio.gather(*tasks)

        # Reparse page errors
        page_error_tasks = []
        if page_error:
            logger.warning("Reparse page errors")
            for i in page_error:
                await get_page_data(session, i, page_error_tasks)
            await asyncio.gather(*page_error_tasks)

        # Reparse item errors
        items_error_tasks = []
        if item_error:
            logger.warning("Reparse item errors")
            for i in item_error:
                task = asyncio.create_task(get_item_data(session, i))
                items_error_tasks.append(task)
            await asyncio.gather(*items_error_tasks)

        print()
        logger.success("Finish collect data")


@logger.catch
def main():
    asyncio.run(get_gather_data())
    logger.info("Start to write data in file")
    write_result_files(
        base_dir=BASE_LINUX_DIR,
        prefix="msk",
        all_books_result=result,
        id_to_add=id_to_add,
        id_to_del=id_to_del,
        not_in_sale=not_in_sale,
        df_price_one=df_price_one,
        df_price_two=df_price_two,
        df_price_three=df_price_three,
    )
    logger.success("Data was wrote in file successfully")


if __name__ == "__main__":
    main()
