import os
import sys
import time
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from selenium_data import get_book_data
from loguru import logger
import pandas.io.formats.excel

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import check_danger_string

pandas.io.formats.excel.ExcelFormatter.header_style = None

BASE_URL = "https://www.moscowbooks.ru/"
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}
count = 1
result = []


async def get_item_data(session, item):
    global count
    link = BASE_URL + item
    try:
        async with session.get(link, headers=headers) as resp:
            soup = bs(await resp.text(), "lxml")
            age_control = soup.find("input", id="age_verification_form_mode")
            script_index = 1
            if age_control:
                logger.warning("Age Control found")
                closed_page = get_book_data(link)
                soup = bs(closed_page, "lxml")
                script_index = 5

            # Article
            try:
                article = link.split("/")[-1]
            except:
                article = "Нет артикула"
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
                category = soup.find_all("li", class_="breadcrumbs__item")[
                    -2
                ].text.strip()
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
                    i.find_all()[0].text.strip(): i.find_all()[1].text.strip()
                    for i in all_details
                }
            except:
                details_dict = {}

            book_dict = {
                "link": link,
                "article": article,
                "title": title,
                "author": author,
                "description": description,
                "category": category,
                "price": price,
                "stock": stock,
                "image": img,
            }
            book_dict.update(details_dict)

            result.append(book_dict)
            print(f"\rDone - {count}", end="")
            count += 1
    except Exception as e:
        logger.exception(f"Exception in book - {link}")
        with open("error.txt", "a+") as file:
            file.write(f"{link} ---> {e}\n")


async def get_gather_data():
    semaphore = asyncio.Semaphore(10)
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
        for page in range(1, int(max_pagination) + 1):
            async with semaphore:
                try:
                    page_response = await session.get(
                        f"{BASE_URL}/books/?page={page}", headers=headers
                    )
                    page_html = await page_response.text()
                    soup = bs(page_html, "lxml")
                    all_books_on_page = soup.find_all("div", class_="catalog__item")
                    all_items = [book.find("a")["href"] for book in all_books_on_page]
                    for item in all_items:
                        task = asyncio.create_task(get_item_data(session, item))
                        tasks.append(task)
                except Exception as e:
                    logger.exception(f"Exception on page {page} - {e}")
                    with open("page_error.txt", "a+") as file:
                        file.write(f"{page} + - + {e}")

        await asyncio.gather(*tasks)
        print()
        logger.success("Finish collect data")


@logger.catch
def main():
    logger.add(f"error.log", format="{time} {level} {message}", level="ERROR")
    asyncio.run(get_gather_data())
    logger.info("Start to write data in file")
    pd.DataFrame(result).to_excel("msk_result.xlsx", index=False)
    logger.success("Data was wrote in file successfully")


if __name__ == "__main__":
    main()
