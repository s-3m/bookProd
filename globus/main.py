import sys
import os
from urllib import parse

import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import check_danger_string, filesdata_to_dict, fetch_request

pandas.io.formats.excel.ExcelFormatter.header_style = None
DEBUG = False
BASE_URL = "https://www.biblio-globus.ru"
BASE_LINUX_DIR = "/media/source/globus" if not DEBUG else "source"
logger.add(
    f"{BASE_LINUX_DIR}/result/globus_error.log",
    format="{time} {level} {message}",
    level="ERROR",
)
logger.add(
    f"{BASE_LINUX_DIR}/result/globus_error_serialize.json",
    format="{time} {level} {message}",
    level="ERROR",
    serialize=True,
)

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Connection": "keep-alive",
    # 'Cookie': '_ga=GA1.1.616419859.1731410744; _ym_uid=1731410745831810371; _ym_d=1731410745; RecentlyViewedProductsCookie=10979497%2c10902386; _ym_isad=2; _ym_visorc=w; _ga_XQBB831D6S=GS1.1.1731485055.4.0.1731485055.60.0.0',
    "Referer": "https://www.biblio-globus.ru/catalog/index/101",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36",
    "cache-control": "no-cache",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "YaBrowser";v="24.10", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

prices = filesdata_to_dict(f"{BASE_LINUX_DIR}/prices")
df_price_one = prices["1"]
df_price_two = prices["2"]
df_price_three = prices["3"]
sample = filesdata_to_dict(f"{BASE_LINUX_DIR}/sale", combined=True)
not_in_sale = filesdata_to_dict(f"{BASE_LINUX_DIR}/not_in_sale", combined=True)

all_books_result = []
id_to_del = []
id_to_add = []

done_count = 0

semaphore_ = asyncio.Semaphore(20)


async def get_book_data(session, book_link):
    async with semaphore_:
        link = f"{BASE_URL}{book_link}"
        try:
            response = await fetch_request(session, link, headers)
            soup = bs(response, "lxml")

            try:
                title = soup.find("h1").text.strip()
                title = await check_danger_string(title, "title")
                if not title:
                    logger.warning(f"Delete DANGER book: {BASE_URL}{book_link}")
                    return
            except:
                title = "Нет названия"

            # Автор
            try:
                author = soup.find("p", class_="goToDescription").text.strip()
            except:
                author = "Автор не указан"

            # Описание
            try:
                description = soup.find("div", id="collapseExample").text.strip()
                description = await check_danger_string(description, "description")
            except:
                description = "Нет описания"

            # Цена
            try:
                price_div = soup.find("div", class_="price_box").find_all(
                    "span", class_="price_new"
                )
                price = price_div[0].text.strip().split("\xa0")[0]
            except:
                price = "цена не указана"

            # Наличие
            try:
                stock = (
                    soup.find("div", class_="qtyInStock")
                    .find("span")
                    .text.strip()
                    .split(" ")[1]
                )
            except:
                stock = "Не указано"

            # Основные характеристики
            try:
                char_table = soup.find("table", class_="decor2")
                all_row = char_table.find_all("tr")
                main_char = {
                    i.find_all("td")[0]
                    .text.strip()
                    .split(":")[0]: i.find_all("td")[1]
                    .text.strip()
                    for i in all_row
                }
            except:
                main_char = {}

            # Дополнительные характеристики
            try:
                add_char_table = soup.find("table", class_="decor")
                all_add_row = add_char_table.find_all("td")
                add_char = {
                    i.text.split(": ")[0].strip(): i.text.split(": ")[1].strip()
                    for i in all_add_row
                }
            except:
                add_char = {}

            # ФОТО
            try:
                photo = soup.find("a", attrs={"data-fancybox": "gallery"}).get("href")
            except:
                photo = "Нет фото"

            # Гоавная категория
            try:
                category = soup.find_all("li", class_="breadcrumb-item")[
                    -1
                ].text.strip()
            except:
                category = "Категория не указана"

            main_char.update(add_char)

            # Item status
            item_status = (
                True
                if soup.find("p", class_="item-status").text.strip().lower()
                == "в наличии"
                else False
            )

            book_result = {
                "Ссылка": link,
                "Название": title,
                "Автор": author,
                "Описание": description,
                "Фото": photo,
                "Цена": price,
                "Наличие": stock,
                "Категория": category,
            }

            book_result.update(main_char)
            article = main_char["Артикул"] + ".0"
            for d in [df_price_one, df_price_two, df_price_three]:
                if article in d and item_status:
                    d[article]["price"] = price
                    break

            if article in not_in_sale and item_status:
                not_in_sale[article]["on sale"] = "да"
            if article not in sample and item_status:
                book_result["Артикул"] = article
                id_to_add.append(book_result)
            if article in sample and not item_status:
                book_result["Артикул"] = article
                id_to_del.append({"article": article})

            all_books_result.append(book_result)
            global done_count
            done_count += 1
            print(f"\rDone - {done_count}", end="")
        except Exception as e:
            logger.exception(f"Ошибка со страницей {book_link}")
            with open(f"{BASE_LINUX_DIR}/result/error.txt", "a+") as f:
                f.write(f"{book_link} --- {e}\n")


async def get_page_data(session, category_link):
    page_link = f"{BASE_URL}{category_link}"
    cat_number = page_link.split("/")[-1].strip()
    async with session.get(page_link, headers=headers) as resp:
        soup = bs(await resp.text(), "lxml")

        row_products = soup.find("div", class_="row products")
        if not row_products:
            return
        pagination = soup.find("ul", class_="pagination")
        max_page = 1
        if pagination:
            if pagination.find_all("li"):
                max_page_element = pagination.find_all("a", class_="page-link")[-1].get(
                    "href"
                )
                max_page = parse.parse_qs(parse.urlsplit(max_page_element).query).get(
                    "page"
                )[0]
                max_page = int(max_page)

        for page in range(1, max_page + 1):
            await asyncio.sleep(2)
            async with session.get(
                f"{BASE_URL}/catalog/category?id={cat_number}&page={page}",
                headers=headers,
            ) as resp:
                soup = bs(await resp.text(), "lxml")
                row_products = soup.find("div", class_="row products")
                all_books = row_products.find_all("div", class_="product")
                all_books_on_page = [
                    i.find("a").get("href")
                    for i in all_books
                    if i.find("span", class_="price_item_title")
                ]
                # for book_link in all_books_on_page:
                # await asyncio.sleep(2)
                if all_books_on_page:
                    tasks_ = [
                        asyncio.create_task(get_book_data(session, book_link))
                        for book_link in all_books_on_page
                    ]
                    await asyncio.gather(*tasks_)
                else:
                    break


async def checker(session, cat_link):
    async with session.get(f"{BASE_URL}{cat_link}", headers=headers) as resp:
        soup = bs(await resp.text(), "lxml")
        main_option = soup.find("div", class_="row products")
    return True if main_option else False


async def check_option(session, cat_link):
    async with session.get(f"{BASE_URL}{cat_link}", headers=headers) as resp:
        soup = bs(await resp.text(), "lxml")
        first_option = soup.find_all("a", class_="product-preview-title")
        second_option = soup.find_all("ul", id="catalogue")
        main_option = soup.find("div", class_="row products")

        if first_option:
            all_links = [i.get("href") for i in first_option]
            for first_option_link in all_links:
                if await checker(session, first_option_link):
                    await get_page_data(session, first_option_link)
                else:
                    await check_option(session, first_option_link)

        if second_option:
            all_second_links = []
            for ul in second_option:
                all_a = ["/catalog/index/" + i.get("href") for i in ul.find_all("a")]
                all_second_links.extend(all_a)

            for second_option_link in all_second_links:
                if await checker(session, second_option_link):
                    await get_page_data(session, second_option_link)
                else:
                    await check_option(session, second_option_link)

        if main_option:
            await get_page_data(session, cat_link)


async def collect_all_menu(session, menu_item_link):
    async with session.get(f"{BASE_URL}{menu_item_link}", headers=headers) as resp:
        soup = bs(await resp.text(), "lxml")
        big_items = soup.find("ul", id="catalogue").find_all("a")
        all_sub_cat = ["/catalog/index/" + i.get("href") for i in big_items]
    return all_sub_cat


@logger.catch
async def get_gather_data():
    logger.info("Начинаю сбор данных БИБЛИО-ГЛОБУС")
    semaphore = asyncio.Semaphore(8)
    timeout = aiohttp.ClientTimeout(total=800)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        logger.info("Формирование списка категорий")
        # async with semaphore:
        async with session.get(f"{BASE_URL}/catalog/index/4") as response:
            soup = bs(await response.text(), "lxml")
            li_menu = soup.find(
                "ul", class_="nav nav-pills flex-column category-menu"
            ).find_all("li", recursive=False)
            main_menu_links = [i.find("a")["href"] for i in li_menu]

        all_links = []

        tasks = [
            asyncio.create_task(collect_all_menu(session, menu_item))
            for menu_item in main_menu_links[2:4]
        ]
        await asyncio.gather(*tasks)
        logger.info(f"Список категорий сформирован")

        for i in tasks:
            all_links.extend(i.result())

        logger.info(all_links)
        logger.info("Начинаю сбор основных данных")
        print()

        # for cat_link in all_links:
        new_tasks = [
            asyncio.create_task(check_option(session, cat_link))
            for cat_link in all_links
        ]

        await asyncio.gather(*new_tasks)
        logger.success("Сбор данных завершён")

    logger.info("Начинаю запись данных в файл")
    pd.DataFrame(all_books_result).to_excel(
        f"{BASE_LINUX_DIR}/result/GLOBUS_all.xlsx", index=False
    )
    pd.DataFrame(id_to_add).to_excel(
        f"{BASE_LINUX_DIR}/result/GLOBUS_add.xlsx", index=False
    )
    pd.DataFrame(id_to_del).to_excel(
        f"{BASE_LINUX_DIR}/result/GLOBUS_del.xlsx", index=False
    )

    df_not_in_sale = pd.DataFrame().from_dict(not_in_sale, orient="index")
    df_not_in_sale.index.name = "article"
    df_not_in_sale.to_excel(f"{BASE_LINUX_DIR}/result/not_in_sale.xlsx")

    df_one = pd.DataFrame().from_dict(df_price_one, orient="index")
    df_one.index.name = "article"
    df_one.to_excel(f"{BASE_LINUX_DIR}/result/GLOBUS_price_one.xlsx")

    df_two = pd.DataFrame().from_dict(df_price_two, orient="index")
    df_two.index.name = "article"
    df_two.to_excel(f"{BASE_LINUX_DIR}/result/GLOBUS_price_two.xlsx")

    df_three = pd.DataFrame().from_dict(df_price_three, orient="index")
    df_three.index.name = "article"
    df_three.to_excel(f"{BASE_LINUX_DIR}/result/GLOBUS_price_three.xlsx")

    logger.success("Данные записаны в файлы")


if __name__ == "__main__":
    asyncio.run(get_gather_data())
