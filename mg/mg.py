import time
import sys
import os

import pandas as pd
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
from pprint import pprint
from fake_useragent import UserAgent
import aiohttp
import asyncio
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ozon.ozon_api import Ozon
from utils import (
    check_danger_string,
    fetch_request,
    write_result_files,
    forming_add_files,
)
from filter import filtering_cover

pandas.io.formats.excel.ExcelFormatter.header_style = None
DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.dkmg.ru"
BASE_LINUX_DIR = "/media/source/mg" if not DEBUG else "source"
logger.add(
    f"{BASE_LINUX_DIR}/error.log",
    format="{time} {level} {message}",
    level="ERROR",
)
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random,
}

result = []

count = 1
item_error = []
cat_error = []
unique_title = set()
ozon = Ozon("None", "None", "mg")


async def get_item_data(session, link: str):
    link = link if link.startswith("http") else f"{BASE_URL}{link}"
    try:
        item_data = {"Ссылка": link}
        response = await fetch_request(session, link, headers)
        soup = bs(response, "lxml")
        try:
            category_area = soup.find("div", class_="way")
            if category_area:
                all_cat_items = category_area.find_all("a")
                category = all_cat_items[-2].text.strip()
                sub_category = all_cat_items[-1].text.strip()
            item_data["Категория"] = category
            item_data["Подкатегория"] = sub_category
        except:
            item_data["Категория"] = "Нет категории"
            item_data["Подкатегория"] = "Нет подкатегории"
        try:
            title = soup.find("h1").text.strip()
            title = await check_danger_string(title, "title")
            if not title or title in unique_title:
                return
            item_data["Название"] = title
        except:
            item_data["Название"] = "Нет названия"
        try:
            options = soup.find("div", class_="item_basket_cont").find_all("tr")
            for option in options:
                item_data[option.find_all("td")[0].text.strip()[:-1]] = option.find_all(
                    "td"
                )[1].text.strip()
                if option.find_all("td")[0].text.strip() == "ISBN:":
                    isbn = option.find_all("td")[1].text.strip()
            try:
                additional_options = soup.find(
                    "div", class_="additional_information"
                ).find_all("tr")
                for option in additional_options:
                    item_data[option.find_all("td")[0].text.strip()[:-1]] = (
                        option.find_all("td")[1].text.strip()
                    )
            except:
                pass
        except:
            pass

        count_edition: str = item_data.get("Тираж")
        quantity_page: str = item_data.get("Страниц")

        if not quantity_page:
            item_data["Страниц"] = "100"
        elif not quantity_page.isdigit():
            item_data["Страниц"] = count_edition.split(" ")[0]

        if not count_edition:
            item_data["Тираж"] = "1000"
        elif not count_edition.isdigit():
            item_data["Тираж"] = count_edition.split(" ")[0]

        try:
            info = soup.find("div", class_="content_sm_2").find("h4")
            if info.text.strip() == "Аннотация":
                info = info.find_next().text.strip()
            else:
                info = "Описание отсутствует"
            info = await check_danger_string(info, "description")
            if len(info) < 5:
                item_data["description"] = "Автор рекомендует книгу к прочтению!"
            else:
                item_data["description"] = info
        except:
            item_data["description"] = "Автор рекомендует книгу к прочтению!"
        try:
            price = (
                soup.find_all("div", class_="product_item_price")[1]
                .text.strip()
                .split(".")[0]
            )
            price = ozon._price_calculate(input_price=price)
            price["price"] = price["price"][:-2]
            price["old_price"] = price["old_price"][:-2]
            if int(price["price"]) >= 60_000:
                return
            item_data["price"] = price["price"]
            item_data["old_price"] = price["old_price"]
        except Exception as e:
            logger.error(e)
            item_data["price"] = "0"
            item_data["old_price"] = "0"

        item_id = soup.find("div", class_="wish_list_btn_box").find(
            "a", class_="btn_desirable2 to_wishlist"
        )
        if item_id:
            item_id = item_id["data-tovar"]
            item_data["id"] = item_id
        try:
            quantity = soup.find("div", class_="wish_list_poz").text.strip()
            item_data["Наличие"] = quantity
        except:
            item_data["Наличие"] = "Наличие не указано"
        try:
            photo = soup.find("a", class_="highslide")["href"]
            photo = BASE_URL + photo
            if photo == "https://www.dkmg.ru/goods_img/no_photo.png":
                item_data["Фото"] = "https://zapobedu21.ru/images/26.07.2017/kniga.jpg"
            else:
                item_data["Фото"] = photo
        except:
            item_data["Фото"] = "Нет изображения"

        item_data["Артикул_OZ"] = isbn + ".0"

        # Cover filter
        cover_type = item_data.get("Тип обложки")
        if cover_type:
            item_data["Тип обложки"] = filtering_cover(cover_type)

        # Author filter
        item_data["Автор"] = (
            item_data["Автор"] if item_data.get("Автор") else "Нет автора"
        )
        # ISBN filter
        item_data["ISBN"] = (
            item_data["ISBN"] if item_data.get("ISBN") else "978-5-0000-0000-0"
        )
        # Publisher filter
        item_data["Издательство"] = (
            item_data["Издательство"] if item_data.get("Издательство") else "АСТ"
        )

        # Year filter
        publish_year = item_data.get("Год публикации")
        if publish_year:
            try:
                int_publish_year = int(publish_year)
                if int_publish_year < 2018:
                    item_data["Год публикации"] = "2018"
            except:
                item_data["Год публикации"] = "2018"
        else:
            item_data["Год публикации"] = "2018"

        # Age filter
        age = item_data.get("Возраст от")

        if not age or age == ":":
            item_data["Возраст от"] = "3+"
        elif "+" not in age:
            item_data["Возраст от"] = age + "+"

        if quantity == "есть в наличии":
            result.append(item_data)

        unique_title.add(title)
        global count
        print(
            f"\rDONE - {count} | Error item - {len(item_error)} | Page error - {len(cat_error)}",
            end="",
        )
        count += 1
    except Exception as e:
        logger.exception(link)
        item_error.append(link)


async def get_gather_data():
    tasks = []
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit_per_host=10)
    ) as session:
        response = await session.get(BASE_URL, headers=headers)
        response_text = await response.text()
        soup = bs(response_text, "lxml")
        cat_list = soup.find_all("h4")
        cat_list = [item.find("a")["href"] for item in cat_list[:8]]
        for cat_link in cat_list:
            try:
                url = BASE_URL + cat_link
                # response = await session.get(BASE_URL + cat_link, headers=headers)
                response = await fetch_request(session, url, headers)
                soup = bs(response, "lxml")
                pagin_max = int(
                    soup.find("div", class_="navitem")
                    .find_all("a")[-2]["href"]
                    .split("=")[-1]
                )
                main_category = soup.find("h1").text.split(" (")[0]
                logger.info(f"\n---Делаю категорию - {main_category}---")

                for page_numb in range(1, pagin_max + 1):
                    logger.info(
                        f"----------------стр - {page_numb} из {pagin_max}-----------"
                    )
                    # response = await session.get(
                    #     f"{BASE_URL}{cat_link}?page={page_numb}&orderNew=asc"
                    # )
                    page_url = f"{BASE_URL}{cat_link}?page={page_numb}&orderNew=asc"
                    response = await fetch_request(session, page_url, headers)
                    soup = bs(response, "lxml")
                    items_on_page = soup.find_all("div", class_="product_img")
                    items_links = [item.find("a")["href"] for item in items_on_page]
                    for link in items_links:
                        task = asyncio.create_task(get_item_data(session, link))
                        tasks.append(task)
            except Exception as e:
                cat_error.append(f"{BASE_URL}{cat_link}?page={page_numb}&orderNew=asc")
                continue
        await asyncio.gather(*tasks)

        if item_error:
            for link in item_error:
                item_error_tasks = [asyncio.create_task(get_item_data(session, link))]
            await asyncio.gather(*item_error_tasks)

        if cat_error:
            reparse_tasks = []
            for link in cat_error:
                try:
                    response = await fetch_request(session, link, headers)
                    soup = bs(response, "lxml")
                    items_on_page = soup.find_all("div", class_="product_img")
                    items_links = [item.find("a")["href"] for item in items_on_page]
                    for item_link in items_links:
                        task = asyncio.create_task(get_item_data(session, item_link))
                        reparse_tasks.append(task)
                except Exception as e:
                    continue
            await asyncio.gather(*reparse_tasks)

        logger.info("Start to write to excel")
        result_df = pd.DataFrame(result)
        new_shop_df, old_shop_df = forming_add_files(result_df=result_df, prefix="mg")
        write_result_files(
            base_dir=BASE_LINUX_DIR,
            prefix="mg",
            all_books_result=result,
            id_to_add=(new_shop_df, old_shop_df),
        )


def main():
    logger.info("Start parsing Gvardia")
    asyncio.run(get_gather_data())
    logger.info("Finish to write to excel")
    logger.success("Gvardia pars success")


if __name__ == "__main__":
    start_time = time.time()
    main()
    pprint(time.time() - start_time)
