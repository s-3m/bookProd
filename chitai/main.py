import random
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ozon.ozon_api import get_items_list
from utils import (
    check_danger_string,
    sync_fetch_request,
    write_result_files,
)
from filter import filtering_cover

pandas.io.formats.excel.ExcelFormatter.header_style = None
logger.add("chitai_error.log", format="{time} {level} {message}", level="ERROR")
DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.chitai-gorod.ru"
BASE_LINUX_DIR = "/media/source/chitai" if not DEBUG else "source"
semaphore = asyncio.Semaphore(10)

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
    time.sleep(random.uniform(0.5, 3))
    try:
        response = sync_fetch_request(link, headers, cookies)
        soup = bs(response, "lxml")

        try:
            title = soup.find("h1").text.strip()
            title = asyncio.run(check_danger_string(title, "title"))
            if not title:
                logger.warning(f"Delete DANGER book: {BASE_URL}{book_url}")
                return
        except:
            title = "Нет названия"

        try:
            author = soup.find("a", class_="product-info-authors__author").text.strip()
        except:
            author = "Нет автора"

        try:
            category_area = soup.find_all("li", class_="breadcrumbs__item")
            category = category_area[1].text.strip()
            sub_category = category_area[2].text.strip()
        except:
            category = "Категория не указана"
            sub_category = "Категория не указана"

        try:
            description = soup.find(
                "article", class_="detail-description__text"
            ).text.strip()
            description = asyncio.run(check_danger_string(description, "description"))
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
            stock_status = soup.find(
                "div", class_="offer-availability-status"
            ).text.strip()
        except:
            stock_status = None

        try:
            stock = soup.find("link", attrs={"itemprop": "availability"})
            if stock:
                stock = stock.next.strip()
        except:
            stock = None

        try:
            detail_section = soup.find("div", id="properties")
            detail_element = detail_section.find_all("li")

            detail_dict = {
                i.find_all("span")[0].text.strip(): i.find_all("span")[1].text.strip()
                for i in detail_element
            }
        except:
            detail_dict = None
        article = detail_dict["ID товара"] + ".0"
        book_result = {
            "Артикул_OZ": article,
            "Ссылка": link,
            "Название": title,
            "Автор": author,
            "Категория": category,
            "Подкатегория": sub_category,
            "Описание": description,
            "Фото": photo,
            "Цена": price,
            "Наличие": stock,
        }

        if detail_dict:
            book_result.update(detail_dict)

        # Filter on some piece
        count_edition: str = book_result.get("Тираж")
        quantity_page: str = book_result.get("Количество страниц")

        if not quantity_page:
            book_result["Количество страниц"] = "100"
        elif not quantity_page.isdigit():
            book_result["Количество страниц"] = count_edition.split(" ")[0]

        if not count_edition:
            book_result["Тираж"] = "1000"
        elif not count_edition.isdigit():
            book_result["Тираж"] = count_edition.split(" ")[0]

        # Cover filter
        cover_type = book_result.get("Тип обложки")
        book_result["Тип обложки"] = (
            filtering_cover(cover_type) if cover_type else "Мягкая обложка"
        )
        # ISBN filter
        isbn = book_result.get("ISBN")
        global last_isbn
        if isbn:
            last_isbn = isbn
        else:
            book_result["ISBN"] = last_isbn

            # Year filter
        publish_year = book_result.get("Год издания")
        if publish_year:
            if (
                "<2018" in publish_year
                or "< 2018" in publish_year
                or ">2024" in publish_year
                or "> 2024" in publish_year
                or len(publish_year) < 4
            ):
                book_result["Год издания"] = "2018"

        # Publisher filter
        publisher = book_result.get("Издательство")
        if not publisher:
            book_result["Издательство"] = "АСТ"

        online_option = soup.find("div", class_="product-offer-price")
        online_option_2 = soup.find("span", class_="offer-availability-status--green")
        in_shop_option = soup.find("p", class_="product-offer-header__title")
        not_in_option = soup.find("div", class_="detail-product__unavailable")
        if in_shop_option:
            moscow_shop_check = soup.find(
                "div", class_="product-offer-shops__title"
            ).text
            if "В наличии в" in moscow_shop_check:
                in_shop_option = True
                offline_price = soup.find(
                    "h5",
                    class_="product-offer-retail-title product-offer__title product-offer-retail-title--capitalize",
                )
                if offline_price:
                    offline_price = offline_price.text.strip().split(" ")[1].strip()
                    book_result["Цена"] = offline_price
                    book_result["Наличие"] = "Только в магазине"
                else:
                    book_result["Цена"] = "Цена не указана"

            else:
                in_shop_option = False

        avalible_status = True if online_option else False

        if article not in sample and avalible_status:
            id_to_add.append(book_result)

        all_books_result.append(book_result)

        global done_count
        done_count += 1
        print(
            f"\rDone - {done_count} | Item error - {len(item_error)} | Page error - {len(page_error)}",
            end="",
        )
    except Exception as e:
        logger.exception(f"Error - {link}")
        item_error.append(link)


page_to_stop = 4600


def get_page_data(book_category_link, page_number=1, reparse_url=False):
    global page_to_stop
    url = f"{book_category_link}?page={page_number}" if not reparse_url else reparse_url
    try:
        time.sleep(random.uniform(0.5, 3))
        response = sync_fetch_request(url, headers, cookies)
        soup = bs(response, "lxml")
        product_list = soup.find("div", class_="app-catalog__list")
        all_articles = product_list.find_all("article", class_="product-card")
        stop_count = 0
        with ThreadPoolExecutor(max_workers=3) as executor:
            for article in all_articles:
                buy_possibility = article.find(
                    "div", class_="chg-app-button__content"
                ).text.strip()
                book_link = article.find("a", class_="product-card__title")[
                    "href"
                ].strip()
                if buy_possibility == "Где купить?":
                    stop_count += 1
                if buy_possibility == "Купить":
                    executor.submit(get_book_data, book_link)

        if not reparse_url:
            if stop_count >= 48:
                page_to_stop = page_number
                logger.info(f"Stopped at page {page_number}")
    except Exception as e:
        logger.exception(f"Error on page - {url}")
        page_error.append(url)


async def get_gather_data():
    logger.info("Начинаю сбор данных")
    timeout = aiohttp.ClientTimeout(total=800)
    async with aiohttp.ClientSession(
        headers=headers,
        cookies=cookies,
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=timeout,
        trust_env=True,
    ) as session:
        for i in [f"{BASE_URL}/catalog/books-18030"]:
            logger.info(f"Start parsing {i}")
            async with session.get(i, headers=headers) as resp:
                soup = bs(await resp.text(), "lxml")
                max_pages = int(
                    soup.find_all("a", class_="chg-app-pagination__item")[-1].text
                )
                with ThreadPoolExecutor(max_workers=3) as executor:
                    for page in range(1, max_pages + 1):
                        if page > page_to_stop:
                            break
                        executor.submit(get_page_data, i, page, False)

        print()
        logger.success("Main data was collected")

        # Reparse item errors
        if item_error:
            logger.warning(f"Start reparse {len(item_error)} errors")
            new_item_list = item_error.copy()
            item_error.clear()
            with ThreadPoolExecutor(max_workers=3) as executor:
                for item in new_item_list:
                    executor.submit(get_book_data, item)

        # Reparse page errors
        if page_error:
            logger.warning(f"Start reparse {len(item_error)} pages errors")
            new_page_list = page_error.copy()
            page_error.clear()
            with ThreadPoolExecutor(max_workers=3) as executor:
                for url in new_page_list:
                    executor.submit(get_page_data, False, 1, url)

        logger.warning(
            f"Datas was collected. Not reparse: item errors - {len(item_error)} --- page errors - {len(page_error)}"
        )
        logger.info("Start write files")
        write_result_files(
            base_dir=BASE_LINUX_DIR,
            prefix="chit_gor",
            all_books_result=all_books_result,
            id_to_add=id_to_add,
        )
        logger.info("Finished write files")


@logger.catch
def main():
    asyncio.run(get_gather_data())
    logger.success("Script finished")


if __name__ == "__main__":
    main()
