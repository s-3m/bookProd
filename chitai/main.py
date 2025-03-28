import sys
import os
from concurrent.futures import ThreadPoolExecutor
import pandas.io.formats.excel
from bs4 import BeautifulSoup as bs
import aiohttp
import asyncio
import pandas as pd
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import (
    check_danger_string,
    sync_fetch_request,
    filesdata_to_dict,
    write_result_files,
)
from filter import filtering_cover
from compare_selenium import get_gather_data as checker_del

pandas.io.formats.excel.ExcelFormatter.header_style = None
logger.add("chitai_error.log", format="{time} {level} {message}", level="ERROR")
DEBUG = True if sys.platform.startswith("win") else False
BASE_URL = "https://www.chitai-gorod.ru"
BASE_LINUX_DIR = "/media/source/chitai" if not DEBUG else "source"
semaphore = asyncio.Semaphore(10)

prices = filesdata_to_dict(f"{BASE_LINUX_DIR}/prices")
sample = filesdata_to_dict(f"{BASE_LINUX_DIR}/sale", combined=True)
not_in_sale = filesdata_to_dict(f"{BASE_LINUX_DIR}/not_in_sale", combined=True)

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru,en;q=0.9",
    "cache-control": "no-cache",
    # 'cookie': '__ddg1_=pvduRZr2PWRQfgBSlf6h; refresh-token=; tmr_lvid=03935c7450b807684f9dcd65334067b0; tmr_lvidTS=1719430836103; _ym_uid=1719430836919836282; _ym_d=1719430836; gdeslon.ru.__arc_domain=gdeslon.ru; gdeslon.ru.user_id=15254033-97f3-4a90-ae4b-740f08cf987d; _bge_ci=BA1.1.1122769529.1719430836; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; flocktory-uuid=0241fd6a-c187-4d15-b067-6914fd7ea4c9-9; adrcid=A6I49NSU-aGn_FnD2kzLfwA; adrcid=A6I49NSU-aGn_FnD2kzLfwA; stDeIdU=c130f130-3401-4ce5-9052-788e665fbea5; _ymab_param=NOyyliya_BgJ0VOb3JL1PA1-1gyIOZRewTIGnSgxe-t2ci28PM-AMDZfHAuZzH4TsvmxnZPeYOHvYxXI9RgNC-VeR8Q; chg_visitor_id=470bb97a-014e-404d-9c3d-a67245b92f38; adid=173169335285853; analytic_id=1731693382619506; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; tagtag_aid=ca8994e09ac019fbd41e0fc168321848; origem=cityads; deduplication_cookie=cityads; deduplication_cookie=cityads; _ga_YVB4ZXMWPL=GS1.2.1731764757.1.1.1731765315.60.0.0; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MzIwODY5NzcsImlhdCI6MTczMTkxODk3NywiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjA0ODlmNzEzNzQ4NjRiNjYxMGNiOGJmZTlmNTY1M2UxOWRkYzQ3NTAwMmZlNTc1MzNlYWViMzk0MGJhOGZlZTkiLCJ0eXBlIjoxMH0.Tq3QOvFYkYoTpnAT4MDH51OIaSENrWDhAYCpNpXbFPg; _ga=GA1.1.1903252348.1719430836; _ym_isad=2; acs_3=%7B%22hash%22%3A%22768a608b20ce960ff29026da95a81203ec583ad1%22%2C%22nextSyncTime%22%3A1732005380055%2C%22syncLog%22%3A%7B%22224%22%3A1731918980055%2C%221228%22%3A1731918980055%2C%221230%22%3A1731918980055%7D%7D; acs_3=%7B%22hash%22%3A%22768a608b20ce960ff29026da95a81203ec583ad1%22%2C%22nextSyncTime%22%3A1732005380055%2C%22syncLog%22%3A%7B%22224%22%3A1731918980055%2C%221228%22%3A1731918980055%2C%221230%22%3A1731918980055%7D%7D; adrdel=1731918980080; adrdel=1731918980080; domain_sid=L3BsIrNkQonH7entRBvC0%3A1731918980423; clickCityAdsID=7MRZ235eurZf323; epn_click_id=7MRZ235eurZf323; tmr_detect=0%7C1731918987403; __ddg9_=85.198.105.3; partner_name=cityads; mindboxDeviceUUID=0660d298-673e-43fc-8993-474c6e6cd4c8; directCrm-session=%7B%22deviceGuid%22%3A%220660d298-673e-43fc-8993-474c6e6cd4c8%22%7D; __ddg10_=1731930396; __ddg8_=Kj0vHcaaytcn7CsU; _ga_W0V3RXZCPY=GS1.1.1731930384.6.1.1731930398.0.0.0; _ga_6JJPBGS8QY=GS1.1.1731930384.6.1.1731930398.0.0.0; _ga_LN4Z31QGF4=GS1.1.1731930376.7.1.1731930404.32.0.1425492556',
    "priority": "u=0, i",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "YaBrowser";v="24.10", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36",
}

all_books_result = []
id_to_add = []
id_to_del = set(sample.keys())
new_del = []

done_count = 0
item_error = []
page_error = []
last_isbn = None


def get_book_data(book_url: str):
    link = book_url if book_url.startswith("http") else f"{BASE_URL}{book_url}"
    try:
        response = sync_fetch_request(link, headers)
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
            sale = soup.find("span", class_="product-offer-price__old-price")
            if sale:
                price = sale.text.strip()[:-1].strip()
            else:
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
            detail_section = soup.find(
                "section", class_="detail-product__description-wrapper"
            )
            details = detail_section.find_all(
                "div", class_="product-detail-features__item"
            )
            detail_dict = {
                i.find_all()[0].text.strip(): i.find_all()[1].text.strip()
                for i in details
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

        for d in prices:
            if article in prices[d] and avalible_status:
                prices[d][article]["price"] = book_result["Цена"]

        if article in not_in_sale and avalible_status:
            not_in_sale[article]["on sale"] = "да"
        elif article not in sample and avalible_status:
            id_to_add.append(book_result)
        if article in id_to_del and avalible_status:
            id_to_del.remove(article)

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
        response = sync_fetch_request(url, headers)
        soup = bs(response, "lxml")
        product_list = soup.find("div", class_="products-list")
        all_articles = product_list.find_all(
            "article", class_="product-card product-card product"
        )
        stop_count = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            for article in all_articles:
                buy_possibility = article.find(
                    "span", class_="action-button__text"
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
        connector=aiohttp.TCPConnector(ssl=False),
        timeout=timeout,
        trust_env=True,
    ) as session:
        for i in [f"{BASE_URL}/catalog/books-18030", f"{BASE_URL}/sales"]:
            logger.info(f"Start parsing {i}")
            async with session.get(i, headers=headers) as resp:
                soup = bs(await resp.text(), "lxml")
                parse_city = soup.find("span", class_="header-city__title").text.strip()
                logger.info(f"City - {parse_city}")
                max_pages = int(
                    soup.find_all("a", class_="pagination__button")[-2].text
                )
                with ThreadPoolExecutor(max_workers=5) as executor:
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
            with ThreadPoolExecutor(max_workers=5) as executor:
                for item in new_item_list:
                    executor.submit(get_book_data, item)

        # Reparse page errors
        if page_error:
            logger.warning(f"Start reparse {len(item_error)} pages errors")
            new_page_list = page_error.copy()
            page_error.clear()
            with ThreadPoolExecutor(max_workers=5) as executor:
                for url in new_page_list:
                    executor.submit(get_page_data, False, 1, url)

        write_result_files(
            base_dir=BASE_LINUX_DIR,
            prefix="chit_gor",
            all_books_result=all_books_result,
            id_to_add=id_to_add,
            id_to_del=id_to_del,
            not_in_sale=not_in_sale,
            prices=prices,
        )

        # Check del file
        logger.warning("Check del file")
        del_dict = [{"article": i, "stock": None, "link": None} for i in id_to_del]

        await checker_del(del_dict)

        global new_del
        for i in del_dict:
            if i["stock"] in (0, "0"):
                new_del.append(i["article"])

        logger.warning(
            f"Datas was collected. Not reparse: item errors - {len(item_error)} --- page errors - {len(page_error)}"
        )


@logger.catch
def main():
    asyncio.run(get_gather_data())
    logger.info("Start write files")
    write_result_files(
        base_dir=BASE_LINUX_DIR,
        prefix="chit-gor",
        all_books_result=all_books_result,
        id_to_add=id_to_add,
        id_to_del=new_del,
        not_in_sale=not_in_sale,
        prices=prices,
    )
    logger.info("Finished write files")
    logger.success("Script finished")


if __name__ == "__main__":
    main()
