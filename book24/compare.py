import json
import os
import random
import sys
import time
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import quantity_checker
import httpx
import schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from wb.wb_utils import prepare_to_daily_parse, push_stock_to_wb
import quickjs

DEBUG = True if sys.platform.startswith("win") else False
BASE_LINUX_DIR = "/media/source/book24" if not DEBUG else "source"
BASE_URL = "https://book24.ru"
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru,en;q=0.9",
    "cache-control": "max-age=0",
    "priority": "u=0, i",
    "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "YaBrowser";v="25.4", "Yowser";v="2.5"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 YaBrowser/25.4.0.0 Safari/537.36",
    # 'cookie': '__ddg1_=wi14AxcUAqZwKkb34CTW; BITRIX_SM_book24_visitor_id=73a71272-cb34-4343-b767-680909cb289c; _pk_id.2.e90c=5e28643edada1f1f.1751228673.; BITRIX_SM_location_name=%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0; BITRIX_SM_location_code=0c5b2444-70a0-4932-980c-b4dc0d3f02b5; BITRIX_SM_location_country=RU; BITRIX_SM_location_region_code=; gdeslon.ru.__arc_domain=gdeslon.ru; gdeslon.ru.user_id=5421dde8-1329-43b8-9c60-0a9dafb2ca2e; _ym_uid=1751228673873614473; _ym_d=1751228673; BITRIX_SM_location_coords=%5B%2255.75396%22%2C%2237.620393%22%5D; tmr_lvid=8b86f0a6c29c8954dbdecabf93356579; tmr_lvidTS=1751228673347; _ym_isad=2; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; flocktory-uuid=74e4b281-104c-4220-85e9-1dac4ad58a19-6; domain_sid=I-4HouGNYVTiqZK4ZwRM7%3A1751228674560; _ga=GA1.1.486362205.1751228675; COOKIES_ACCEPTED=Y; r2UserId=1751229291105099; analytic_id=1751229291114002; BITRIX_SM_location_accept=Y; _pk_ses.2.e90c=1; tmr_detect=0%7C1751268671369; _ga_0W6DM1HXWY=GS2.1.s1751268645$o2$g1$t1751268849$j60$l0$h0; mindboxDeviceUUID=13f1c2bf-99ab-4c15-86a4-fa986464686b; directCrm-session=%7B%22deviceGuid%22%3A%2213f1c2bf-99ab-4c15-86a4-fa986464686b%22%7D; __ddg10_=1751270663; __ddg9_=5.144.116.252; __ddg8_=uvk9erOC43xwlD2j',
}
page_done = 0


thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = httpx.Client(
            headers=headers,
            timeout=20,
            follow_redirects=True,
            verify=False,
            http2=False,
        )
    return thread_local.session


def mapping_nuxt(soup):
    nuxt_code = None
    for script in soup.find_all("script"):
        text = script.text
        if "window.__NUXT__=" in text:
            nuxt_code = text
            break
    ctx = quickjs.Context()
    ctx.eval("var window = {};")
    ctx.eval(nuxt_code)
    data = ctx.eval(
        """
    JSON.stringify(
        window.__NUXT__.fetch['CatalogPage:0'].products
    )
    """
    )
    data = json.loads(data)
    return data


def get_page_data(page):
    global page_done
    session = get_session()
    parse_data = {}
    response_text = None
    for _ in range(5):
        try:
            for i in range(5):
                time.sleep(random.uniform(0.4, 1))
                response = session.get(f"{BASE_URL}/catalog/page-{page}/?available=2")
                if response.status_code == 200:
                    response_text = response.text
                    break

            soup = BeautifulSoup(response_text, "lxml")
            page_data = mapping_nuxt(soup)

            for book in page_data:
                parse_data[str(book["id"])] = book["quantity"]

            page_done += 1
            print(f"\rDone - {page_done}", end="")
            return parse_data

        except Exception as e:
            logger.exception(f"Page error - {e} ")
            continue
    return None


def get_gather_data(sample):
    pages_data = {}
    session = httpx.Client(
        headers=headers, http2=True, timeout=20, follow_redirects=True, verify=False
    )
    pagination_response = session.get("https://book24.ru/catalog/?available=2")
    soup = BeautifulSoup(pagination_response.text, "lxml")
    max_pagination = soup.find_all("li", class_="pagination__button-item")[-2].text
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(get_page_data, page)
            for page in range(1, int(max_pagination) + 1)
        ]
    for future in as_completed(futures):
        pages_data.update(future.result())

    for item in sample:
        if item["article"][1:] in pages_data:
            item["stock"] = (
                pages_data[item["article"][1:]]
                if pages_data[item["article"][1:]] > 1
                else 0
            )
        else:
            item["stock"] = 0


def main():
    logger.info("Start script")
    # wb sample
    wb_sample = prepare_to_daily_parse(prefix="b24")

    get_gather_data(wb_sample)

    checker = quantity_checker(wb_sample)
    if not checker:
        logger.warning("Detected too many ZERO items")
        return

    # Push to WB with API
    push_stock_to_wb(wb_sample)


def super_main():
    load_dotenv("../.env")
    schedule.every().day.at("00:00").do(main)
    while True:
        schedule.run_pending()


if __name__ == "__main__":
    super_main()
