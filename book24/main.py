from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
import re
from loguru import logger
import polars as pl

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
all_books = []
count = 0
errors = []


def get_item_data(link):
    global count
    global errors
    item_data = {}
    full_url = BASE_URL + link
    item_data["Ссылка"] = full_url
    try:
        response = requests.get(full_url, headers=headers)
        soup = BeautifulSoup(response.text, "lxml")
        breadcrumbs = soup.find_all(class_="breadcrumbs__item")
        book_chapter = breadcrumbs[2].text.strip()
        unnecessary_chapters = [
            "Эксклюзивная продукция",
            "Канцтовары",
            "Календари 2025",
            "Сувениры. Аксессуары",
            "Новый год",
            "Хозтовары",
            "Подарочные сертификаты",
            "Аудиокниги",
        ]
        if book_chapter in [unnecessary_chapters]:
            return
        title = (
            soup.find("h1", class_="product-detail-page__title")
            .text.split(":")[-1]
            .strip()
        )
        item_data["Название"] = title
        char_area = soup.find("div", id="product-characteristic")
        dt = [i.text.strip() for i in char_area.find_all("dt")]
        dd = [i.text.strip() for i in char_area.find_all("dd")]
        full_chars = zip(dt, dd)
        for i in full_chars:
            if i[0] == "ISBN":
                item_data["ISBN"] = i[1]
                break

        status_btn = soup.find("div", class_="product-detail-page__sidebar")
        status = status_btn.find("span", class_="b24-btn__content").text.strip()
        if status == "Добавить в корзину":
            item_data["Статус"] = "в наличии"
        elif status == "Оформить предзаказ":
            item_data["Статус"] = "предзаказ"
        isbn = soup.find("meta", attrs={"itemprop": "isbn"}).get("content")
        item_data["ISBN"] = isbn

        all_scripts = soup.find_all("script")
        quantity = 0
        for i in all_scripts:
            if i.text.startswith("window.__NUXT__"):
                my_str = i.text.split("productInfo:")[1]
                match = re.search(r"quantity:(\d+)", my_str)
                if match:
                    quantity = int(match.group(1))
                break
        item_data["Остаток"] = quantity
        all_books.append(item_data)
        count += 1
        print(f"\rDone - {count} | errors - {len(errors)}", end="")
    except Exception as e:
        logger.exception(f"{full_url}")
        errors.append(link)


def get_page_data(page):
    response = requests.get(
        f"{BASE_URL}/catalog/page-{page}/?available=2", headers=headers
    )
    soup = BeautifulSoup(response.text, "lxml")
    all_items = soup.find_all("div", class_="product-list__item")
    items_list = [
        i.find("a").get("href")
        for i in all_items
        if i.find("span", class_="b24-btn__content").text.strip() == "В корзину"
    ]
    return items_list


def main():
    max_pagination = 8833
    for page in range(1, max_pagination + 1):
        items_list = get_page_data(page)
        with ThreadPoolExecutor(max_workers=5) as executor:
            result = [executor.submit(get_item_data, link) for link in items_list]

    if errors:
        with ThreadPoolExecutor(max_workers=5) as executor:
            result = [executor.submit(get_item_data, link) for link in errors]

    result_df = pl.DataFrame(all_books)
    result_df.write_excel("book24.xlsx", autofit=True)


if __name__ == "__main__":
    main()
