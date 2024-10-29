import os

from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import aiohttp
import asyncio
import pandas as pd

from compare import get_compare
from selenium_data import get_book_data

BASE_URL = "https://www.moscowbooks.ru/"
USER_AGENT = UserAgent()
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "user-agent": USER_AGENT.random
}

result = {}
count = 1

cookies = {
    'ASP.NET_SessionId': 'e4t3auauxqqhc2zki3wcudq2',
}


async def get_item_data(session, link, parse_error=False):
    global count
    try:
        async with session.get(link) as response:
            await asyncio.sleep(15)
            soup = bs(await response.text(), "lxml")
            age_control = soup.find('input', id='age_verification_form_mode')
            script_index = 1
            if age_control:
                closed_page = get_book_data(link)
                soup = bs(closed_page, "lxml")
                script_index = 5
            need_element = soup.find_all('script')
            a = need_element[script_index].text.split('MbPageInfo = ')[1].replace('false', 'False').replace('true',
                                                                                                            'True')
            need_data_dict = eval(a[:-1])['Products'][0]
            stock = need_data_dict['Stock']
            article= link.split('/')[-2]
            # all_details = soup.find_all('dl', class_='book__details-item')
            # for detail in all_details:
            #     detail = detail.find_all('dt')
            #     if detail[0].text.strip() == 'ISBN:':
            #         isbn = detail[1].text.strip()

            article = article + '.0'
            # item_data['Наличие'] = stock


            print(f'\r{count}', end='')
            count = count + 1

            result[article] = {'Наличие': stock}

    except Exception as e:
        if parse_error:
            with open('error.txt', 'a+') as file:
                file.write(f'{link} ------ reparse error ------ {e}\n')
        else:
            with open('error.txt', 'a+') as file:
                file.write(f'{link} ------ {e}\n')


async def reparse_error(session):
    reparse_count = 0
    reparse_tasks = []
    error_file = 'error.txt'
    try:
        while True:
            if not os.path.exists(error_file) or reparse_count > 10:
                break
            else:
                with open('error.txt', 'r') as file:
                    error_links_list = [i.split(' ------ ')[0] for i in file.readlines()]
                    os.remove('error.txt')
                if error_links_list:
                    for link in error_links_list:
                        task = asyncio.create_task(get_item_data(session, link, parse_error=True))
                        reparse_tasks.append(task)
                    await asyncio.gather(*reparse_tasks)
                    reparse_count += 1
    except:
        pass


tasks = []


async def create_item_task(session, full_link, page_count):
    for page in range(1, int(page_count) + 1):
        # for page in range(1, 3):
        try:
            page_response = await session.get(
                f'{full_link}?sortby=date&sortdown=true&page={page}')
            page_html = await page_response.text()
            soup = bs(page_html, "lxml")
            all_books_on_page = soup.find_all('div', class_='catalog__item')
            all_items = [book.find('a')['href'] for book in all_books_on_page]
            for item in all_items:
                link = f'{BASE_URL}{item}' if not item.startswith('/') else f'{BASE_URL}{item[1:]}'
                task = asyncio.create_task(get_item_data(session, link))
                tasks.append(task)
        except Exception as e:
            with open('error_page.txt', 'a+') as file:
                file.write(f"{full_link} --- page {page} --- {e}\n")
            continue


async def get_gather_data():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), trust_env=True,
                                     cookies=cookies) as session:

        big_categories = ['/books/', '/books/exclusive-and-collective-editions/', '/bookinist/', '/gift_book/']

        for big_category in big_categories:

            if big_category == '/books/':
                response = await session.get(f'{BASE_URL}/books/', headers=headers)
                response_text = await response.text()
                soup = bs(response_text, "lxml")
                soup_categories = soup.find('ul', class_='aside-nav__list').find_all('a')
                categories_links = [link['href'] for link in soup_categories]
                categories_links.append('/books/office-and-other/magazines-newspapers/')

                for category_link in categories_links:
                    response = await session.get(BASE_URL + category_link[1:], headers=headers)
                    cat_resp_text = await response.text()
                    cat_soup = bs(cat_resp_text, "lxml")
                    max_pagination = cat_soup.find('ul', class_='pager__list').find_all('li')[-2].text
                    if not max_pagination:
                        max_pagination = 1
                    full_link = f'{BASE_URL}{category_link}'
                    await create_item_task(session, full_link, max_pagination)

            elif big_category == '/gift_book/':
                response = await session.get(f"{BASE_URL}{big_category}")
                response_text = await response.text()
                soup = bs(response_text, "lxml")
                all_cat = soup.find('div', class_='catalog__list').find_all('a')
                all_cat_list = [i.get('href') for i in all_cat if i.get('href') is not None]
                for category_link in all_cat_list:
                    response = await session.get(f'{BASE_URL}{category_link}')
                    response_text = await response.text()
                    cat_soup = bs(response_text, "lxml")
                    max_pagination = cat_soup.find('ul', class_='pager__list')
                    if not max_pagination:
                        max_pagination = 1
                    else:
                        max_pagination = max_pagination.find_all('li')[-2].text
                    full_link = f'{BASE_URL}{category_link}'
                    await create_item_task(session, full_link, max_pagination)

            else:
                response = await session.get(f"{BASE_URL}{big_category}")
                response_text = await response.text()
                cat_soup = bs(response_text, "lxml")
                max_pagination = cat_soup.find('ul', class_='pager__list').find_all('li')[-2].text
                full_link = f'{BASE_URL}{big_category}'
                await create_item_task(session, full_link, max_pagination)

        await asyncio.gather(*tasks)
        await reparse_error(session)


def main():
    asyncio.run(get_gather_data())

    df = pd.DataFrame().from_dict(result, orient='index')
    df.index.name = 'Артикул'
    df.to_excel(f'new_result.xlsx')


    asyncio.run(get_compare(result))


if __name__ == "__main__":
    main()
