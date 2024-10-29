import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select


def get_book_data(link):
    s = Service(executable_path=f'{os.path.dirname(os.path.dirname(os.path.realpath(__file__)))}/chromedriver.exe')
    o = webdriver.ChromeOptions()
    o.add_argument("--ignore-certificate-errors")
    o.add_argument('--allow-running-insecure-content')
    o.add_argument('--disable-blink-features=AutomationControlled')
    o.add_argument("--headless")
    o.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=s, options=o)

    try:
        driver.get(link)
        time.sleep(2)
        day_input = driver.find_element(By.XPATH, '/html/body/div/div/form/div[1]/div/div[1]/select')
        month_input = driver.find_element(By.XPATH, '/html/body/div/div/form/div[1]/div/div[2]/select')
        year_input = driver.find_element(By.XPATH, '/html/body/div/div/form/div[1]/div/div[3]/select')
        btn = driver.find_element(By.XPATH, '/html/body/div/div/form/div[2]/div[1]/input')

        select_day = Select(day_input)
        select_month = Select(month_input)
        select_year = Select(year_input)

        select_day.select_by_visible_text('25')
        time.sleep(1)
        select_month.select_by_visible_text('Июнь')
        time.sleep(1)
        select_year.select_by_visible_text('1985')
        time.sleep(1)

        btn.click()
        time.sleep(2)

        # driver.switch_to.window(driver.window_handles[0])
        page_source = driver.page_source

    finally:
        driver.close()
        driver.quit()

    return page_source
