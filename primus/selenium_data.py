import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

caps = DesiredCapabilities.CHROME
caps["pageLoadStrategy"] = "none"


def get_book_data(link):
    driver = uc.Chrome(headless=True, use_subprocess=False, version_main=131)

    try:
        driver.get(link)

        # WebDriverWait(driver, 10).until(
        #     lambda d: d.execute_script("return document.readyState") == "interactive"
        # )

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "[data-add-cart-counter-max-quantity")
            )
        )

        page_source = driver.page_source

    finally:
        driver.close()
        driver.quit()

    return page_source
