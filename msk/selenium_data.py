import time
import subprocess
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
import undetected_chromedriver as uc
from loguru import logger
from selenium.webdriver.chrome.service import Service
from playwright.async_api import async_playwright

from webdriver_manager.chrome import ChromeDriverManager


def kill_chrome_processes():
    try:
        subprocess.run(["pkill", "-f", "chrome"], check=False)
        subprocess.run(["pkill", "-f", "chromedriver"], check=False)
        time.sleep(2)
    except:
        pass


async def pw_get_book_data(link):
    async with async_playwright() as pw:
        browser_type = pw.chromium

        context = await browser_type.launch_persistent_context(
            user_data_dir="./browser_data",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            headless=True,
        )
        # Добавляем stealth скрипты
        await context.add_init_script(
            """
            delete navigator.__proto__.webdriver;
            Object.defineProperty(navigator, 'webdriver', { 
                get: () => undefined 
            });
        """
        )

        new_page = await context.new_page()
        try:
            print("🔄 Переходим на сайт...")
            await new_page.goto(
                link,
                timeout=30000,
            )
            time.sleep(1)
            print("✅ Страница успешно загружена!")

            # Ждем загрузки формы
            await new_page.wait_for_selector("#age_verification_form")

            # Выбираем день: 1
            await new_page.select_option("#avf_day", value="1")

            # Выбираем месяц: 1 (Январь)
            await new_page.select_option("#avf_month", value="1")

            # Выбираем год: 1990
            await new_page.select_option("#avf_year", value="1990")

            # Нажимаем кнопку "Подтвердить"
            await new_page.click("input[type='submit'][value='Подтвердить']")

            # Ждем навигации или проверяем результат
            await new_page.wait_for_timeout(2000)  # Подождать 2 секунды

            return await new_page.content()

        except Exception as e:
            logger.exception(f"❌ Ошибка: {e}")
            return None


def get_book_data(link):
    kill_chrome_processes()
    try:
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=0")
        service = Service(ChromeDriverManager().install())
        driver = uc.Chrome(
            headless=True,
            use_subprocess=True,
            options=options,
            service=service,
        )

        try:
            driver.get(link)
            time.sleep(2)
            day_input = driver.find_element(
                By.XPATH, "/html/body/div/div/form/div[1]/div/div[1]/select"
            )
            month_input = driver.find_element(
                By.XPATH, "/html/body/div/div/form/div[1]/div/div[2]/select"
            )
            year_input = driver.find_element(
                By.XPATH, "/html/body/div/div/form/div[1]/div/div[3]/select"
            )
            btn = driver.find_element(
                By.XPATH, "/html/body/div/div/form/div[2]/div[1]/input"
            )

            select_day = Select(day_input)
            select_month = Select(month_input)
            select_year = Select(year_input)

            select_day.select_by_visible_text("25")
            time.sleep(1)
            select_month.select_by_visible_text("Июнь")
            time.sleep(1)
            select_year.select_by_visible_text("1985")
            time.sleep(1)

            btn.click()
            time.sleep(2)

            # driver.switch_to.window(driver.window_handles[0])
            page_source = driver.page_source
            return page_source

        finally:
            driver.close()
            driver.quit()
            time.sleep(2)
    except Exception as e:
        logger.exception("Неудачная попытка создать драйвер!")
        return None


if __name__ == "__main__":
    page = get_book_data("https://www.moscowbooks.ru/book/968695/")
    print(page)
