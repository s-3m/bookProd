import time
import subprocess
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
import undetected_chromedriver as uc


def kill_chrome_processes():
    try:
        subprocess.run(["pkill", "-f", "chrome"], check=False)
        subprocess.run(["pkill", "-f", "chromedriver"], check=False)
        time.sleep(2)
    except:
        pass


def get_book_data(link):
    kill_chrome_processes()
    folder_path = Path(__file__).parent
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-debugging-port=0")
    driver = uc.Chrome(
        headless=True,
        use_subprocess=True,
        driver_executable_path=folder_path / "chromedriver",
        options=options,
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

    finally:
        driver.close()
        driver.quit()
        time.sleep(2)

    return page_source


if __name__ == "__main__":
    page = get_book_data("https://www.moscowbooks.ru/book/968695/")
    print(page)
