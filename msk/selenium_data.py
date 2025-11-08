import time
import subprocess
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
import undetected_chromedriver as uc


def get_chrome_version():
    """Получаем версию установленного Chrome"""
    try:
        result = subprocess.run(
            ["google-chrome", "--version"], capture_output=True, text=True
        )
        version_str = result.stdout.strip()
        # Извлекаем номер версии (например, "139.0.7258.138")
        version = version_str.split()[-1].split(".")[0]  # Возвращает "139"
        print(version)
        return int(version)
    except:
        try:
            result = subprocess.run(
                ["chromium-browser", "--version"], capture_output=True, text=True
            )
            version_str = result.stdout.strip()
            version = version_str.split()[-1].split(".")[0]
            return int(version)
        except:
            return None


def kill_chrome_processes():
    try:
        subprocess.run(["pkill", "-f", "chrome"], check=False)
        subprocess.run(["pkill", "-f", "chromedriver"], check=False)
        time.sleep(2)
    except:
        pass


def get_book_data(link):
    chrome_version = 139
    kill_chrome_processes()
    for attempt in range(6):
        try:
            folder_path = Path(__file__).parent
            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--remote-debugging-port=0")
            driver = uc.Chrome(
                headless=True,
                use_subprocess=True,
                options=options,
                version=chrome_version,
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
            chrome_version = get_chrome_version()
    return None


if __name__ == "__main__":
    page = get_book_data("https://www.moscowbooks.ru/book/968695/")
    print(page)
