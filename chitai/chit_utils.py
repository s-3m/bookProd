from playwright.sync_api import sync_playwright, ProxySettings
from urllib.parse import urlparse
import time


def parse_with_playwright_proxy(proxy_url, target_url):
    with sync_playwright() as p:
        # Парсим прокси URL
        parsed = urlparse(proxy_url)
        proxy_host = parsed.hostname
        proxy_port = parsed.port
        proxy_user = parsed.username
        proxy_pass = parsed.password

        print(f"🔧 Используем прокси: {proxy_host}:{proxy_port}")
        print(f"👤 Пользователь: {proxy_user}")

        # Запускаем браузер с прокси
        poxy_settings = ProxySettings(
            {
                "server": f"http://{proxy_host}:{proxy_port}",
                "username": proxy_user,
                "password": proxy_pass,
            }
        )
        browser = p.chromium.launch(
            headless=True,  # True для production
            proxy=poxy_settings,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-web-security",
            ],
        )

        # Создаем контекст с пользовательскими настройками
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        )

        # Добавляем stealth скрипты
        context.add_init_script(
            """
            delete navigator.__proto__.webdriver;
            Object.defineProperty(navigator, 'webdriver', { 
                get: () => undefined 
            });
        """
        )

        page = context.new_page()

        try:
            print("🔄 Переходим на сайт...")
            page.goto(
                target_url,
                timeout=30000,
            )
            wait_until = "networkidle"
            # Ждем загрузки контента
            page.wait_for_load_state("networkidle")
            time.sleep(2)

            print("✅ Страница успешно загружена!")

            # Проверяем, что мы не попали на защиту
            content = page.content()
            access_token = None
            if "DDoS-Guard" in content or "cloudflare" in content.lower():
                print("⚠️ Обнаружена защита, пытаемся обойти...")
                # Дополнительные действия для обхода защиты
                page.wait_for_timeout(5000)
                cookies = page.context.cookies()
                for cookie in cookies:
                    if cookie["name"] == "access-token":
                        access_token = cookie["value"]

            return access_token

        except Exception as e:
            print(f"❌ Ошибка: {e}")
            # Делаем скриншот для диагностики
            page.screenshot(path="error_screenshot.png")
            print("📸 Скриншот ошибки сохранен")
            return None

        finally:
            browser.close()
