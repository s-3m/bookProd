"""
Потокобезопасный загрузчик страниц через пул прокси.

Логика:
  * у каждого прокси своя сессия (cookies живут, пока прокси не заморожен);
  * редиректы обрабатываются вручную: цепочка всегда видна в логе, цикл
    (в т.ч. на /userconfirm/ = капча) считается блоком;
  * при блоке (цикл редиректов / 403 / 429) прокси замораживается на 30 мин;
  * URL, поймавший блок на двух разных прокси, пропускается (возвращается None);
  * вызывающий код блокируется, пока не освободится прокси, поэтому
    ThreadPoolExecutor должен иметь max_workers >= числа прокси;
  * если сайт отдаёт форму проверки возраста, она проходится автоматически
    (POST на /submit/userage/ с антифорджери-токеном), cookie остаётся в сессии прокси.

Две гипотезы отпечатка, которые можно включать/выключать:
  * USE_CURL_CFFI: TLS-отпечаток настоящего Chrome вместо requests (pip install curl_cffi);
  * ROTATE_UA: у каждой сессии свой User-Agent (в режиме requests; в режиме curl_cffi
    User-Agent задаёт сама имитация браузера, а версии Chrome чередуются по прокси).

Зависимости: pip install curl_cffi requests loguru beautifulsoup4

Использование:
    from proxy_fetcher import fetch_html
    html = fetch_html(url, headers)      # str | "404" | None
    # cookies НЕ передавайте: сессия сама ведёт токен и cookie возраста
"""
import random
import re
import threading
import time
from collections import deque
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from loguru import logger
from requests.exceptions import ConnectionError as ReqConnError
from requests.exceptions import Timeout

try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests import exceptions as cffi_exc
    _HAVE_CFFI = True
except Exception:  # curl_cffi не установлен
    cffi_requests = cffi_exc = None
    _HAVE_CFFI = False

# ---------- переключатели гипотез ----------
USE_CURL_CFFI = True             # TLS-отпечаток Chrome (нужен curl_cffi)
ROTATE_UA = True                 # свой User-Agent у каждой сессии (режим requests)

# ---------- проверка возраста (moscowbooks.ru) ----------
AGE_MARK = "age_verification_form"                         # id формы в HTML
AGE_URL = "https://www.moscowbooks.ru/submit/userage/"     # куда уходит POST
AGE_ORIGIN = "https://www.moscowbooks.ru"
AGE_DATE = {"Day": "1", "Month": "1", "Year": "1990"}      # имена полей как в Payload

# ---------- параметры (подстраиваются по логам) ----------
PAUSE = (1.0, 2.0)               # пауза перед каждым запросом одного прокси, с
BATCH_MIN, BATCH_MAX = 25, 30    # запросов на прокси за один заход (если включён плановый отдых)
FIRST_BATCH = (8, 30)            # первая пачка короче: прокси не уходят на отдых одновременно
REST_PLANNED = 0                 # плановый отдых после пачки, с (0 = выключен)
FREEZE = 30 * 60                 # заморозка при блокировке
REST_NET = 5 * 60                # отдых при серии сетевых ошибок
MAX_REDIRECT_FAILS = 2           # на скольких прокси URL поймал блок, прежде чем его пропустят
MAX_ERR_FAILS = 5                # повторов URL при сетевых/5xx ошибках
MAX_WAIT = 2 * 60 * 60           # сколько один URL готов ждать свободный прокси, с
MAX_HOPS = 5                     # максимум редиректов в одной цепочке
WAIT_LOG = 60                    # если все прокси заняты/на отдыхе дольше, пишем в лог (не чаще раза в WAIT_LOG с)

REDIRECT_CODES = (301, 302, 303, 307, 308)
BLOCK_CODES = (403, 429)

_USE_CFFI = USE_CURL_CFFI and _HAVE_CFFI
if USE_CURL_CFFI and not _HAVE_CFFI:
    logger.warning("curl_cffi не установлен (pip install curl_cffi), работаю через requests")

_UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]
_CFFI_PREFERRED = ["chrome131", "chrome124", "chrome123", "chrome120", "chrome119", "chrome116", "chrome110"]


def _cffi_targets() -> list[str]:
    try:
        from curl_cffi.requests import BrowserType
        names = {b.value for b in BrowserType}
    except Exception:
        return ["chrome"]
    return [t for t in _CFFI_PREFERRED if t in names] or ["chrome"]


_CFFI_TARGETS = _cffi_targets() if _USE_CFFI else []

_NET_ERRORS: tuple = (ReqConnError, Timeout)
if _HAVE_CFFI:
    _NET_ERRORS += tuple(
        getattr(cffi_exc, n)
        for n in ("ConnectionError", "Timeout", "ProxyError", "SSLError", "ConnectTimeout", "ReadTimeout")
        if hasattr(cffi_exc, n)
    )


def _normalize(url: str) -> str:
    """Без двойных слешей и со слешем в конце: иначе сайт делает лишний 301 (2 запроса вместо 1)."""
    scheme, sep, rest = url.partition("://")
    if not sep:
        return url
    rest = re.sub(r"/{2,}", "/", rest)
    if "?" not in rest and "#" not in rest and not rest.endswith("/"):
        rest += "/"
    return f"{scheme}://{rest}"


def _mask(proxy: str) -> str:
    return proxy.split("@")[-1]  # без логина и пароля


class _Slot:
    __slots__ = ("proxy", "session", "used", "limit", "avail_at", "busy", "net_errors", "ua", "target")

    def __init__(self, proxy: str):
        self.proxy = proxy
        self.session = None
        self.used = 0
        self.limit = random.randint(*FIRST_BATCH)
        self.avail_at = 0.0
        self.busy = False
        self.net_errors = 0
        self.ua = None
        self.target = None

    def new_session(self):
        proxies = {"http": self.proxy, "https": self.proxy}
        if _USE_CFFI:
            self.target = random.choice(_CFFI_TARGETS)
            return cffi_requests.Session(impersonate=self.target, proxies=proxies)
        self.ua = random.choice(_UA_LIST)
        s = requests.Session()
        s.proxies.update(proxies)
        return s

    def close(self):
        if self.session is not None:
            try:
                self.session.close()
            except Exception:
                pass
            self.session = None


class ProxyFetcher:
    def __init__(self, proxies: list[str]):
        proxies = [p.strip() for p in proxies if p.strip()]
        self._slots = deque(_Slot(p) for p in proxies)
        self._cond = threading.Condition()
        self._last_wait_log = 0.0
        mode = f"curl_cffi {_CFFI_TARGETS}" if _USE_CFFI else f"requests, ROTATE_UA={ROTATE_UA}"
        logger.info(f"ProxyFetcher: {len(proxies)} прокси, режим: {mode}")

    def _resting(self) -> str:
        """Сколько прокси сейчас не доступны (отдых или блок): '12/20'."""
        with self._cond:
            now = time.time()
            n = sum(1 for sl in self._slots if sl.avail_at > now)
            return f"{n}/{len(self._slots)}"

    # ---------- выдача прокси ----------
    def _acquire(self, deadline: float) -> _Slot | None:
        started = time.time()
        with self._cond:
            while True:
                now = time.time()
                for _ in range(len(self._slots)):
                    slot = self._slots[0]
                    self._slots.rotate(-1)
                    if not slot.busy and slot.avail_at <= now:
                        slot.busy = True
                        return slot
                if now >= deadline:
                    return None
                if now - started >= WAIT_LOG and now - self._last_wait_log >= WAIT_LOG:
                    self._last_wait_log = now
                    busy = sum(1 for sl in self._slots if sl.busy)
                    resting = sum(1 for sl in self._slots if not sl.busy and sl.avail_at > now)
                    logger.warning(
                        f"\nПоток ждёт свободный прокси {now - started:.0f} c | "
                        f"в работе: {busy}, на отдыхе: {resting} из {len(self._slots)}"
                    )
                self._cond.wait(timeout=min(10, deadline - now))

    def _release(self, slot: _Slot, rest: float = 0.0):
        with self._cond:
            slot.busy = False
            if rest:
                slot.avail_at = time.time() + rest
            self._cond.notify()

    # ---------- заголовки ----------
    @staticmethod
    def _headers_for(slot: _Slot, headers: dict | None) -> dict:
        h = dict(headers or {})
        if _USE_CFFI:
            # User-Agent и порядок заголовков задаёт имитация браузера; свой UA = расхождение с TLS
            h = {k: v for k, v in h.items() if k.lower() != "user-agent"}
        elif ROTATE_UA and slot.ua:
            h = {k: v for k, v in h.items() if k.lower() != "user-agent"}
            h["user-agent"] = slot.ua
        return h

    # ---------- проверка возраста ----------
    def _pass_age_gate(self, slot, html, page_url, hdrs, cookies, timeout) -> bool:
        """Отправляет форму проверки возраста. Cookie сохраняется в сессии прокси."""
        form = BeautifulSoup(html, "html.parser").find("form", id=AGE_MARK)
        if form is None:
            return False

        # скрытые поля формы: __RequestVerificationToken, url
        data = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if name and inp.get("type") not in ("button", "submit"):
                data[name] = inp.get("value", "")

        # запасной вариант, если токена нет внутри формы
        if "__RequestVerificationToken" not in data:
            m = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', html)
            if m:
                data["__RequestVerificationToken"] = m.group(1)

        data.update(AGE_DATE)
        data.setdefault("url", "")

        h = dict(hdrs)
        h["referer"] = page_url
        h["origin"] = AGE_ORIGIN
        h["x-requested-with"] = "XMLHttpRequest"
        h["accept"] = "*/*"

        r = slot.session.post(
            AGE_URL, data=data, headers=h, cookies=cookies,
            timeout=timeout, allow_redirects=False,
        )
        logger.debug(
            f"userage -> {r.status_code} {r.text[:100]} | {_mask(slot.proxy)} | keys: {list(data)}"
        )
        return r.status_code == 200

    # ---------- один запрос через выбранный прокси ----------
    def _request(self, slot: _Slot, url, headers, cookies):
        """Возвращает (outcome, payload, rest). outcome: ok | 404 | block | net | other."""
        if slot.session is None:
            slot.session = slot.new_session()
            slot.used = 0
        time.sleep(random.uniform(*PAUSE))
        hdrs = self._headers_for(slot, headers)
        timeout = 30 if _USE_CFFI else (10, 30)
        cur, hops = url, []
        age_done = False
        try:
            while True:
                r = slot.session.get(cur, headers=hdrs, cookies=cookies, timeout=timeout, allow_redirects=False)
                if r.status_code not in REDIRECT_CODES:
                    # страница с формой возраста: отправляем форму и повторяем GET уже с cookie
                    if r.status_code == 200 and not age_done and AGE_MARK in r.text:
                        age_done = True
                        self._pass_age_gate(slot, r.text, cur, hdrs, cookies, timeout)
                        continue
                    break
                loc = r.headers.get("Location")
                if not loc:
                    break
                nxt = urljoin(cur, loc)
                hops.append(f"{r.status_code} {cur}")
                if nxt == cur or len(hops) > MAX_HOPS:
                    slot.used += 1
                    logger.warning(
                        f"\nБЛОК цикл редиректов | {_mask(slot.proxy)} | запросов до блока: {slot.used} "
                        f"| прокси на отдыхе: {self._resting()} | цепочка: {' -> '.join(hops)} -> {nxt}"
                    )
                    return "block", None, FREEZE
                cur = nxt
        except _NET_ERRORS as e:
            slot.net_errors += 1
            logger.warning(f"{type(e).__name__} | {_mask(slot.proxy)} | {url}")
            rest = REST_NET if slot.net_errors >= 3 else 0.0
            return "net", None, rest
        except Exception as e:
            logger.warning(f"{type(e).__name__} | {_mask(slot.proxy)} | {url}")
            return "other", None, 0.0

        slot.net_errors = 0
        slot.used += 1
        if r.status_code == 200:
            if AGE_MARK in r.text:
                logger.warning(f"Возрастная форма не пройдена | {_mask(slot.proxy)} | {url}")
                return "other", None, 0.0
            return "ok", r.text, 0.0
        if r.status_code == 404:
            return "404", "404", 0.0
        if r.status_code in BLOCK_CODES:
            logger.warning(
                f"\nБЛОК HTTP {r.status_code} | {_mask(slot.proxy)} | запросов до блока: {slot.used} "
                f"| прокси на отдыхе: {self._resting()} | {url}"
            )
            return "block", None, FREEZE
        return "other", None, 0.0

    # ---------- публичный метод ----------
    def fetch(self, url, headers, cookies=None, max_wait: float = MAX_WAIT):
        """Возвращает html, "404" или None (не удалось / страница-ловушка)."""
        url = _normalize(url)
        deadline = time.time() + max_wait
        block_fails = err_fails = 0

        while time.time() < deadline:
            slot = self._acquire(deadline)
            if slot is None:
                break
            rest = 0.0
            try:
                outcome, payload, rest = self._request(slot, url, headers, cookies)
                if outcome in ("ok", "404"):
                    if REST_PLANNED and slot.used >= slot.limit:
                        rest = REST_PLANNED
                        slot.limit = random.randint(BATCH_MIN, BATCH_MAX)
                    return payload
                if outcome == "block":
                    block_fails += 1
                else:
                    err_fails += 1
            finally:
                if rest:                       # любой отдых/заморозка = новая сессия потом
                    slot.close()
                self._release(slot, rest)

            if block_fails >= MAX_REDIRECT_FAILS:
                logger.error(f"блок на {block_fails} прокси, пропуск: {url}")
                return None
            if err_fails >= MAX_ERR_FAILS:
                logger.error(f"слишком много ошибок, пропуск: {url}")
                return None

        logger.error(f"\nВсе прокси недоступны {max_wait:.0f} c, пропуск: {url}")
        return None


# ---------- синглтон на процесс ----------
_fetcher: ProxyFetcher | None = None
_init_lock = threading.Lock()


def _get_fetcher() -> ProxyFetcher:
    global _fetcher
    if _fetcher is None:
        with _init_lock:
            if _fetcher is None:
                with open(Path(__file__).parent / "proxy.txt") as f:
                    _fetcher = ProxyFetcher(f.readlines())
    return _fetcher


def fetch_html(url, headers, cookies=None):
    return _get_fetcher().fetch(url, headers, cookies)
