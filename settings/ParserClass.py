# BASE LIBRARY
import random
import os
import json
import threading
import time
# REQUEST&BS4
import requests
import retrying
from retry import retry
from bs4 import BeautifulSoup
import fake_useragent
# SELENIUM
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from fake_useragent import UserAgent
from settings.LoggerClass import Logger

class Parser(Logger):
    def __init__(self):
        self.USER = fake_useragent.UserAgent().random
        self.COOCKIE = {
            '_yasc': 'zW1NpPAz16xJ/Xv05/61mZRN/ewMyhv7Jgx9Yzq4m+RsvZZ+aAzdqP0rCliZe2Legw==',
            'i': 'F+Wvu0RUYG1zpp/ta3Ys+K/wqM1VThUxbqWmvX4FKmOK0jt4e+bGNOwP+kSrCUbgqUaSFUyhWmMSPszDsTEeDev9fCw=',
            'yandexuid': '423890391720968555',
            'yashr': '434441231720968555',
            'yuidss': '423890391720968555',
            'ymex': '1752504571.yrts.1720968571#1752504555.yrtsi.1720968555',
            'yabs-sid': '2445225591721030978'
        }
        self.HEADERS = {
            "User-Agent": self.USER,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "TE": "Trailers",
            # Это поле добавлено для имитации более полного набора заголовков, иногда помогает серверу принять запрос
        }
        self.storage_path = None

    @retry(tries=10, delay=3, backoff=2, exceptions=(requests.exceptions.RequestException,))
    def fetch_data(self, url, data=None, session=None, headers=None, coockies=None, return_session=False):
        """Выполняет запрос и возвращает контент страницы"""
        headers = headers or self.HEADERS
        coockies = coockies or self.COOCKIE
        try:
            self.logger(f'Выполнение запроса по url {url}', saveonly=True, first=False, infunction=True)
            if not session:
                session = requests.Session()
            if data:
                response = session.post(url, headers=headers, data=data, cookies=coockies, timeout=60)
            else:
                response = session.get(url, headers=headers, cookies=coockies, timeout=60)

            response.raise_for_status()
            time.sleep(random.uniform(1, 3))
            if return_session:
                return response, session
            return response

        except requests.RequestException as e:
            self.logger(f'Ошибка при выполнении запроса на url:{url}\nfetch_data(url):\n{e}', saveonly=False,
                        first=False, infunction=True)
            raise


    def save_data(self, name: str, path: str, src):
        """Функция сохраняет .json в папку data"""
        try:
            with open(f"{path}/{name}.json", 'w', encoding='utf-8') as file:
                file.write(json.dumps(src, indent=4, ensure_ascii=False))
                self.logger('Файл успешно сохранён.', saveonly=True, first=False, infunction=True)
        except OSError as e:
            self.logger(f"Ошибка в функции save_page при сохранении файла {name}: {e}", saveonly=False, first=False,
                        infunction=True)
            raise


    def read_data(self, name, path, extension='json'):
        """Функция читает файл и возвращает .json-файл в виде словаря"""
        try:
            with open(f'{path}/{name}.json', 'r', encoding='utf-8') as file:
                if extension == 'json':
                    src = json.load(file)
                else:
                    src = file.read()
                return src
        except FileNotFoundError:
            self.logger(f"Файл {name} не найден в директории {path}.", saveonly=False, first=False, infunction=True)
            raise
        except Exception as e:
            self.logger(f"Ошибка в фукнции read_page при чтении файла {name}: {e}", saveonly=False, first=False,
                        infunction=True)
            raise


    def setup_driver(self):
        """
        Настраивает Chrome WebDriver для работы в headless-режиме и возвращает экземпляр драйвера.
        """
        # Настройка параметров Chrome
        options = Options()
        options.add_argument('--headless')   # Запуск в headless-режиме (без графического интерфейса).
        options.add_argument('--disable-gpu')  # Отключение использования GPU (требуется для работы в headless-режиме).
        options.add_argument('--no-sandbox')  # Отключение песочницы (для среды CI/CD).
        options.add_argument('--disable-dev-shm-usage')  # Отключение использования /dev/shm (для среды CI/CD).
        options.add_argument('--allow-running-insecure-content')  # Разрешение выполнения небезопасного контента.
        options.add_argument('--ignore-certificate-errors')  # Игнорирование ошибок сертификатов.
        options.add_argument('--log-level=3')  # Уровень логирования (3 = предупреждения и выше).
        options.add_argument('--disable-software-rasterizer')  # Отключение программного растеризатора.
        options.add_argument('--disable-extensions')  # Отключение всех расширений.
        options.add_argument('--disable-infobars')  # Отключение информационных панелей.
        options.add_argument('--disable-browser-side-navigation')  # Отключение боковой навигации в браузере.
        options.add_argument('--disable-renderer-backgrounding')  # Отключение фона рендеринга.
        options.add_argument('--disable-background-timer-throttling')  # Отключение ограничения таймеров в фоновом режиме.
        options.add_argument('--disable-backgrounding-occluded-windows')  # Отключение фоновой обработки скрытых окон.
        options.add_argument('--disable-client-side-phishing-detection')  # Отключение защиты от фишинга на стороне клиента.
        options.add_argument('--disable-sync')  # Отключение синхронизации браузера.
        options.add_argument('--disable-web-resources')  # Отключение веб-ресурсов.
        options.add_argument('--disable-translate')  # Отключение автоматического перевода страниц.
        options.add_argument('--disable-default-apps')  # Отключение стандартных приложений Chrome.
        options.add_argument('--disable-hang-monitor')  # Отключение монитора зависаний.
        options.add_argument('--disable-prompt-on-repost')  # Отключение запросов при повторной отправке данных.
        options.add_argument('--disable-popup-blocking')  # Отключение блокировки всплывающих окон.
        options.add_argument('--disable-features=VizDisplayCompositor')  # Отключение функции VizDisplayCompositor.
        options.add_argument('--disable-features=site-per-process')  # Отключение функции site-per-process.
        options.add_argument('--blink-settings=imagesEnabled=false')  # Отключение загрузки изображений.
        options.add_argument('--disable-software-rasterizer')

        USER_AGENT = UserAgent()
        options.add_argument(f'user-agent={USER_AGENT.random}')


        # Создание экземпляра Chrome WebDriver
        service = Service(ChromeDriverManager().install()) # Автоматическое обнаружение и установка ChromeDriver
        driver = webdriver.Chrome(service=service, options=options)

        return driver

    @retrying.retry(stop_max_attempt_number=6, wait_fixed=5000)
    def selenium_click_and_get_page(self, url: str, button_selector: str, driver, retries=3):
        driver.get(url)
        attempt = 0
        while attempt < retries:
            try:
                # Ожидание, пока элемент не станет кликабельным
                element = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, button_selector))
                )
                element.click()

                time.sleep(random.uniform(1, 4))

                WebDriverWait(driver, 10).until(
                    EC.staleness_of(element)
                )
                # Предполагаем, что после клика мы попадаем на новый URL
                new_url = driver.current_url
                return new_url
            except StaleElementReferenceException as e:
                # Обработка попыток, если элемент не найден
                attempt += 1
                time.sleep(random.uniform(1, 4))
                self.logger(f'Элемент не найден {url}, retrying {attempt}/{retries}: {e}')
            except Exception as e:
                self.logger(f'Ошибка в функции selenium_click_and_get_page на странице {url}: {e}')
                return None
        return None


    @retrying.retry(stop_max_attempt_number=6, wait_fixed=5000)
    def selenium_crossing(self, url: str, js_request: str, driver):
        """
        Использование selenium для перехода по js-наполняемой ссылке.
        """
        try:
            driver.get(url)

            # Явное ожидание полной загрузки страницы
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script('return document.readyState') == 'complete')
            self.logger(f'Выполняем JavaScript запрос: {js_request}', saveonly=True, first=False, infunction=True)

            driver.execute_script(js_request)
            time.sleep(random.uniform(1, 4))

            # Явное ожидание изменения URL или другой условие, если требуется
            WebDriverWait(driver, 10).until(
                lambda d: d.current_url != url)  # Ожидание, пока URL изменится

            final_url = driver.current_url
            return final_url
        except TimeoutException as e:
            self.logger(
                f"Истекло время ожидания в функции selenium_crossing с url: {url}, и js скриптом: {js_request}: {str(e)}",
                saveonly=False, first=False, infunction=True)
            time.sleep(random.uniform(1, 4))
            return None
        except Exception as e:
            self.logger(f"Произошла ошибка в функции selenium_crossing с  url:{url}, и js:{js_request}: {str(e)}",
                        saveonly=False, first=False, infunction=True)
            return None
        