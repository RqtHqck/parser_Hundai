# BASE LIBRARY
import random
import json
import threading
import time
# REQUEST&BS4
import requests
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



class Parser:
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
        self.file_path = None
        self.lock_treads = threading.Lock()

    def logger(self, text, saveonly=False, first=False, infunction=False, mode='a'):
        """Система логирования"""
        try:
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            with open('settings/app.log', mode, encoding='utf-8') as f:
                if first:
                    f.write(f'\n\n{current_time} - {text}\n')
                else:
                    f.write(f'{current_time} - {text}\n')
                if not saveonly:
                    print(f'\t{current_time} - {text}') if infunction else print(f'{current_time} - {text}')
                else:
                    pass
        except FileNotFoundError:
            print("ERROR::FileNotFoundError.")
        except Exception as e:
            print(f"ERROR was ecountered::\n{e}")

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
            with open(f'{path}/{name}', 'r', encoding='utf-8') as file:
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
        # Автоматическая установка последней версии ChromeDriver

        # Настройка параметров Chrome
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Запуск в headless-режиме
        chrome_options.add_argument('--log-level=3')
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        chrome_options.add_argument(f'user-agent={user_agent}')
        # Создание экземпляра Chrome WebDriver
        service = Service(ChromeDriverManager().install())  # Автоматическое обнаружение и установка ChromeDriver
        driver = webdriver.Chrome(service=service, options=chrome_options)
        # self.logger('Веб-драйвер для selenium был создан', saveonly=False, first=False, infunction=True)
        return driver

    def selenium_click_and_get_page(self, url, button_selector, driver, retries=3):
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

    def selenium_crossing(self, url, js_request: str, driver):
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