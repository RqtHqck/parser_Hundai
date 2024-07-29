# BASE LIBRARY
import random
import threading
import time
import os
import json
import re
from dotenv import load_dotenv
from openpyxl import Workbook, load_workbook
from openpyxl.utils.exceptions import SheetTitleException
# THREADING
from concurrent.futures import ThreadPoolExecutor, as_completed
# MULTIPROCESSUING
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
import multiprocessing
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
# MY IMPORTS
from settings.ParserClass import Parser
from settings.SQLiteClass import SQLiteDB

URL = 'https://www.elcats.ru'   #URL сайта
MODELS_URL = "https://www.elcats.ru/hyundai/"   #URL с модельным рядом
AUTO_BRAND = 'HUNDAI'   #Бренд
AUTO_MODEL = None   #Выбранная модель
MODEL_URL = None    #Ссылка на выбранную модель

parser = Parser()
            
        
def parse_all_models_into_file(url, parser):
    """
    Парсит все модели со страницы моделей HUNDAI
    Возвращает список models и список urls,
    """ 
    try:
        # Находим таблицу
        soup = BeautifulSoup(parser.fetch_data(url).text, 'lxml') 
        tables = soup.find_all('table', class_="parts_table")
        # Получаем модели и юрл к ним
        models = [link.text.strip() for table in tables for link in table.find_all('a') if link.text.strip()]
        urls = [f'https://www.elcats.ru/hyundai/Modification.aspx?Model={model}' for model in models]
        
        # Сохраняем найденные ресурсы в JSON файл
        source = {}
        for model, url in zip(models, urls):
            source[model] = url
        parser.save_data(name="Serias", path='data', src=source)
    except Exception as e:
        parser.logger(f'Ошибка при выполнении функции parse_all_models_into_file: {e}')
        raise
    
    
def parse_region(url):
    """
    Парсит всё в регионе (модификации)
    Возвращает 
    """
    def format_year(year_str):
        """Обрабатывает дату, чтобы убиралось '-'."""
        if '-' in year_str:
            parts = year_str.split('-')
            if len(parts) == 2:
                start_year = parts[0].strip()
                end_year = parts[1].strip()
                if not end_year:  # If there's no end year
                    return start_year  # Just return the start year
                return f"{start_year} - {end_year}"  # Return the range as is
        return year_str  # Return single year as is
    
    # try:
    soup = BeautifulSoup(parser.fetch_data(url).text, 'lxml')
    table = soup.find('div', id="content").find('fieldset').find('table', cellpadding="6").find_all('tr')
    # except Exception as e:
    #     parser.logger(f'Ошибка при выполнении функции parse_region: У серии нет выбора региона', saveonly=False, first=False, infunction=False)
    #     return [], [], []
    
    # Получаем таблицу и регионы
    regions = table[1].find_all('td')
    table = table[3:]

    try:
        # Проверка есть ли регион европа
        regions = [item.find('label').text for item in regions]
        if 'ЕВРОПА' not in regions:
            return [], [], []   
        else:
            js_functions = [item.find('a')['href'] for item in table]
                        
            # ГОД
            years = []
            for year in table:
                if year:
                    year = format_year(year.find_all('td')[-1].text.strip())
                    years.append(year)
                    
            regex_for_url_and_name = re.compile(r"submit\('([^']*)','([^']*)'\)")
            urls_list = []
            models_name = []
            for js in js_functions:
                match = regex_for_url_and_name.search(js)
                if match:
                    arg1 = match.group(1)
                    arg2 = match.group(2)
                    # ССЫЛКА
                    region_url = f"https://www.elcats.ru/hyundai/Options.aspx?Code={arg1}&Title={arg2}"
                    urls_list.append(region_url)
                    models_name.append(arg1)
            return models_name, urls_list, years
    except Exception as e:
        parser.logger(f'Ошибка при выполнении функции parse_region: {e}', saveonly=True, first=False, infunction=False)
        return [], [], []   


def collect_items_dict():
    serias_dict = parser.read_data(name='Serias', path='data')
    
    models_dict = {}

    # Запуск потоков для быстрого прохода по моделям и сбора словаря
    with ThreadPoolExecutor(max_workers=14) as executor:
        futures = {executor.submit(parse_region, seria_url): seria_name for seria_name, seria_url in serias_dict.items()}
        
        for future in as_completed(futures):
            seria_name = futures[future]
            try:
                models_name, urls_list, years = future.result()
                
                # Создаём в словаре ключ по серии
                if seria_name not in models_dict:
                    models_dict[seria_name] = []
                
                # Добавляем данные в ключ по серии
                for mdl_name, url, year in zip(models_name, urls_list, years):
                    models_dict[seria_name].append((mdl_name, url, year))
            except Exception as e:
                parser.logger(f'Error processing {seria_name}: {e}', saveonly=True)
                
    # Удаление пустых ключей
    models_dict = {k: v for k, v in models_dict.items() if v}
    # Сохраняем словарь где ключ - серия, а значение [название модели, год, url, год]
    parser.save_data(name="Models", path='data', src=models_dict)
    parser.logger(f"Было найдено [{len(models_dict)}] серий автомобиля [{AUTO_BRAND}] региона ЕВРОПА")


def process_model(task, lock, db_path):
    # Получает аргументы из потока
    seria_name, model_name, model_url, issue_date = task
    parser.logger(f'\n|ПРОЦЕСС:: {seria_name}||{model_name}||{issue_date}', saveonly=False, first=False,
                           infunction=False)
    
    # Ищет url комплектации
    driver = parser.setup_driver()
    details_url = parse_complectation(model_url, driver)  # Комплектация
    details_dict = parse_details_page(details_url)
    driver.quit()    


    tasks = []
    THREADS = 16
    # Собираем tasks из sub_cat и js переходов на страницу таблицы с деталью
    for sub_cat_title, sub_cat_cont_and_details_js in details_dict.items():
        sub_cut_title_cont_data = sub_cat_cont_and_details_js[0]
        js_data = sub_cat_cont_and_details_js[1]
        for s_c_t_cont, detail_js in zip(sub_cut_title_cont_data, js_data):
            
            tasks.append((seria_name, model_name, model_url, issue_date, f"{sub_cat_title} | {s_c_t_cont}", detail_js, details_url))                    
        
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(process_inner_details, task, lock, db_path) for task in tasks]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                parser.logger(f"Ошибка потока: {e}")


def process_inner_details(task, lock, db_path):
    try:
        driver = parser.setup_driver()
        seria_name, model_name, model_url, issue_date, sub_cat_title, detail_js, details_url = task
        parser.logger(f'\n|РАЗДЕЛ:: {model_name}||{issue_date}||{sub_cat_title}', saveonly=False, first=False,
                            infunction=1)
        
        try:
            conditional_part_url = parser.selenium_crossing(details_url, detail_js, driver)
        except:
            parser.logger(f'Ошибка при переходе на страницу запчасти с таблицей деталей\nurl:{details_url}, js:{detail_js} ')

        # Добавяем существующие данные в DATA
        DATA = [seria_name, AUTO_BRAND, model_name, issue_date, sub_cat_title, model_url]
        if conditional_part_url and 'Parts.aspx' in conditional_part_url:
            # Если страница с таблицей парсим данные
            part_url = conditional_part_url
            part_mage_url = parse_part_picture(URL, conditional_part_url)
            DATA.extend([part_url, part_mage_url])
            parse_table(DATA, lock, db_path)
            
        elif 'Unit.aspx' in conditional_part_url:
            # Если это страница с выбором юнита, выбираем и дальше парсим таблицу
            sub_details_js_list = parse_sub_details(conditional_part_url)

            for sub_detail_js in sub_details_js_list:
                # Переход на страницу поддеталей и парсинг таблицы
                part_url = parser.selenium_crossing(conditional_part_url, sub_detail_js, driver)
                if part_url is None:
                    continue

                part_mage_url = parse_part_picture(URL, part_url)
                DATA.extend([part_url, part_mage_url])
                parse_table(DATA, lock, db_path)
    except Exception as e:
        parser.logger(f"Приозошла ошибка в функции process_inner_details: {e}")
    finally:
        driver.quit()


def parse_complectation(url, driver):
    """
    Нажимает на кнопку далее на старнице комплектаци
    """
    btn_selector = "#btnHyundaiNext"    #ID кнопки для перехода
    try:
        complectations_url = parser.selenium_click_and_get_page(url, btn_selector, driver)
        return complectations_url
    except Exception as e:
        parser.logger(f"Ошибка обработки в функици parse_complectation: {e}")

    
def parse_details_page(complectations_url):
    """
    Парсит переход на страницу с таблицей с запчастями
    Возвращает словарь запчастей, где группа - ключ, а значение все js для перехода на таблицу с деталью этой группы
    """
    try:
        soup = BeautifulSoup(parser.fetch_data(complectations_url).text, 'lxml')
        next_sibling = soup.find('span', id="ctl00_cphMasterPage_trvGroups1").find_next_sibling()
        details_dict={}
        while next_sibling:
            if next_sibling.name == 'table':
                group_name = (next_sibling.find_all('a')[1]).text
                next_sibling = next_sibling.find_next_sibling()
                if next_sibling and next_sibling.name == 'div':
                    # Получение всех href, начинающихся с javascript
                    links = next_sibling.find_all('a', href=re.compile(r'^javascript'))
                    model_sub_part_titles_cont = [link.text.split(']')[1].strip() for link in links]  # Список подразделов раздела
                    model_hrefs = [link.get('href').strip(';') for link in links]  # Список ссылок
                    # ЗАПОЛНЕНИЕ DICT
                    details_dict[group_name] = [model_sub_part_titles_cont, model_hrefs]
            next_sibling = next_sibling.find_next_sibling()
        return details_dict
    except Exception as e:
        parser.logger(f"Произошла ошибка в функции parse_details_page: {e}")


def parse_sub_details(part_url):
    """
    Делает переход по юниту в селениуме
    Возвращает страницу part_url
    """

    """
    Возвращает js список для перехода на страницу таблицы подзапчасти
    """
    try:
        soup = BeautifulSoup(parser.fetch_data(part_url).text,'lxml')
        table = soup.find('table', id='ctl00_cphMasterPage_tblUnit')
        js_fns_a = table.find_all('a')  # Получение всех ссылок с js
        links_js = [js.get('href').strip(';') for js in js_fns_a]
        return links_js
    except Exception as e:
        parser.logger(f'Ошибка при выполнении функции parse_sub_detail: {e}', saveonly=False, first=False, infunction=True)


def parse_part_picture(gen_url, part_url):
    """
    Парсит картинку part_image_url
    """

    try:
        soup = BeautifulSoup(parser.fetch_data(part_url).text, 'lxml')
        part_image_url = gen_url + '/' + str(soup.find('img', id='ctl00_cphMasterPage_imgParts')['src'])[3:]
        return part_image_url
    except Exception as e:
        parser.logger(f"Произошла ошибка в функции parse_part_picture: {e}")


def collect_playload(part_url):
    """
    Функция открывает каждую группу деталей и парсит данные.
    Возвращает словарь с подставленными __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION
    Возрващает список id элементов, по которым нужно сделать запрос, чтобы открыть
    """
    try:
        form_data = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '',
        'ctl00$cphMasterPage$urMain$UserOffice': 'ru',
        '__CALLBACKID': '__Page',
        '__CALLBACKPARAM': '',
        '__EVENTVALIDATION': ''
        }
        # Получаем __VIEWSTATEGENERATOR, __VIEWSTATE, __EVENTVALIDATION
        soup = BeautifulSoup(parser.fetch_data(part_url).text, 'lxml')
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})['value']
        viewstate_generation = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value']
        event_validation = soup.find('input', {'name': '__EVENTVALIDATION'})['value']

        form_data['__VIEWSTATE'] = viewstate
        form_data['__VIEWSTATEGENERATOR'] = viewstate_generation
        form_data['__EVENTVALIDATION'] = event_validation

        divs = soup.find_all(class_='CNode')
        ids = [div['id'] for div in divs]

        return form_data, ids
    except Exception as e:
        parser.logger(f"Произошла ошибка в функции collect_playload: {e}")


def parse_table(DATA, lock, db_path):
    """
    Парсит таблицу деталей
    Возвращает список деталей (5шт)
    """
    part_url = DATA[-2]
    try:
        # Собирает словарь playloads, парсит id товаров
        form_data, ids = collect_playload(part_url)

        # Получаем страницу 
        response, session = parser.fetch_data(url=part_url, return_session=True)

        for i in range(len(ids)):
            # Перебираем детали, открывая по айди таблицы, перебирая id для form_data
            form_data['__CALLBACKPARAM'] = ids[i]
            updated_response = parser.fetch_data(part_url, form_data, session)
            soup = BeautifulSoup(updated_response.text, 'lxml')
            rows = soup.find(class_='OpelParts').find_all('tr')[1:] # Пропускаем заголовок и получаем все вложенные поля таблицы

            one_table_data = []  # Список деталей для текущего ID
            for row in rows:  # один ряд деталей
                details_list = [detail.text.strip() for detail in row]
                sorted_lst = [details_list[i] if details_list[i] != '' else '-' for i in [1, 0, 4, 2]]    # Детали в правильном порядке

                one_table_data.append(sorted_lst)  # Добавляем список данных одной детали в row_lst
            for detail_row in one_table_data:
                row_to_write = []
                row_to_write.extend(DATA[1:])
                row_to_write.extend(detail_row)
                
                
                # ДОБАВЛЕНИЕ В SQLite
                table_name = DATA[0]
                data = [row_to_write[i] for i in [0, 1, 2, 3, 4, 7, 8, 9, 10, 5, 6]]
                SQLiteDB.add_data_to_table(db_path, table_name, data)
                parser.logger(f"[+]ДЕТАЛЬ:: {row_to_write[1]}||{row_to_write[2]}||{row_to_write[3]}|{row_to_write[7]}", saveonly=False, first=False,
                    infunction=2)
                # add_to_new_sheet(str(parser.file_path), cleared_model_name, row_to_write, lock, parser)

    except Exception as e:
        parser.logger(f"Произошла ошибка в функции parse_table: {e}")

        
def parse():
    try:
        parser.logger('|---Программа начала свою работу---|', False, True)
        # ---------------РАЗБОР МОДЕЛЕЙ НА СТРАНИЦЕ---------------
        parse_all_models_into_file(MODELS_URL, parser)
        parser.logger('|---Получение всех моделей...')
        collect_items_dict()
        models_dict = parser.read_data(name='Models', path='data')

        # Устанавливаем хранилище базы данных и создаём бд
        db_path = Parser.storage_path = os.path.join('data', 'HUNDAI_eu', f'HUNDAI_eu.db')
        SQLiteDB.create_empty_database(db_path)

        # Начсинаем отсчитывать время работы
        parser.start_time_save() 
         
        PROCESSES = 1
        manager = Manager()
        file_lock = manager.Lock()

        last_table_name = SQLiteDB.fetch_existed_tables_and_continue(db_path)
        passed = False

        for seria_name, seria_models_list in models_dict.items():
            tasks = []
            # Очистка названия серии для создания таблицы
            seria_name = SQLiteDB.transliterate_and_sanitize_table_name(seria_name)
            
            # ===ПРОВЕРКА СУЩЕСТВУЮЩИХ СУБД ДЛЯ ВОСССТАНОВЛЕНИЯ===
            if last_table_name is None:
                SQLiteDB.create_table(db_path, seria_name)
            elif seria_name == last_table_name:
                SQLiteDB.delete_table(db_path, seria_name)
                SQLiteDB.create_table(db_path, seria_name)
                passed = True
            elif not passed:
                continue
            else:
                SQLiteDB.create_table(db_path, seria_name)
            # =====================================================
            
            # ===Создание задачи===
            for data in seria_models_list:
                mdl_name = SQLiteDB.transliterate_and_sanitize_table_name(data[0])
                mdl_url = data[1]
                mdl_year = data[2]
                tasks.append((seria_name, mdl_name, mdl_url, mdl_year))
            # =====================
            
            # Вынесение в процесс парсинг серии
            with ProcessPoolExecutor(max_workers=PROCESSES) as executor:
                futures = [executor.submit(process_model, task, file_lock, db_path) for task in tasks]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        parser.logger(f"Ошибка потока: {e}")
            # ==================================

            tasks.clear()
        parser.end_time_save()
        parser.logger(f'|------------------------------------------------------|', saveonly=False, first=False, infunction=False)
        parser.logger(f'|---Модели были успешно собраны по пути data/HUNDAI_EU_EU/HUNDAI_EU.db', saveonly=False, first=False, infunction=False)

    except KeyboardInterrupt:
        parser.logger('\nKeyboardInterrupt')
    except Exception as e:
        parser.logger(f'|---Ошибка в работе программы\n')
    finally:
        parser.logger(f'|------------------------------------------------------|', saveonly=False, first=False, infunction=False)
        parser.logger('|---Завершение работы программы...', saveonly=False, first=False, infunction=False)


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    parse()

