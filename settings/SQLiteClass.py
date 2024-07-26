import os
import sqlite3
import re
from transliterate import translit
from settings.LoggerClass import Logger

class SQLiteDB(Logger):
    """
    Класс для управления работы с базой данный SQLite
    """
    @classmethod
    def create_empty_database(cls, db_path):
        try:
            # Создаем директорию, если её не существует
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            with sqlite3.connect(db_path) as conn:
                conn.execute("""CREATE TABLE IF NOT EXISTS metadata (
                                table_name TEXT PRIMARY KEY,
                                creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            );""")
            # Создаем файл базы данных, если он не существует
            if not os.path.exists(db_path):
                open(db_path, 'w').close()
                cls.logger(f'База данных была создана по пути "{db_path}".', saveonly=True, first=False, infunction=True)
            else:
                cls.logger(f'База данных уже существует по пути "{db_path}".', saveonly=True, first=False, infunction=True)

        except Exception as e:
            cls.logger(f'General error in create_empty_database function: {e}', saveonly=False, first=False,
                   infunction=True)
            raise


    @classmethod
    def create_table(cls, db_path, table_name):
        """Создаёт таблицу в базе данных по указанному пути."""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # Проверяем, существует ли таблица
                check_table_exists = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
                cursor.execute(check_table_exists)
                result = cursor.fetchone()

                # Если таблица существует, удаляем её
                if result:
                    drop_table = f"DROP TABLE IF EXISTS {table_name};"
                    conn.execute(drop_table)

                # Создаем таблицу
                sql_request = f"""CREATE TABLE {table_name} (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    auto_brand TEXT NOT NULL,
                                    auto_model TEXT NOT NULL,
                                    auto_issue_date TEXT NOT NULL,
                                    part_sub_cat_title TEXT NOT NULL,
                                    model_url TEXT NOT NULL,
                                    part_index TEXT NOT NULL,
                                    part_title TEXT NOT NULL,
                                    part_code TEXT NOT NULL,
                                    part_info TEXT NOT NULL,
                                    part_count INTEGER NOT NULL,
                                    part_url TEXT NOT NULL,
                                    part_image_url TEXT NOT NULL
                                 );"""
                conn.execute(sql_request)
                cursor.execute("INSERT OR IGNORE INTO metadata (table_name) VALUES (?);", (table_name,))
                conn.commit()
                cls.logger(f'Таблица "{table_name}" была создана в базе данных по пути "{db_path}".', saveonly=True,
                       first=False, infunction=True)

        except sqlite3.OperationalError as e:
            cls.logger(f'SQLite operational error in create_table: {e}', saveonly=False, first=False, infunction=True)
            raise
        except sqlite3.DatabaseError as e:
            cls.logger(f'SQLite database error in create_table: {e}', saveonly=False, first=False, infunction=True)
            raise
        except Exception as e:
            cls.logger(f'General error in create_table function: {e}', saveonly=False, first=False, infunction=True)
            raise

    @classmethod
    def fetch_existed_tables_and_continue(cls, db_path):
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                # Получаем последнюю созданную базу данных по таблице metadata в поле creation_time
                cursor.execute("SELECT table_name FROM metadata ORDER BY creation_time DESC LIMIT 1;")
                result = cursor.fetchone()
                return result[0] if result else None
        except sqlite3.Error as e:
            cls.logger(f"SQLite error in function fetch_existed_tabes_and_continue when tried to restore db: {e}",saveonly=False, first=False,
                       infunction=True)
            return None
        except Exception as e:
            cls.logger(f"SQLite error in function fetch_existed_tabes_and_continue when tried to restore db: {e}",saveonly=False, first=False,
                       infunction=True)
            return None

    @classmethod
    def add_data_to_table(cls, db_path, table_name, data):
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                # Проверяем, существует ли таблица
                check_table_exists = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
                cursor.execute(check_table_exists)
                result = cursor.fetchone()

                if not result:
                    raise sqlite3.OperationalError(f'Table "{table_name}" does not exist in database.')

                # Вставляем данные в таблицу
                sql_request = f"""INSERT INTO {table_name} (
                                    auto_brand, auto_model, auto_issue_date, part_sub_cat_title,
                                    model_url , part_index, part_title, part_code,
                                    part_info, part_count, part_url, part_image_url
                                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                conn.executemany(sql_request, data)
                conn.commit()
                cls.logger(f'Данные были добавлены в таблицу "{table_name}".', saveonly=False, first=False,
                       infunction=True)

        except sqlite3.OperationalError as e:
            cls.logger(f'SQLite operational error in add_data_to_table: {e}', saveonly=False, first=False, infunction=True)
            raise
        except sqlite3.DatabaseError as e:
            cls.logger(f'SQLite database error in add_data_to_table: {e}', saveonly=False, first=False, infunction=True)
            raise
        except Exception as e:
            cls.logger(f'Ошибка в работе функции add_data_to_table с таблицей {table_name}: {e}', saveonly=False, first=False, infunction=True)
            raise

    @classmethod
    def transliterate_and_sanitize_table_name(cls, table_name):
        """
        Преобразует русское название таблицы в латиницу и очищает его,
        оставляя только буквы, цифры и подчеркивания.
        """
        try:
            # Транслитерация названия таблицы
            transliterated_name = translit(table_name, 'ru', reversed=True)

            # Регулярное выражение для фильтрации
            pattern = re.compile(r'[A-Za-z0-9_]+')
            matches = pattern.findall(transliterated_name)
            sanitized_name = '_'+''.join(matches)
            
            return sanitized_name
        except Exception as e:
            cls.logger(f'Ошибка транслитерации имени серии {table_name} функции transliterate_and_sanitize_table_name: {e}', saveonly=False, first=False, infunction=True)
            raise
    @classmethod
    def detele_database(cls, db_path):
        try:
            os.remove(db_path)
        except Exception as e:
            cls.logger(f'Ошибка при удалении базы данных по пути {db_path} функции detele_database: {e}', saveonly=False, first=False, infunction=True)
            raise

    @classmethod
    def delete_table(cls, db_path, table_name):
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"""DROP TABLE {table_name};""")
        except Exception as e:
            cls.logger(f'Ошибка при удалении базы данных по пути {db_path} функции detele_database: {e}', saveonly=False, first=False, infunction=True)
            raise


    @classmethod
    def reset_table(cls, db_path, table_name):
        if not os.path.exists(db_path):
            cls.logger(f'База данных по пути {db_path} не существует.')
            return

        try:
            # Подключаемся к базе данных
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()

            # Удаляем все данные из таблицы
            cursor.execute(f'DELETE FROM {table_name}')
            # Сброс id
            cursor.execute(f'UPDATE sqlite_sequence SET seq = 0 WHERE name = "{table_name}"')

            # Сохраняем изменения
            connection.commit()

        except sqlite3.Error as e:
            cls.logger(f'Ошибка при удалении данных из таблицы {table_name} базы данных по пути {db_path}: {e}')
            raise
