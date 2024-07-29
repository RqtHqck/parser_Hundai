class Logger:
    """
    Пользовательский класс для вывода-сохранения сообщений приложения 
    """
    import os
    log_path = os.path.join('settings', 'app.log')
    time_work_path = os.path.join('settings', 'time.log')
    @classmethod 
    def start_time_save(cls,  mode='a', encoding='utf-8'):
        """Записывает время начала работы"""
        import time
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        with open(cls.time_work_path, mode=mode, encoding=encoding) as f:
            f.write(f'\n\n{current_time} :: Program start')
       
    @classmethod       
    def end_time_save(cls,  mode='a', encoding='utf-8'):
        """Записывает время окончания работы"""
        import time
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        with open(cls.time_work_path, mode=mode, encoding=encoding) as f:
            f.write(f'\n\n{current_time} :: Program end\n\n')
       
       
    @classmethod
    def logger(cls, text, saveonly=False, first=False, infunction=False, mode='a'):
        import time
        """Система логирования"""
        try:
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            with open(cls.log_path, mode, encoding='utf-8') as f:
                if first:
                    f.write(f'\n\n{current_time} - {text}\n')
                else:
                    f.write(f'{current_time} - {text}\n')
                if not saveonly:
                    print(f'"\t"*{int(infunction)}{current_time} - {text}') if infunction else print(f'{current_time} - {text}')
                else:
                    pass
        except FileNotFoundError:
            print("ERROR::FileNotFoundError.")
        except Exception as e:
            print(f"ERROR was ecountered::\n{e}")