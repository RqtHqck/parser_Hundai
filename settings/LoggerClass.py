class Logger:

    def start_time_save(self,  mode='a', encoding='utf-8'):
        """Записывает время начала работы"""
        import time
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        with open(self.log_file, mode=mode, encoding=encoding) as f:
            f.write(f'\n\n{current_time} :: Program started job.\n')
       
            
    def end_time_save(self,  mode='a', encoding='utf-8'):
        """Записывает время окончания работы"""
        import time
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        with open(self.log_file, mode=mode, encoding=encoding) as f:
            f.write(f'\n\n{current_time} :: Program ended job.\n')
       

    def logger(self, text, saveonly=False, first=False, infunction=False, mode='a'):
        import time
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