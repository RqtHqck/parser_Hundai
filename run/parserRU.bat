@echo off

REM Установка пути к виртуальному окружению
set VENV_PATH=venv

REM Проверка наличия виртуального окружения
if not exist %VENV_PATH% (
    echo No virtual environment. Creating...
    python -m venv %VENV_PATH%
) else (
    echo Virtual environment already exists
)

REM Активация виртуального окружения
CALL %VENV_PATH%\Scripts\activate.bat

REM Обновление pip
echo Updating pip...
python -m pip install --upgrade pip

REM Установка зависимостей
if exist requirements.txt (
    echo Installing requirements from requirements.txt...
    pip install -r requirements.txt
) else (
    echo No requirements.txt file. Skipping requirements installation...
)

REM Запуск парсера
echo Running parser...
python parserRU.py

REM Проверка кода завершения последней команды
if %ERRORLEVEL% neq 0 (
    echo An error occurred during the execution of parser.py. Press any key to view details...
    PAUSE
) else (
    echo Script executed successfully.
)

REM Деактивация виртуального окружения
deactivate