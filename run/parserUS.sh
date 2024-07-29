#!/bin/bash

VENV_PATH=venv

# Проверка наличия виртуального окружения
if [ ! -d "$VENV_PATH" ]; then
    echo "No virtual environment. Creating..."
    python3 -m venv $VENV_PATH
else
    echo "Virtual environment exists"
fi

# Активация виртуального окружения
source $VENV_PATH/bin/activate

# Обновление pip
echo "Updating pip..."
pip install --upgrade pip

# Установка зависимостей
if [ -f requirements.txt ]; then
    echo "Installing requirements from requirements.txt..."
    pip install -r requirements.txt
else
    echo "No requirements.txt file. Skipping requirements installation..."
fi

echo "Running script..."
python parserUS.py

# Деактивация виртуального окружения
deactivate