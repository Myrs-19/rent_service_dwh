from airflow import DAG

from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator

import datetime

# ссылка на все фильтры по ростову - аренда квартир
# https://rostov.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat&region=4959&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&type=4

# my.rs@mail.ru 89996940159
# пароль от аккаунта циан: i8e7F!Fs_68Nf5S

EMAIL = "my.rs@mail.ru"
PASSWORD = "i8e7F!Fs_68Nf5S"

import os

DAG_ID = os.path.basename(__file__)[:-3]

def load(**kwargs):
    # циан -  https://rostov.cian.ru/

    import re

    import hashlib

    import datetime
    from time import sleep

    from selenium import webdriver
    from selenium.webdriver.common.by import By

    import pandas as pd

    # from selenium.common.exceptions import InvalidSelectorException

    from rent_serv.EndPagesCianException import EndPagesCianException
    # from NotEqualExceptCountOffersCian import NotEqualExceptCountOffersCian

    # устанавливаем настройки для создания драйвера
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--disable-gpu')
    # options.addArguments("--no-sandbox")
    # options.addArguments("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    driver.maximize_window()

    driver.get("https://rostov.cian.ru")

    sleep(3)

    log_in_button = driver.find_element(by=By.LINK_TEXT, value="Войти")

    log_in_button.click()

    sleep(3)

    ## заходим под свой аккаунт
    # другой способ зайти в аккаунт
    other_enter_method = driver.find_element(by=By.XPATH, value="//button[@data-name='SwitchToEmailAuthBtn']")

    other_enter_method.click()

    # находим элемент для ввода почты
    enter_email = driver.find_element(by=By.XPATH, value="//input[@name='username']")
    enter_email.send_keys(EMAIL)

    sleep(1)

    # кнопка "Продолжить"
    next_button = driver.find_element(by=By.XPATH, value="//button[@data-name='ContinueAuthBtn']")
    next_button.click()

    sleep(2)

    # Вводим пароль 
    pass_print = driver.find_element(by=By.XPATH, value="//input[@name='password']")
    pass_print.send_keys(PASSWORD)

    sleep(1)

    # кнопка "Продолжить"
    next_button = driver.find_element(by=By.XPATH, value="//button[@data-name='ContinueAuthBtn']")
    next_button.click()

    sleep(1)

    ## скачивание excel файла со всеми объявлениями по фильтру
    # захожу на страничку со всеми фильтрами аренды квартиры
    driver.get("https://rostov.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat&region=4959&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1&type=4")

    sleep(3)

    # кнопка "Сохранить файл Excel"
    load_button = driver.find_element(by=By.XPATH, value="//button[@data-name='PaginationButtonsContainer']")
    load_button.click()
    sleep(1)
    load_button_1 = driver.find_element(by=By.XPATH, value="//button[@data-name='download_excel_trigger']")
    load_button_1.click()

    driver.close()

args = {
    "retries": 3
}

with DAG(
    dag_id = DAG_ID,
    start_date = datetime.datetime(year=2025, month=3, day=19),
    schedule = "0 5,23 * * *", # каждый день в 5 утра и 23 вечера по utc
    catchup = False,
    max_active_runs=1,
    default_args = args

) as dag:
    start = EmptyOperator(
        task_id = "start"
    )

    end = EmptyOperator(
        task_id = "end"
    )

    parse_and_load = PythonOperator(
        task_id = "parse_and_load",
        python_callable=load
    )

    start >> parse_and_load >> end