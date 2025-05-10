from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.bash import BashOperator

import datetime

import datetime
from time import sleep

from selenium import webdriver
from selenium.webdriver.common.by import By

# from selenium.common.exceptions import InvalidSelectorException

from rent_serv.EndPagesCianException import EndPagesCianException
# from NotEqualExceptCountOffersCian import NotEqualExceptCountOffersCian

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

    # устанавливаем настройки для создания драйвера
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
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

    sleep(3)

    driver.close()

def load_postgresql(**kwargs):
    from sqlalchemy import create_engine
    import pandas as pd
    import datetime

    print(kwargs)

    excel_file = pd.read_excel(f"/run/media/mseleznev/data/rent_serv_data/cian_rostov_{kwargs['ts']}.xlsx")

    engine = create_engine('postgresql+psycopg2://srv.etl.dwh:dwh-Jbn$123#!17@localhost:5432/dwh')
    
    excel_file.columns = [
        'id_offer',
        'amount_rooms',
        'offer_type',
        'address',
        'square',
        'house_address',
        'parking_space',
        'price',
        'phones',
        'description',
        'repair',
        'square_rooms',
        'balcony',
        'windows_oriention',
        'bathroom',
        'is_possible_with_kids_animals',
        'additional_description',
        'residential_complex_title',
        'ceiling_height',
        'lift',
        'garbage_chute',
        'link_to_offer'
        ]
    
    excel_file['dataflow_id'] = kwargs['run_id']
    excel_file['dataflow_dttm'] = datetime.datetime.now()

    # Запись в PostgreSQL
    excel_file.to_sql(
        schema='ods',
        name='c_rostov',      # Название таблицы
        con=engine,                # Подключение
        if_exists='append',        # 'append', 'replace' или 'fail'
        index=False,               # Не записывать индекс
    )

args = {
    #"retries": 3
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

    '''
    # скачивать можно не больше 3 раз за день
    parse_and_load = PythonOperator(
        task_id = "parse_and_load",
        python_callable = load
    )
    '''

    move_file_to_ods = BashOperator(
        task_id = "move_file_to_ods",
        #bash_command = "mv /home/mseleznev/Загрузки/offers.xlsx /run/media/mseleznev/data/rent_serv_data/cian_rostov_{{ data_interval_start.strftime('%Y%m%d%H%M%S%f') }}.xlsx",
        bash_command = "cp /home/mseleznev/Загрузки/offers.xlsx /run/media/mseleznev/data/rent_serv_data/cian_rostov_{{ ts }}.xlsx",
    )

    load_to_postgresql = PythonVirtualenvOperator(
        task_id = "load_to_postgresql",
        python_callable = load_postgresql,
        requirements = [
            "sqlalchemy==2.0", 
            "pandas==2.2.3",
            "openpyxl==3.1.5",
            "psycopg2-binary==2.9.10"
        ],
        system_site_packages=False
    )

    # start >> parse_and_load >> move_file_to_ods >> end
    # start >> move_file_to_ods >> end
    # start >> parse_and_load >> move_file_to_ods >> load_to_postgresql >> end
    start >> move_file_to_ods >> load_to_postgresql >> end