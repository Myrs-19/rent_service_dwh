# циан -  https://rostov.cian.ru/

import re

import hashlib

import datetime
from time import sleep

from selenium import webdriver
from selenium.webdriver.common.by import By

import pandas as pd

# from selenium.common.exceptions import InvalidSelectorException

from EndPagesCianException import EndPagesCianException
# from NotEqualExceptCountOffersCian import NotEqualExceptCountOffersCian

# устанавливаем настройки для создания драйвера
options = webdriver.ChromeOptions()
# options.add_argument("--headless=new")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
options.add_argument("--window-size=1920,1080")
options.add_argument('--disable-gpu')

driver = webdriver.Chrome(options=options)

driver.maximize_window()

# формируем словарь объявлений по прототипу http://localhost:3000/en/layers_description/ods/ods-first-table
count_page = 1
offers_by_prototype = {
    "dataflow_dttm" : [],
    "dataflow_id" : [],
    "address" : [],
    "title" : [],
    "price" : [],
    "commission": [],
    "link_to_offer" : [],
    "source_sys" : []
}
while True:
    # ссылка на первую страницу https://rostov.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat&region=4959&room1=1&room2=1&room3=1&room4=1&room5=1&type=4&p=3
    # переходим по страничкам
    try:
        url = f"https://rostov.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat&p={count_page}&region=4959&room1=1&room2=1&room3=1&room4=1&room5=1&type=4"
        driver.get(url)
        sleep(10)
        
        page_number_with_p = re.findall("p=[0-9]+", driver.current_url)[0]

        # проверка что редиректа не было 
        actually_page_number = re.findall("[0-9]+", page_number_with_p)[0]
        if int(actually_page_number) != int(count_page):
            raise EndPagesCianException(f"номер страницы по циклу не совпадает с номером по driver.current_url = {actually_page_number}, ожидалось = {count_page}")
            
        offers = list(driver.find_elements(By.XPATH, f"(//div[@data-testid='offer-card'])"))

        # получаем ссылки на объявления - link_to_offer
        list_href = [offer.find_element(By.TAG_NAME, "a").get_attribute(name="href") for offer in offers]
        list_title = []
        list_address = []
        list_price = []
        list_commission = []
        for i in range(0, 2):
            
            href = list_href[i]

            # переходим на объявление
            driver.get(href)
            sleep(5)

            # получаем title
            title_element = driver.find_element(By.XPATH, f"(//div[@data-name='OfferTitleNew'])")

            title = title_element.find_element(By.TAG_NAME, "h1").get_attribute(name="innerHTML")

            # получаем address
            address_element = driver.find_element(By.XPATH, f"(//div[@data-name='AddressContainer'])")

            address = address_element.find_elements(By.TAG_NAME, "a")

            address = [el.get_attribute(name="innerHTML") for el in address]

            address = ", ".join(address)

            # получаем price

            price_element = driver.find_element(By.XPATH, f"(//div[@data-testid='price-amount'])")

            price = price_element.find_element(By.TAG_NAME, "span").get_attribute(name="innerHTML")

            price = price.split("&nbsp;")
            price = price[0] + price[1]

            # получаем commission
            commission_element = driver.find_element(By.XPATH, f"(//div[@data-name='OfferFactItem'])[3]")
            commission_element = commission_element.find_elements(By.TAG_NAME, "span")[1]
            
            commission = commission_element.get_attribute(name="innerHTML")
        
            list_href.append(href)
            list_title.append(title)
            list_address .append(address)
            list_price.append(price)
            list_commission.append(commission)

        # print(list_href)
        # print(list_title)
        # print(list_address)
        # print(list_price)
        # print(list_commission)

        dataflow_dttm = datetime.datetime.now()
        dataflow_id = hashlib.md5(str(dataflow_dttm).encode()).hexdigest()

        # offers_by_prototype["dataflow_dttm"] += [dataflow_dttm] * len(offers)
        offers_by_prototype["dataflow_dttm"] += [dataflow_dttm] * 2
        # offers_by_prototype["dataflow_id"] += [dataflow_id] * len(offers)
        offers_by_prototype["dataflow_id"] += [dataflow_id] * 2
        # offers_by_prototype["source_sys"] += ["c"] * len(offers)
        offers_by_prototype["source_sys"] += ["c"] * 2
        offers_by_prototype["address"] += list_address
        offers_by_prototype["title"] += list_title
        offers_by_prototype["price"] += list_price
        offers_by_prototype["commission"] += list_commission
        # offers_by_prototype["link_to_offer"] += list_href
        offers_by_prototype["link_to_offer"] += list_href[0:2]

        # print(offers_by_prototype)

        pd.DataFrame(offers_by_prototype).to_csv(f"./test/{dataflow_dttm}.csv", index=False)

        break

    except EndPagesCianException as e:
        print("страницы закончились")
        break

input()

driver.close()