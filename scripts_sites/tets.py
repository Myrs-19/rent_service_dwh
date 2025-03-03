# циан -  https://rostov.cian.ru/

import re

from time import sleep

from selenium import webdriver
from selenium.webdriver.common.by import By

from selenium.common.exceptions import InvalidSelectorException

from EndPagesCianException import EndPagesCianException
from NotEqualExceptCountOffersCian import NotEqualExceptCountOffersCian

regex = "p=[0-9]+"

options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
options.add_argument("--window-size=1920,1080")
options.add_argument('--disable-gpu')

driver = webdriver.Chrome(options=options)

driver.maximize_window()

# ссылка на первую страницу https://rostov.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat&region=4959&room1=1&room2=1&room3=1&room4=1&room5=1&type=4&p=3
# переходим по страничкам
count_page = 1
# список заявлений
offers = []
while True:
    try:
        url = f"https://rostov.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=flat&p={count_page}&region=4959&room1=1&room2=1&room3=1&room4=1&room5=1&type=4"
        driver.get(url)
        sleep(10)
        
        page_number_with_p = re.findall("p=[0-9]+", driver.current_url)[0]

        # проверка что редиректа не было 
        actually_page_number = re.findall("[0-9]+", page_number_with_p)[0]
        if int(actually_page_number) != int(count_page):
            raise EndPagesCianException(f"номер страницы по циклу не совпадает с номером по driver.current_url = {actually_page_number}, ожидалось = {count_page}")
            
        offers += driver.find_elements(By.XPATH, f"(//div[@data-testid='offer-card'])")

        print(driver.current_url)

        # кол-во заявлений на странице, подсчитанное вручную, особенность циан
        # для последней страницы не считаем
        arms_count_offers = 28 * count_page
        actually_count_offers = len(offers)
        print(actually_count_offers)
        # if actually_count_offers != arms_count_offers:
        #     raise NotEqualExceptCountOffersCian(f"кол-во страниц загрузилось {actually_count_offers} вместо {arms_count_offers}")

        count_page += 10
    except EndPagesCianException as e:
        print("страницы закончились")
        break

print(f"общее кол-во заявлений = {len(offers)}")

input()

driver.close()