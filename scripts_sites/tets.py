

# циан -  https://rostov.cian.ru/

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

# from selenium.webdriver.firefox.service import Service

# from selenium.webdriver.firefox.options import Options

from selenium.webdriver.chrome.options import Options

options = Options()
# options.add_argument("--headless")

# Указываем путь к geckodriver (замени на свой путь)
# service = Service("/home/mseleznev/бизнес/сервис сбора цен на жилье/geckodriver-v0.36.0-linux64/geckodriver")  

print("global scope [0]: create driver")

# driver = webdriver.Chrome(service=service, options=options)

driver = webdriver.Chrome(options=options)

print("global scope [1]: get driver")

# driver = webdriver.Firefox()

page_url = "https://rostov.cian.ru/" 

driver.get(page_url)

print(f"global scope [2]: get page {page_url}")

# assert "Python" in driver.title
buttom_rent = driver.find_element(By.LINK_TEXT, "Снять")

print("global scope [3]: find buttom_rent")

buttom_rent.click()

# elem.clear()

print("global scope [4]: click buttom_rent")

chose_all_group = driver.find_element(By., "Снять")


# elem.send_keys("pycon")

# print("global scope [5]: send keys")

# elem.send_keys(Keys.RETURN)

# print("global scope [6]: send keys return")

# assert "No results found." not in driver.page_source

input()

driver.close()

print("global scope [7]: driver close")


