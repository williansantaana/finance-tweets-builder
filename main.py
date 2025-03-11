import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from config.database import execute_query
from datetime import datetime
import time
import csv
import re
import sys
import base64
import requests

def download_image(element):
    try:
        url = element.find_element(By.XPATH, ".//img[contains(@class, 'StreamMessageEmbed')]").get_attribute("src")
        response = requests.get(url)
        response.raise_for_status()
        image_base64 = base64.b64encode(response.content).decode('utf-8')
        return image_base64
    except Exception as e:
        print("Erro ao baixar ou converter a imagem")
        return None

def scrap_message(driver, symbol, total_messages):
    messages = driver.find_elements(By.XPATH, ".//div[contains(@class, 'StreamMessage_container__')]")

    for message in messages[total_messages:]:
        try:
            href_value = message.find_element(By.XPATH, ".//a[contains(@href, '/message/')]").get_attribute("href")
            match = re.search(r"/message/(\d+)", href_value)
            if not match: continue
            message_id = match.group(1)

            select_query = f'select id from tweets where pub_id = {message_id}'
            result = execute_query(select_query)

            if result is not None and len(result) > 0: continue

            author = message.find_element(By.XPATH, ".//span[@aria-label='Username']").text.strip()
            date_str = message.find_element(By.XPATH, ".//time").get_attribute("datetime")
            date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            text = message.find_element(By.XPATH, ".//div[starts-with(@class, 'RichTextMessage_body__')]").text.strip()
            image_base64 = download_image(message)

            insert_query = 'insert into tweets (pub_id, pub_author, pub_text, pub_img, pub_date, symbol) values (%s, %s, %s, %s, %s, %s)'
            insert_data = (message_id, author, text, image_base64, date, symbol)

            execute_query(insert_query, insert_data)

        except Exception as e:
            print("Erro ao extrair mensagem:", e)

    return len(messages)


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("Nenhum parâmetro foi passado.")
        sys.exit()

    symbol = sys.argv[1]

    driver = webdriver.Chrome()
    driver.get('https://stocktwits.com/signin?next=/login')
    time.sleep(3)
    username_input = driver.find_element(By.NAME, 'login')
    password_input = driver.find_element(By.NAME, 'password')
    username_input.send_keys(os.getenv("STOCKWITS_USERNAME"))
    password_input.send_keys(os.getenv("STOCKWITS_PASSWORD"))
    password_input.send_keys(Keys.RETURN)
    time.sleep(5)
    driver.get(f'https://stocktwits.com/symbol/{symbol}')
    time.sleep(3)

    SCROLL_PAUSE_TIME = 2
    last_height = driver.execute_script("return document.body.scrollHeight")
    total_messages = 0

    while True:
        total_messages = scrap_message(driver, symbol, total_messages)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            print("Não há mais conteúdo disponível para carregar.")
            break
        last_height = new_height

    driver.quit()