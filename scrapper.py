import os
import re
import sys
import time
import base64
import requests
from datetime import datetime
from dotenv import load_dotenv
from config.database import execute_query
from playwright.sync_api import sync_playwright

load_dotenv()

def download_image(element):
    try:
        # Busca o elemento de imagem dentro do container da mensagem
        img = element.query_selector("xpath=.//img[contains(@class, 'StreamMessageEmbed')]")
        if not img:
            return None
        url = img.get_attribute("src")
        response = requests.get(url)
        response.raise_for_status()
        image_base64 = base64.b64encode(response.content).decode('utf-8')
        return image_base64
    except Exception as e:
        print("Erro ao baixar ou converter a imagem:", e)
        return None

def scrap_message(page, symbol, total_messages):
    # Busca todas as mensagens na página
    messages = page.query_selector_all("xpath=.//div[contains(@class, 'StreamMessage_container__')]")
    current_count = len(messages)

    # Itera apenas sobre as mensagens que ainda não foram processadas
    for message in messages[total_messages:]:
        try:
            a_element = message.query_selector("xpath=.//a[contains(@href, '/message/')]")
            if not a_element:
                continue
            href_value = a_element.get_attribute("href")
            match = re.search(r"/message/(\d+)", href_value)
            if not match:
                continue
            message_id = match.group(1)

            select_query = f"select id from tweets where pub_id = {message_id} and symbol = '{symbol}'"
            result = execute_query(select_query)

            time_element = message.query_selector("xpath=.//time")
            date_str = time_element.get_attribute("datetime") if time_element else ""
            date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ") if date_str else None

            if result is not None and len(result) > 0:
                update_query = "UPDATE tweets SET pub_date = %s WHERE pub_id = %s"
                execute_query(update_query, (date, message_id))
                continue

            author_element = message.query_selector("xpath=.//span[@aria-label='Username']")
            author = author_element.text_content().strip() if author_element else ""

            text_element = message.query_selector("xpath=.//div[starts-with(@class, 'RichTextMessage_body__')]")
            text = text_element.text_content().strip() if text_element else ""

            image_base64 = download_image(message)

            insert_query = 'insert into tweets (pub_id, pub_author, pub_text, pub_img, pub_date, symbol) values (%s, %s, %s, %s, %s, %s)'
            insert_data = (message_id, author, text, image_base64, date, symbol)
            execute_query(insert_query, insert_data)
        except Exception as e:
            print("Erro ao extrair mensagem:", e)

    return current_count

def main():
    if len(sys.argv) <= 1:
        print("Nenhum parâmetro foi passado.")
        sys.exit()

    symbol = sys.argv[1]

    with sync_playwright() as p:
        # Inicia o navegador (pode ser headless=True para execução sem interface)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Acessa a página de login
        page.goto('https://stocktwits.com/signin?next=/login')
        time.sleep(3)

        # Preenche os campos de login e senha
        page.fill("input[name='login']", os.getenv("STOCKWITS_USERNAME"))
        page.fill("input[name='password']", os.getenv("STOCKWITS_PASSWORD"))
        page.press("input[name='password']", "Enter")
        time.sleep(5)

        # Acessa a página do símbolo desejado
        page.goto(f'https://stocktwits.com/symbol/{symbol}')
        time.sleep(3)

        SCROLL_PAUSE_TIME = 2
        last_height = page.evaluate("document.body.scrollHeight")
        total_messages = 0

        while True:
            total_messages = scrap_message(page, symbol, total_messages)
            # Rola até o final da página para carregar novas mensagens
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                print("Não há mais conteúdo disponível para carregar.")
                break
            last_height = new_height

        browser.close()

if __name__ == "__main__":
    main()
