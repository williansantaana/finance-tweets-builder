import os
import re
import sys
import time
import base64
import requests
import concurrent.futures
from datetime import datetime
from dotenv import load_dotenv
from config.database import execute_query
from playwright.sync_api import sync_playwright
from transformers import pipeline

load_dotenv()

def save_log(log):
    execute_query("INSERT INTO execution_logs (log) VALUES (%s)", (log,))
    print(log)

def download_image(element):
    try:
        img = element.query_selector("xpath=.//img[contains(@class, 'StreamMessageEmbed')]")
        if not img:
            return None
        url = img.get_attribute("src")
        response = requests.get(url)
        response.raise_for_status()
        image_base64 = base64.b64encode(response.content).decode('utf-8')
        return image_base64
    except Exception as e:
        save_log(f"Erro ao baixar imagem: {e}")
        return None

def scrap_message(page, symbol, total_messages, pipe=None):
    messages = page.query_selector_all("xpath=.//div[contains(@class, 'StreamMessage_container__')]")
    current_count = len(messages)

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
                continue

            author_element = message.query_selector("xpath=.//span[@aria-label='Username']")
            author = author_element.text_content().strip() if author_element else ""

            text_element = message.query_selector("xpath=.//div[starts-with(@class, 'RichTextMessage_body__')]")
            text = text_element.text_content().strip() if text_element else ""

            image_base64 = download_image(message)

            sentiment = None

            if pipe is not None:
                try:
                    pipe_result = pipe(text)
                    if isinstance(pipe_result, list) and len(pipe_result) > 0:
                        sentiment = pipe_result[0]['label']
                    elif isinstance(pipe_result, dict):
                        sentiment = pipe_result['label']
                except Exception as e:
                    sentiment = None

            insert_query = 'insert into tweets (pub_id, pub_author, pub_text, pub_img, pub_date, symbol, sentiment) values (%s, %s, %s, %s, %s, %s, %s)'
            insert_data = (message_id, author, text, image_base64, date, symbol, sentiment)

            execute_query(insert_query, insert_data)
        except Exception as e:
            save_log(f"Erro ao processar mensagem: {e}")

    return current_count

def get_symbols():
    select_query = f"SELECT symbol FROM symbols ORDER BY execution_counter, id"
    result = execute_query(select_query)
    return [row['symbol'] for row in result] if result else []

def process_symbol(symbol, pipe):
    update_query = f"UPDATE symbols SET execution_counter = execution_counter + 1 WHERE symbol = '{symbol}'"
    execute_query(update_query)

    with sync_playwright() as p:
        SLEEP_TIME = 5
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto('https://stocktwits.com/signin?next=/login')
        time.sleep(SLEEP_TIME)

        page.fill("input[name='login']", os.getenv("STOCKTWITS_USERNAME"))
        page.fill("input[name='password']", os.getenv("STOCKTWITS_PASSWORD"))
        time.sleep(SLEEP_TIME)
        page.press("input[name='password']", "Enter")
        time.sleep(SLEEP_TIME)

        page.goto(f'https://stocktwits.com/symbol/{symbol}')
        time.sleep(SLEEP_TIME)

        last_height = page.evaluate("document.body.scrollHeight")
        total_messages = 0

        while True:
            total_messages = scrap_message(page, symbol, total_messages, pipe)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SLEEP_TIME)
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                save_log(f"Symbol {symbol}: Não há mais conteúdo disponível para carregar.")
                break
            last_height = new_height

        browser.close()

def main():
    symbols = get_symbols()
    if not symbols:
        save_log("Nenhum símbolo encontrado na base de dados.")
        sys.exit()

    max_workers = int(os.getenv("MAX_WORKERS", 5))
    symbol_iter = iter(symbols)
    future_to_symbol = {}

    pipe = pipeline(
        "sentiment-analysis",
        model="StephanAkkerman/FinTwitBERT-sentiment",
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submete as 5 primeiras tarefas
        for _ in range(max_workers):
            try:
                symbol = next(symbol_iter)
                future = executor.submit(process_symbol, symbol, pipe)
                future_to_symbol[future] = symbol
                
                save_log(f"Submetendo tarefa para o símbolo: {symbol}")
            except StopIteration:
                break

        # À medida que uma tarefa é concluída, submete a próxima
        while future_to_symbol:
            done, _ = concurrent.futures.wait(
                future_to_symbol, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                symbol_concluido = future_to_symbol.pop(future)
                try:
                    future.result()
                    save_log(f"Symbol {symbol_concluido} concluído com sucesso.")
                except Exception as exc:
                    save_log(f"Symbol {symbol_concluido} gerou uma exceção: {exc}")

                # Submete nova tarefa se houver symbol disponível
                try:
                    next_symbol = next(symbol_iter)
                    new_future = executor.submit(process_symbol, next_symbol, pipe)
                    future_to_symbol[new_future] = next_symbol

                    save_log(f"Submetendo tarefa para o símbolo: {symbol}")
                except StopIteration:
                    pass

    save_log(f"Execução concluída às: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
