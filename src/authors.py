import os
import re
import sys
import time
import concurrent.futures
from datetime import datetime
from dotenv import load_dotenv
from config.database import execute_query
from playwright.sync_api import sync_playwright

load_dotenv()

def save_log(log):
    execute_query("INSERT INTO execution_logs (log) VALUES (%s)", (log,))
    print(log)


def get_authors():
    select_query = f"SELECT id, author FROM stocktwits_authors ORDER BY execution_counter, id DESC LIMIT 1000"
    return execute_query(select_query)


def main():
    authors = get_authors()
    if not authors:
        save_log("Nenhum autor encontrado na base de dados.")
        sys.exit()

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

        for item in authors:
            author = item['author']
            id = item['id']
            page.goto(f'https://stocktwits.com/{author}')
            time.sleep(SLEEP_TIME)

            following_a = page.query_selector(f"xpath=.//a[contains(@href, '/{author}/following')]")
            followers_a = page.query_selector(f"xpath=.//a[contains(@href, '/{author}/followers')]")

            following = following_a.query_selector("xpath=.//strong").text_content().strip()
            followers = followers_a.query_selector("xpath=.//strong").text_content().strip()
            
            query_engagement = """
                SELECT avg(sp.post_likes) AS avg_post_likes, avg(sp.post_reshares) AS avg_post_reshares, avg(sp.post_comments) AS avg_post_comments
                FROM stocktwits_posts sp 
                WHERE sp.post_author = %s
            """
            engagement = execute_query(query_engagement, (author,))

            avg_post_likes = engagement[0]['avg_post_likes'] if engagement else 0
            avg_post_reshares = engagement[0]['avg_post_reshares'] if engagement else 0
            avg_post_comments = engagement[0]['avg_post_comments'] if engagement else 0

            update_query = "UPDATE stocktwits_authors SET total_following = %s, total_followers = %s, avg_likes = %s, avg_reshares %s, avg_comments %s, updata_at %s, execution_counter = execution_counter + 1 WHERE id = %s"
            update_data = (following, followers, avg_post_likes, avg_post_reshares, avg_post_comments, datetime.now(), id)
            execute_query(update_query, update_data)
        
        browser.close()

    # save_log(f"Execução concluída às: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
    sys.exit(0)
