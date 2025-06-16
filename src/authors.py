import os
import re
import sys
sys.path.insert(0, '/Users/admin-wana/Projects/finance-tweets-builder')
import time
import concurrent.futures
import logging
from datetime import datetime
from dotenv import load_dotenv
from config.database import execute_query
from playwright.sync_api import sync_playwright

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

load_dotenv()

# SQL statements
GET_AUTHORS_SQL = (
    "SELECT id, author "
    "FROM stocktwits_authors "
    "WHERE execution_counter = 0"
)

UPDATE_AUTHOR_SQL = (
    "UPDATE stocktwits_authors SET "
    "total_following = %s, total_followers = %s, "
    "avg_likes = %s, avg_reshares = %s, avg_comments = %s, "
    "updated_at = %s, execution_counter = execution_counter + 1 "
    "WHERE id = %s"
)

ENGAGEMENT_SQL = (
    "SELECT "
    "AVG(sp.post_likes) AS avg_post_likes, "
    "AVG(sp.post_reshares) AS avg_post_reshares, "
    "AVG(sp.post_comments) AS avg_post_comments "
    "FROM stocktwits_posts sp "
    "WHERE sp.post_author = %s"
)

# Constants
SLEEP_TIME = float(os.getenv('SLEEP_TIME', 4))
RESTART_INTERVAL = int(os.getenv('RESTART_INTERVAL', 100))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 10))


def save_log(message: str, level: str = 'error', print_log: bool = False):
    """
    Inserts a log entry into the database and optionally prints it.
    """
    execute_query("INSERT INTO execution_logs (log) VALUES (%s)", (message,))
    if print_log:
        getattr(logging, level.lower(), logging.error)(message)


def get_authors() -> list:
    """Fetch authors with zero execution count."""
    return execute_query(GET_AUTHORS_SQL)


def parse_count(text: str) -> int:
    """Convert follower/following string (e.g. '1.2k', '3M') into integer."""
    if not text:
        return 0
    txt = text.lower().replace(',', '').strip()
    multipliers = {'k': 1e3, 'm': 1e6}
    match = re.match(r"(?P<number>[\d.]+)(?P<suffix>[km]?)", txt)
    if not match:
        return 0
    number = float(match.group('number'))
    suffix = match.group('suffix')
    return int(number * multipliers.get(suffix, 1))


def get_engagement(author: str) -> tuple:
    """Compute average likes, reshares, comments for an author."""
    rows = execute_query(ENGAGEMENT_SQL, (author,))
    if not rows:
        return 0, 0, 0
    row = rows[0]
    return (
        row.get('avg_post_likes', 0) or 0,
        row.get('avg_post_reshares', 0) or 0,
        row.get('avg_post_comments', 0) or 0,
    )


def update_author_record(author_id, following, followers, likes, reshares, comments):
    """Persist author metrics back to the database."""
    params = (following, followers, likes, reshares, comments, datetime.now(), author_id)
    execute_query(UPDATE_AUTHOR_SQL, params)


def scrape_author_stats(page, author: str) -> tuple:
    """Navigate to author's page and extract following/follower counts."""
    page.goto(f'https://stocktwits.com/{author}', timeout=30_000)
    time.sleep(SLEEP_TIME)
    follow_sel = f"xpath=.//a[contains(@href, '/{author}/following')]//strong"
    follower_sel = f"xpath=.//a[contains(@href, '/{author}/followers')]//strong"

    try:
        following = parse_count(page.query_selector(follow_sel).text_content())
    except Exception:
        following = 0
    try:
        followers = parse_count(page.query_selector(follower_sel).text_content())
    except Exception:
        followers = 0

    return following, followers


def process_authors_subset(authors_subset: list):
    """Process a chunk of authors in a fresh browser instance."""
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Login once per browser
        page.goto('https://stocktwits.com/signin?next=/login')
        time.sleep(SLEEP_TIME)
        page.fill("input[name='login']", os.getenv('STOCKTWITS_USERNAME', ''))
        page.fill("input[name='password']", os.getenv('STOCKTWITS_PASSWORD', ''))
        page.press("input[name='password']", "Enter")
        time.sleep(SLEEP_TIME)

        for idx, record in enumerate(authors_subset, start=1):
            author_id, author = record['id'], record['author']
            try:
                following, followers = scrape_author_stats(page, author)
                likes, reshares, comments = get_engagement(author)
                update_author_record(author_id, following, followers, likes, reshares, comments)
                logging.info(f"Processed {author} | followers: {followers}, following: {following}")
            except Exception as exc:
                save_log(f"Error processing {author}: {exc}")

            if idx % RESTART_INTERVAL == 0:
                # periodic browser restart to free resources
                page.close()
                context.close()
                browser.close()
                time.sleep(2)
                return process_authors_subset(authors_subset[idx:])

        page.close()
        context.close()
        browser.close()


def chunkify(lst, n):
    """Yield n chunks from lst as evenly as possible."""
    k, m = divmod(len(lst), n)
    for i in range(n):
        start = i * k + min(i, m)
        end = start + k + (1 if i < m else 0)
        yield lst[start:end]


def main():
    authors = get_authors()
    if not authors:
        logging.info("No authors to process.")
        return

    subsets = list(chunkify(authors, MAX_WORKERS))
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_authors_subset, subset) for subset in subsets]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                save_log(f"Worker error: {exc}", print_log=True)

if __name__ == '__main__':
    main()
    sys.exit(0)
