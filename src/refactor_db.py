import sys
sys.path.insert(0, '/Users/admin-wana/Projects/finance-tweets-builder')
import os
import base64
from dotenv import load_dotenv
from config.database import execute_query
from PIL import Image, UnidentifiedImageError
from io import BytesIO

load_dotenv()

OUTPUT_DIR = 'images'
MAX_DIMENSION = 720  # px

os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_log(log):
    execute_query("INSERT INTO execution_logs (log) VALUES (%s)", (log,))
    print(log)

def get_tweets(page, last_id=None):
    query = "SELECT * FROM tweets WHERE id > %s ORDER BY id LIMIT 1000" 
    result = execute_query(query, (last_id,))
    return result if result else []

def process_and_save(img_data_b64, img_id):
    if not img_data_b64:
        return None
    
    try:
        # 1) Decodifica Base64
        img_data = base64.b64decode(img_data_b64)
        img = Image.open(BytesIO(img_data))

        # 2) Redimensiona mantendo aspect ratio
        w, h = img.size
        max_orig = max(w, h)
        if max_orig > MAX_DIMENSION:
            scale = MAX_DIMENSION / max_orig
            new_size = (int(w * scale), int(h * scale))
            img = img.resize(new_size, Image.LANCZOS)

        # 3) Define formato e extensão
        fmt = img.format or 'JPEG'
        ext = fmt.lower()
        # Se tiver alpha, força PNG
        if img.mode in ("RGBA", "LA") or (fmt.upper() == 'PNG' and 'A' in img.getbands()):
            fmt = 'PNG'
            ext = 'png'
        else:
            fmt = 'JPEG'
            ext = 'jpg'
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')

        # 4) Caminho do ficheiro
        filename = f"{img_id}.{ext}"
        filepath = os.path.join(OUTPUT_DIR, filename)

        # 5) Salva otimizado
        if fmt == 'JPEG':
            img.save(filepath, fmt, quality=85, optimize=True)
        else:
            img.save(filepath, fmt, optimize=True)

        return filepath

    except (base64.binascii.Error, UnidentifiedImageError) as e:
        # Erro na decodificação ou leitura da imagem
        print(f"[ERRO no process_and_save] id={img_id}: formato inválido ou corrupto ({e})")
        return None
    except Exception as e:
        # Qualquer outro erro
        print(f"[ERRO inesperado no process_and_save] id={img_id}: {e}")
        return None
    
def get_image_path(post_id):
    q = "select post_img_path from stocktwits_posts_tmp where post_id = %s and post_img_path is not null"
    result = execute_query(q, (post_id,))
    if result:
        return result[0]['post_img_path']
    return None

def main():
    last_id = 0
    while True:
        print(f"Fetching tweets after ID: {last_id}")
        tweets = get_tweets(None, last_id)
        last_id = tweets[-1]['id'] if tweets else last_id

        if not tweets:
            print("No more tweets to process.")
            break

        for tweet in tweets:
            # path = process_and_save(tweet['pub_img'], tweet['pub_id'])
            image_path = get_image_path(tweet['pub_id'])

            query = "insert into stocktwits_posts (symbol, post_id, post_author, post_date, post_text, post_img_path, sentiment) values (%s, %s, %s, %s, %s, %s, %s)"
            execute_query(query, (
                tweet['symbol'],
                tweet['pub_id'],
                tweet['pub_author'],
                tweet['pub_date'],
                tweet['pub_text'],
                image_path,
                tweet['sentiment']
            ))


if __name__ == "__main__":
    main()
