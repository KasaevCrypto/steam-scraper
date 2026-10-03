import requests
import re
import time
import csv
import sys
import urllib.parse

def get_names():
    names = []
    session = requests.Session()
    print("Скачиваем список всех предметов Rust...")
    for start in range(0, 5500, 100):
        url = f"https://steamcommunity.com/market/search/render/?query=&start={start}&count=100&search_descriptions=0&sort_column=popular&sort_dir=desc&appid=252490&norender=1"
        resp = session.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if 'results' in data and data['results']:
                for item in data['results']:
                    names.append(item['hash_name'])
            else:
                break
        time.sleep(1)
    
    with open('items.txt', 'w', encoding='utf-8') as f:
        for n in names:
            f.write(n + '\n')
    print(f"Найдено {len(names)} предметов. Сохранено в items.txt")

def scrape(chunk_index, total_chunks):
    # Читаем список предметов
    with open('items.txt', 'r', encoding='utf-8') as f:
        items = [line.strip() for line in f if line.strip()]
    
    # Разбиваем на 20 частей (по ~250 предметов на каждый сервер)
    chunk_size = len(items) // total_chunks + 1
    my_items = items[chunk_index*chunk_size : (chunk_index+1)*chunk_size]
    print(f"Сервер {chunk_index}: обрабатываю {len(my_items)} предметов...")

    results = []
    session = requests.Session()
    # Притворяемся обычным браузером
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

    for name in my_items:
        try:
            # Шаг 1: Заходим на страницу предмета, чтобы достать скрытый ID
            url_item = f"https://steamcommunity.com/market/listings/252490/{urllib.parse.quote(name)}"
            r_item = session.get(url_item, timeout=10)
            
            if r_item.status_code == 429:
                print(f"[{name}] Временный блок (429). Ждем 30 сек...")
                time.sleep(30)
                continue

            match = re.search(r'Market_LoadOrderSpread\(\s*(\d+)\s*\)', r_item.text)
            if not match:
                continue
            item_id = match.group(1)

            time.sleep(1) # Небольшая пауза

            # Шаг 2: Делаем запрос к скрытому API Steam за ценой автопокупки (в тенге)
            url_orders = f"https://steamcommunity.com/market/itemordershistogram?country=KZ&language=russian&currency=37&item_nameid={item_id}&two_factor=0"
            r_orders = session.get(url_orders, timeout=10).json()

            buy_order = r_orders.get('highest_buy_order', 0)
            if buy_order:
                buy_order = int(buy_order) / 100 # Steam выдает цену умноженную на 100

            results.append([name, buy_order])
            print(f"{name}: {buy_order}")
        except Exception as e:
            pass
        
        # Обязательная пауза, чтобы сервер не забанили
        time.sleep(3) 

    # Сохраняем кусок данных
    with open(f'result_{chunk_index}.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Name', 'BuyOrder'])
        writer.writerows(results)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == '--get-names':
            get_names()
        elif sys.argv[1] == '--scrape':
            scrape(int(sys.argv[2]), int(sys.argv[3]))
