import requests
import re
import time
import csv
import sys
import urllib.parse
import os

def get_names():
    names = []
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    print("Скачиваем список всех предметов Rust...")
    
    for start in range(0, 5500, 100):
        url = f"https://steamcommunity.com/market/search/render/?query=&start={start}&count=100&search_descriptions=0&sort_column=popular&sort_dir=desc&appid=252490&norender=1"
        
        success = False
        for attempt in range(5):  # 5 попыток на случай временных сбоев
            try:
                resp = session.get(url, timeout=10)
                if resp.status_code == 429:
                    print(f"[get_names] 429 Бан. Ждем {30 * (attempt + 1)} сек...")
                    time.sleep(30 * (attempt + 1))
                    continue
                if resp.status_code == 200:
                    data = resp.json()
                    if 'results' in data and data['results']:
                        for item in data['results']:
                            names.append(item['hash_name'])
                        success = True
                        break
                    else:
                        success = True # Дошли до конца списка
                        break
            except Exception as e:
                print(f"[get_names] Ошибка сети: {e}")
                time.sleep(5)
        
        if not success:
            print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось загрузить предметы со сдвигом {start}")
            
        time.sleep(1.5)
    
    with open('items.txt', 'w', encoding='utf-8') as f:
        for n in names:
            f.write(n + '\n')
    print(f"Найдено {len(names)} предметов. Сохранено в items.txt")

def scrape(chunk_index, total_chunks):
    if not os.path.exists('items.txt'):
        print("Файл items.txt не найден! Прерывание.")
        return
        
    with open('items.txt', 'r', encoding='utf-8') as f:
        items = [line.strip() for line in f if line.strip()]
    
    chunk_size = len(items) // total_chunks + 1
    my_items = items[chunk_index*chunk_size : (chunk_index+1)*chunk_size]
    print(f"Сервер {chunk_index}: обрабатываю {len(my_items)} предметов...")

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    
    filename = f'result_{chunk_index}.csv'
    
    # Создаем файл и пишем заголовки сразу
    if not os.path.exists(filename):
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Name', 'BuyOrder'])

    for name in my_items:
        item_id = None
        buy_order = None
        
        # Шаг 1. ID предмета (с повторами при 429)
        url_item = f"https://steamcommunity.com/market/listings/252490/{urllib.parse.quote(name)}"
        for attempt in range(4):
            try:
                r_item = session.get(url_item, timeout=10)
                if r_item.status_code == 429:
                    print(f"[{name}] Бан 429 на странице предмета. Ждем {30 * (attempt + 1)} сек...")
                    time.sleep(30 * (attempt + 1))
                    continue
                
                match = re.search(r'Market_LoadOrderSpread\(\s*(\d+)\s*\)', r_item.text)
                if match:
                    item_id = match.group(1)
                break
            except Exception as e:
                print(f"[{name}] Ошибка парсинга ID: {e}")
                time.sleep(5)
                
        if not item_id:
            print(f"[{name}] ID не найден. Пропуск.")
            with open(filename, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow([name, "Error: No ID / Blocked"])
            time.sleep(3)
            continue
            
        time.sleep(1.5)

        # Шаг 2. Цена автопокупки (с повторами при 429)
        url_orders = f"https://steamcommunity.com/market/itemordershistogram?country=KZ&language=russian&currency=37&item_nameid={item_id}&two_factor=0"
        for attempt in range(4):
            try:
                r_orders = session.get(url_orders, timeout=10)
                if r_orders.status_code == 429:
                    print(f"[{name}] Бан 429 на API цен. Ждем {30 * (attempt + 1)} сек...")
                    time.sleep(30 * (attempt + 1))
                    continue
                    
                data = r_orders.json()
                raw_buy_order = data.get('highest_buy_order', 0)
                buy_order = (int(raw_buy_order) / 100) if raw_buy_order else 0
                break
            except Exception as e:
                print(f"[{name}] Ошибка API цен: {e}")
                time.sleep(5)

        # ПОСТРОЧНАЯ ЗАПИСЬ (если скрипт упадет, данные не потеряются)
        with open(filename, 'a', newline='', encoding='utf-8') as f:
            if buy_order is not None:
                csv.writer(f).writerow([name, buy_order])
                print(f"{name}: {buy_order}")
            else:
                csv.writer(f).writerow([name, "Error: Failed API"])
                print(f"{name}: Ошибка запроса API")
        
        # Увеличенная пауза для дата-центров Azure
        time.sleep(4)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == '--get-names':
            get_names()
        elif sys.argv[1] == '--scrape':
            scrape(int(sys.argv[2]), int(sys.argv[3]))
