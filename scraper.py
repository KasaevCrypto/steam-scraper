#!/usr/bin/env python3
"""Сбор цен Steam Market (Rust) в GitHub Actions.

Команды:
  names                    собрать список предметов -> items.json
  scrape CHUNK TOTAL       цены для своей части списка -> out/result_N.csv, out/ids_N.json
  merge PARTS_DIR          склеить части -> prices.csv, обновить ids.json
"""
import argparse
import csv
import glob
import json
import os
import random
import re
import sys
import time
import urllib.parse
from collections import Counter
from datetime import datetime, timezone

import requests

APP_ID = 252490  # Rust
MARKET = "https://steamcommunity.com/market"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
PAGE_SIZE = 10  # Steam сейчас отдаёт максимум 10 результатов за запрос
ITEMS_FILE = "items.json"  # {hash_name: is_commodity}
IDS_FILE = "ids.json"      # {hash_name: item_nameid}
OUT_DIR = "out"

COUNTRY = "KZ"
CURRENCY = 37  # тенге
ID_PATTERNS = [
    re.compile(r"Market_LoadOrderSpread\(\s*(\d+)\s*\)"),
    re.compile(r"ItemActivityTicker\.Start\(\s*(\d+)\s*\)"),
]
HEADER = ["name", "is_commodity", "item_nameid", "buy_order", "sell_order", "status", "fetched_at"]
BLOCK_STATUSES = {"rate_limited", "http_403", "network"}
MAX_CONSECUTIVE_BLOCKS = 3  # столько подряд блокировок -> чанк останавливается, минуты не жжём


def log(msg):
    print(msg, flush=True)


def die(msg):
    log(f"ОШИБКА: {msg}")
    sys.exit(1)


class Deadline:
    def __init__(self, minutes):
        self.end = time.monotonic() + minutes * 60

    def left(self):
        return self.end - time.monotonic()


def nap(dl, seconds):
    time.sleep(max(0.0, min(seconds, dl.left())))


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": MARKET + "/",
    })
    return s


def load_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)


def load_ids():
    """Читает ids.json. Понимает и формат {name: id}, и {name: {"item_nameid": id, ...}}."""
    raw = load_json(IDS_FILE, {})
    ids = {}
    for name, v in raw.items():
        if isinstance(v, dict):
            v = v.get("item_nameid")
        if v:
            ids[name] = int(v)
    return ids


def load_items():
    """Читает items.json -> (items, legacy_ids).
    items: {name: is_commodity (bool|None если неизвестно)}
    Понимает форматы значений: bool (наш), int (старая база {name: item_nameid}),
    dict ({"item_nameid": .., "is_commodity": ..}).
    """
    raw = load_json(ITEMS_FILE, None)
    if not raw:
        return None, {}
    items, ids = {}, {}
    for name, v in raw.items():
        if isinstance(v, bool):
            items[name] = v
        elif isinstance(v, int):
            items[name] = None
            ids[name] = v
        elif isinstance(v, dict):
            items[name] = bool(v["is_commodity"]) if "is_commodity" in v else None
            if v.get("item_nameid"):
                ids[name] = int(v["item_nameid"])
        else:
            items[name] = None
    return items, ids


def comm(v):
    return "" if v is None else int(v)


def fetch(session, url, dl, params=None, tries=5, label=""):
    """GET с повторами. Возвращает (status, response).
    status: ok | rate_limited | network | deadline | http_NNN
    """
    last = "network"
    for attempt in range(tries):
        if dl.left() < 5:
            return "deadline", None
        try:
            r = session.get(url, params=params, timeout=20)
        except requests.RequestException as e:
            last = "network"
            log(f"  [{label}] сеть: {type(e).__name__}")
            nap(dl, 5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return "ok", r
        if r.status_code == 429:
            last = "rate_limited"
            wait = min(30 * 2 ** attempt, 240)
            log(f"  [{label}] 429, жду {wait} с (попытка {attempt + 1}/{tries})")
            nap(dl, wait)
            continue
        if r.status_code in (500, 502, 503, 504):
            last = f"http_{r.status_code}"
            nap(dl, 10 * (attempt + 1))
            continue
        # 400/403/404 и т.п.: повтор обычно бессмыслен
        return f"http_{r.status_code}", r
    return last, None


# ─── names ───────────────────────────────────────────────────

def cmd_names(a):
    dl = Deadline(a.deadline_min)
    s = make_session()
    items = {}
    start, total = 0, None
    log("Собираю список предметов Rust...")
    while total is None or start < total:
        status, r = fetch(
            s, f"{MARKET}/search/render/", dl, label=f"search start={start}",
            params={
                "query": "", "start": start, "count": PAGE_SIZE,
                "search_descriptions": 0, "sort_column": "name", "sort_dir": "asc",
                "appid": APP_ID, "norender": 1, "currency": 1, "language": "english",
            },
        )
        if status != "ok":
            die(f"поиск на start={start} не удался: {status}. items.json не создан.")
        try:
            data = r.json()
        except ValueError:
            die(f"поиск на start={start}: ответ не JSON")
        if not data.get("success"):
            die(f"поиск на start={start}: success=false")
        total = int(data.get("total_count", 0))
        for it in data.get("results", []):
            name = it.get("hash_name")
            if name:
                items[name] = bool((it.get("asset_description") or {}).get("commodity"))
        if (start // PAGE_SIZE) % 50 == 0:
            log(f"  start={start}/{total}, уникальных: {len(items)}")
        start += PAGE_SIZE
        nap(dl, a.delay + random.uniform(0, 0.5))

    if not total or len(items) < 0.95 * total:
        die(f"собрано {len(items)} из {total}: список неполный, items.json не записан")
    save_json(ITEMS_FILE, items)
    log(f"Готово: {len(items)} предметов (total_count={total}) -> {ITEMS_FILE}")


# ─── scrape ──────────────────────────────────────────────────

def find_nameid(html):
    for pat in ID_PATTERNS:
        m = pat.search(html)
        if m:
            return int(m.group(1))
    return None


def cents(value):
    try:
        return round(int(value) / 100, 2) if value else ""
    except (TypeError, ValueError):
        return ""


def process_item(s, name, ids, new_ids, a, dl):
    """-> (status, nameid, buy, sell)"""
    nameid = ids.get(name) or new_ids.get(name)
    if not nameid:
        url = f"{MARKET}/listings/{APP_ID}/{urllib.parse.quote(name, safe='')}"
        status, r = fetch(s, url, dl, tries=3, label=f"page {name}")
        if status != "ok":
            return status, None, "", ""
        nameid = find_nameid(r.text)
        if not nameid:
            return "no_id", None, "", ""
        new_ids[name] = nameid
        nap(dl, a.delay + random.uniform(0, 1))

    status, r = fetch(
        s, f"{MARKET}/itemordershistogram", dl, tries=3, label=f"price {name}",
        params={
            "country": COUNTRY, "language": "english", "currency": CURRENCY,
            "item_nameid": nameid, "two_factor": 0,
        },
    )
    if status != "ok":
        return status, nameid, "", ""
    try:
        d = r.json()
    except ValueError:
        return "bad_json", nameid, "", ""
    if not d.get("success"):
        return "no_success", nameid, "", ""
    return "ok", nameid, cents(d.get("highest_buy_order")), cents(d.get("lowest_sell_order"))


def cmd_scrape(a):
    dl = Deadline(a.deadline_min)
    items, legacy_ids = load_items()
    if not items:
        die(f"{ITEMS_FILE} не найден или пуст")
    names = sorted(items)
    mine = names[a.chunk::a.total]  # через одного: нагрузка ровная
    if a.limit > 0:
        mine = mine[:a.limit]
    ids = {**legacy_ids, **load_ids()}  # ids.json новее старой базы
    new_ids = {}
    log(f"Чанк {a.chunk}/{a.total}: {len(mine)} предметов, известных ID: {len(ids)}")

    os.makedirs(OUT_DIR, exist_ok=True)
    res_path = os.path.join(OUT_DIR, f"result_{a.chunk}.csv")
    ids_path = os.path.join(OUT_DIR, f"ids_{a.chunk}.json")
    s = make_session()
    stats = Counter()
    blocks = 0
    stop_reason = None

    with open(res_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        f.flush()
        try:
            for i, name in enumerate(mine, 1):
                if dl.left() < 20:
                    stop_reason = "deadline"
                elif blocks >= MAX_CONSECUTIVE_BLOCKS:
                    stop_reason = "blocked"
                if stop_reason:
                    log(f"Останавливаюсь ({stop_reason}) на {i - 1}/{len(mine)}")
                    for rest in mine[i - 1:]:
                        w.writerow([rest, comm(items[rest]), ids.get(rest, ""), "", "", f"not_processed_{stop_reason}", ""])
                        stats[f"not_processed_{stop_reason}"] += 1
                    break

                status, nameid, buy, sell = process_item(s, name, ids, new_ids, a, dl)
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                w.writerow([name, comm(items[name]), nameid or "", buy, sell, status, ts])
                f.flush()
                stats[status] += 1
                blocks = blocks + 1 if status in BLOCK_STATUSES else 0
                if i % 20 == 0 or status != "ok":
                    log(f"[{i}/{len(mine)}] {name}: {status} buy={buy} sell={sell}")
                nap(dl, a.delay + random.uniform(0, 1))
        finally:
            save_json(ids_path, new_ids)

    log(f"Итог чанка {a.chunk}: {dict(stats)}; новых ID: {len(new_ids)}")


# ─── merge ───────────────────────────────────────────────────

def cmd_merge(a):
    rows = []
    for p in sorted(glob.glob(os.path.join(a.parts, "result_*.csv"))):
        with open(p, newline="", encoding="utf-8") as f:
            rows.extend(csv.DictReader(f))

    ids = load_ids()
    before = len(ids)
    for p in sorted(glob.glob(os.path.join(a.parts, "ids_*.json"))):
        ids.update({k: int(v) for k, v in load_json(p, {}).items()})
    if len(ids) != before:
        save_json(IDS_FILE, ids)

    rows.sort(key=lambda r: r["name"])
    with open("prices.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)

    stats = Counter(r["status"] for r in rows)
    ok = stats.get("ok", 0)
    lines = [
        "### Результат сбора цен",
        f"- Строк всего: **{len(rows)}**, с ценой (ok): **{ok}**",
        f"- Известных item_nameid: {len(ids)} (новых: {len(ids) - before})",
        "",
        "| status | кол-во |",
        "|---|---|",
    ] + [f"| {k} | {v} |" for k, v in stats.most_common()]
    summary = "\n".join(lines)
    log(summary)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(summary + "\n")
    if ok == 0:
        die("ни одной цены не получено (смотрите статусы выше)")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    n = sub.add_parser("names")
    n.add_argument("--delay", type=float, default=2.5)
    n.add_argument("--deadline-min", type=float, default=40)

    sc = sub.add_parser("scrape")
    sc.add_argument("chunk", type=int)
    sc.add_argument("total", type=int)
    sc.add_argument("--delay", type=float, default=3.5)
    sc.add_argument("--limit", type=int, default=0)
    sc.add_argument("--deadline-min", type=float, default=35)

    m = sub.add_parser("merge")
    m.add_argument("parts")

    a = p.parse_args()
    {"names": cmd_names, "scrape": cmd_scrape, "merge": cmd_merge}[a.cmd](a)


if __name__ == "__main__":
    main()
