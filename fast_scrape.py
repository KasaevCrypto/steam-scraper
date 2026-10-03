#!/usr/bin/env python3
"""Быстрый сбор цен Steam Market (Rust) через itemordershistogram.

ID предметов берутся из готового кэша (название -> item_nameid), страницы
предметов не открываются. Каждый сервер матрицы берёт каждый N-й предмет и
ходит с постоянным темпом.

Команды:
  scrape  - один сервер: свой кусок списка -> result-<chunk>.csv
  merge   - склеивает result-*.csv в prices.csv, печатает сводку статусов
            и завершается с ошибкой, если доля ok ниже порога.
"""
import argparse
import csv
import glob
import json
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
URL = ("https://steamcommunity.com/market/itemordershistogram"
       "?country=KZ&language=russian&currency=37&item_nameid={}&two_factor=0")
FIELDS = ["name", "is_commodity", "item_nameid", "buy_order", "sell_order",
          "status", "fetched_at"]


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_price(v):
    # Steam отдаёт цену в минимальных единицах валюты (сотых) строкой
    try:
        return round(int(v) / 100, 2)
    except (TypeError, ValueError):
        return ""


def load_ids(path):
    with open(path, encoding="utf-8") as f:
        ids = json.load(f)
    if not isinstance(ids, dict):
        sys.exit("ids-файл должен быть словарём «название -> item_nameid»")
    return list(ids.items())


def fetch(sess, nid, dry):
    """Возвращает (status, buy, sell)."""
    if dry:
        time.sleep(random.uniform(0.005, 0.02))
        r = random.random()
        if r < 0.05:
            return "rate_limited", "", ""
        if r < 0.07:
            return "http_500", "", ""
        return "ok", round(random.uniform(50, 5000), 2), round(random.uniform(50, 5000), 2)
    try:
        r = sess.get(URL.format(nid), timeout=20)
    except Exception:
        return "error", "", ""
    if r.status_code in (429, 403):
        return "rate_limited", "", ""
    if r.status_code != 200:
        return f"http_{r.status_code}", "", ""
    try:
        d = r.json()
    except Exception:
        return "no_data", "", ""
    if not d.get("success"):
        return "no_data", "", ""
    return "ok", to_price(d.get("highest_buy_order")), to_price(d.get("lowest_sell_order"))


def cmd_scrape(a):
    items = load_ids(a.ids)
    mine = items[a.chunk::a.chunks]
    if not mine:
        sys.exit("пустой кусок списка")

    sess = None
    if not a.dry_run:
        import requests
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": UA,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "ru,en;q=0.8",
        })

    t_start = time.time()
    deadline = t_start + a.deadline
    results = {}
    state = {"consec": 0, "done": 0}

    def make_row(name, nid, st, b, s):
        return {"name": name, "is_commodity": "", "item_nameid": nid,
                "buy_order": b, "sell_order": s, "status": st,
                "fetched_at": now_iso() if st != "not_processed" else ""}

    def run_pass(batch, interval):
        failed = []
        nxt = time.time()
        for i, (name, nid) in enumerate(batch):
            if time.time() >= deadline:
                for n2, id2 in batch[i:]:
                    results.setdefault(n2, make_row(n2, id2, "not_processed", "", ""))
                return failed
            wait = nxt - time.time()
            if wait > 0:
                time.sleep(wait)
            st, b, s = fetch(sess, nid, a.dry_run)
            results[name] = make_row(name, nid, st, b, s)
            state["done"] += 1
            if st == "ok":
                state["consec"] = 0
            else:
                failed.append((name, nid))
                if st == "rate_limited":
                    state["consec"] += 1
                    pause = min(30 * 2 ** (state["consec"] - 1), 240)
                    if a.dry_run:
                        pause = 0.05
                    pause = min(pause, max(0, deadline - time.time()))
                    print(f"[chunk {a.chunk}] 429 подряд={state['consec']}, пауза {pause:.0f}с", flush=True)
                    time.sleep(pause)
                    nxt = time.time()
                else:
                    state["consec"] = 0
            nxt += interval * random.uniform(0.85, 1.15)
            if nxt < time.time() - 1:
                nxt = time.time()
            if state["done"] % 25 == 0:
                c = Counter(r["status"] for r in results.values())
                print(f"[chunk {a.chunk}] {state['done']} запросов, "
                      f"{time.time() - t_start:.0f}с, {dict(c)}", flush=True)
        return failed

    failed = run_pass(mine, a.interval)
    for rnd in range(a.retry_rounds):
        if not failed or time.time() >= deadline:
            break
        print(f"[chunk {a.chunk}] повтор {rnd + 1}: {len(failed)} предметов", flush=True)
        time.sleep(0.1 if a.dry_run else min(20, max(0, deadline - time.time())))
        failed = run_pass(failed, a.interval * 2)

    out = f"result-{a.chunk}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for name, nid in mine:
            w.writerow(results.get(name) or make_row(name, nid, "not_processed", "", ""))
    c = Counter(results.get(n, {"status": "not_processed"})["status"] for n, _ in mine)
    print(f"[chunk {a.chunk}] готово за {time.time() - t_start:.0f}с: {dict(c)}", flush=True)


def cmd_merge(a):
    rows = []
    files = sorted(glob.glob(os.path.join(a.dir, "**", "result-*.csv"), recursive=True))
    for fn in files:
        with open(fn, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                r["_file"] = os.path.basename(fn)
                rows.append(r)

    have = {r["name"] for r in rows}
    if a.ids:
        for name, nid in load_ids(a.ids):
            if name not in have:
                rows.append({"name": name, "is_commodity": "", "item_nameid": nid,
                             "buy_order": "", "sell_order": "", "status": "no_result",
                             "fetched_at": "", "_file": "-"})
    if not rows:
        sys.exit("нет данных result-*.csv")

    rows.sort(key=lambda r: r["name"])
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    total = len(rows)
    c = Counter(r["status"] for r in rows)
    ok = c.get("ok", 0)
    share = 100 * ok / total
    out = ["## Steam Rust: сбор цен\n",
           f"Файлов результата: **{len(files)}**" + (f" из {a.expect}" if a.expect else ""),
           f"Предметов: **{total}**, цены получены: **{ok}** (**{share:.1f}%**)\n",
           "| Статус | Предметов |", "|---|---|"]
    for st, n in c.most_common():
        out.append(f"| {st} | {n} |")
    per = Counter(r["_file"] for r in rows if r["status"] != "ok")
    if per:
        out.append("\nНе-ok по серверам: " + ", ".join(f"{k}: {v}" for k, v in sorted(per.items())))
    verdict = "OK" if share >= a.min_ok else f"ниже порога {a.min_ok}%"
    out.append(f"\nИтог: **{verdict}**")
    text = "\n".join(out)
    print(text)
    if a.summary:
        with open(a.summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    if share < a.min_ok:
        sys.exit(1)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    ps = sub.add_parser("scrape")
    ps.add_argument("--chunk", type=int, required=True)
    ps.add_argument("--chunks", type=int, required=True)
    ps.add_argument("--ids", default="Rust.json")
    ps.add_argument("--interval", type=float, default=1.0,
                    help="пауза между запросами на один сервер, сек")
    ps.add_argument("--deadline", type=float, default=780,
                    help="жёсткий лимит времени на сервер, сек")
    ps.add_argument("--retry-rounds", dest="retry_rounds", type=int, default=2)
    ps.add_argument("--dry-run", dest="dry_run", action="store_true")
    ps.set_defaults(fn=cmd_scrape)

    pm = sub.add_parser("merge")
    pm.add_argument("--dir", default=".")
    pm.add_argument("--ids", default="")
    pm.add_argument("--out", default="prices.csv")
    pm.add_argument("--expect", type=int, default=0)
    pm.add_argument("--min-ok", dest="min_ok", type=float, default=95.0)
    pm.add_argument("--summary", default="")
    pm.set_defaults(fn=cmd_merge)

    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
