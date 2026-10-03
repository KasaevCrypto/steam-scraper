#!/usr/bin/env python3
"""Steam Market: тест максимальной скорости с серверов GitHub (Azure).

Две команды:
  probe   - запускается на каждом сервере матрицы; все серверы стартуют в одну
            и ту же секунду (--start) и ступенями ускоряются. Каждый запрос
            пишется в probe-<job>.jsonl.
  report  - собирает все jsonl и печатает сводку по ступеням: сколько запросов
            в секунду шло суммарно и с какой ступени начались 429.

Сервер останавливается сам после --stop-after неудачных запросов подряд,
чтобы не долбить Steam, когда блок уже случился.
"""
import argparse
import glob
import json
import os
import random
import statistics
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
M = "https://steamcommunity.com/market"
APP_ID = 252490


def build_url(endpoint, ident):
    if endpoint == "histogram":
        return (f"{M}/itemordershistogram?country=KZ&language=russian"
                f"&currency=37&item_nameid={ident}&two_factor=0")
    return f"{M}/search/render/?query=&start={ident}&count=10&appid={APP_ID}&norender=1"


def egress_ip(requests):
    for u in ("https://api.ipify.org", "https://ifconfig.me/ip"):
        try:
            return requests.get(u, timeout=8).text.strip()
        except Exception:
            pass
    return "unknown"


def cmd_probe(a):
    intervals = [float(x) for x in a.intervals.split(",")]

    if a.dry_run:
        requests = None
        ip = "dry-run"
    else:
        import requests
        from requests.adapters import HTTPAdapter
        ip = egress_ip(requests)

    values = []
    if a.endpoint == "histogram":
        ids = json.load(open(a.ids, encoding="utf-8"))
        values = list(ids.values()) if isinstance(ids, dict) else list(ids)
        pool = values[a.job::a.jobs]
    else:
        pool = list(range(0, 5521, 10))[a.job::a.jobs]
    if not pool:
        sys.exit("пустой пул идентификаторов")

    sess = None
    if not a.dry_run:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": UA,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "ru,en;q=0.8",
        })
        sess.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=a.workers))

    print(f"[job {a.job}] ip={ip} pool={len(pool)} stages={intervals} "
          f"stage_len={a.stage_len}s start_in={a.start - time.time():.0f}s", flush=True)

    lock = threading.Lock()
    state = {"stop": False, "bad_run": 0, "reason": ""}
    rows = []
    counts = Counter()

    def do_req(ident, stage, interval):
        t0 = time.time()
        ra = None
        if a.dry_run:
            time.sleep(random.uniform(0.05, 0.2))
            st = 429 if (stage >= 1 and random.random() < 0.3) else 200
        else:
            try:
                r = sess.get(build_url(a.endpoint, ident), timeout=20)
                st = r.status_code
                ra = r.headers.get("Retry-After")
                if st == 200:
                    try:
                        body = r.json()
                        good = bool(body.get("success")) if a.endpoint == "histogram" \
                            else bool(body.get("results"))
                    except Exception:
                        good = False
                    if not good:
                        st = 299  # 200, но данных нет
            except Exception:
                st = 0
        lat = time.time() - t0
        with lock:
            rows.append({"job": a.job, "ip": ip, "t": round(t0, 3),
                         "rel": round(t0 - a.start, 3), "stage": stage,
                         "interval": interval, "status": st,
                         "lat": round(lat, 3), "ra": ra})
            counts[st] += 1
            if st == 200:
                state["bad_run"] = 0
            else:
                state["bad_run"] += 1
                if state["bad_run"] >= a.stop_after and not state["stop"]:
                    state["stop"] = True
                    state["reason"] = f"{a.stop_after} неудач подряд, последний статус {st}"

    wait = a.start - time.time()
    if wait > 0:
        time.sleep(wait)

    idx = 0
    nxt = time.time()
    last_print = 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        while not state["stop"]:
            now = time.time()
            stage = int((now - a.start) // a.stage_len)
            if stage >= len(intervals):
                break
            if now < nxt:
                time.sleep(min(nxt - now, 0.05))
                continue
            ident = pool[idx % len(pool)]
            idx += 1
            ex.submit(do_req, ident, stage, intervals[stage])
            nxt += intervals[stage] * random.uniform(0.85, 1.15)
            if nxt < now - 1:
                nxt = now
            if now - last_print >= 10:
                last_print = now
                with lock:
                    print(f"[job {a.job}] t=+{now - a.start:.0f}s stage={stage} "
                          f"interval={intervals[stage]}s sent={idx} {dict(counts)}", flush=True)
    # ThreadPoolExecutor дожидается завершения всех запросов при выходе из with

    with open(f"probe-{a.job}.jsonl", "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[job {a.job}] готово: {dict(counts)} "
          f"{'СТОП: ' + state['reason'] if state['stop'] else 'все ступени пройдены'}",
          flush=True)


def pct(v, p):
    if not v:
        return 0.0
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))]


def cmd_report(a):
    rows = []
    for fn in glob.glob(os.path.join(a.dir, "**", "probe-*.jsonl"), recursive=True):
        with open(fn, encoding="utf-8") as f:
            rows += [json.loads(l) for l in f if l.strip()]
    if not rows:
        sys.exit("нет данных probe-*.jsonl")
    rows.sort(key=lambda r: r["t"])

    ip_by_job = {r["job"]: r["ip"] for r in rows}
    jobs = sorted(ip_by_job)
    ips = Counter(ip_by_job.values())
    intervals = sorted({(r["stage"], r["interval"]) for r in rows})

    out = []
    out.append("## Steam: тест скорости\n")
    out.append(f"Серверов с данными: **{len(jobs)}**, уникальных IP: **{len(ips)}**")
    shared = {ip: n for ip, n in ips.items() if n > 1}
    if shared:
        out.append(f"IP, общие для нескольких серверов: {shared}")
    out.append("")

    out.append("| Ступень | Пауза/сервер, с | Серверов | Запросов | Общий темп, зап/с | 200 | 429 | прочее | p50 lat, с | p95 lat, с |")
    out.append("|---|---|---|---|---|---|---|---|---|---|")
    first_bad_stage = None
    stage_summary = []
    for stage, interval in intervals:
        rs = [r for r in rows if r["stage"] == stage]
        n = len(rs)
        c = Counter(r["status"] for r in rs)
        n200, n429 = c.get(200, 0), c.get(429, 0)
        other = n - n200 - n429
        active = len({r["job"] for r in rs})
        rel = [r["rel"] for r in rs]
        span = max(a.stage_len, 1) if rel else 1
        rps = n / span
        lat = [r["lat"] for r in rs]
        bad_share = (n - n200) / n if n else 0
        if bad_share > 0.01 and first_bad_stage is None:
            first_bad_stage = stage
        stage_summary.append({"stage": stage, "interval": interval, "jobs": active, "sent": n,
                              "rps": round(rps, 2), "ok": n200, "r429": n429, "other": other})
        out.append(f"| {stage} | {interval} | {active} | {n} | {rps:.1f} | {n200} | {n429} | "
                   f"{other} | {pct(lat, .5):.2f} | {pct(lat, .95):.2f} |")
    out.append("")

    bad = [r for r in rows if r["status"] in (429, 403)]
    if bad:
        b = bad[0]
        t429 = b["t"]
        span30 = max(1.0, min(30.0, t429 - rows[0]["t"]))
        window = [r for r in rows if t429 - span30 <= r["t"] <= t429]
        rate = len(window) / span30
        out.append(f"Первый 429/403: через **{b['rel']:.0f} с** от старта, ступень {b['stage']}, "
                   f"сервер {b['job']}. Темп за предыдущие 30 с (все серверы): **{rate:.1f} зап/с**.")
        ra = [r["ra"] for r in bad if r.get("ra")]
        if ra:
            out.append(f"Заголовок Retry-After встречался: {Counter(ra).most_common(3)}")
        firsts = defaultdict(float)
        for r in bad:
            firsts.setdefault(r["job"], r["rel"])
        spread = max(firsts.values()) - min(firsts.values())
        out.append(f"Серверов, получивших блок: {len(firsts)} из {len(jobs)}; "
                   f"разброс времени первого блока: {spread:.0f} с "
                   f"(маленький разброс = общий лимит, большой = лимиты по серверам).")
    else:
        out.append("429/403 не было ни на одной ступени: потолок выше самой быстрой ступени, "
                   "добавьте более короткие паузы в intervals.")

    if first_bad_stage is not None:
        prev = [s for s in stage_summary if s["stage"] < first_bad_stage]
        if prev:
            out.append(f"\n**Последняя чистая ступень: {prev[-1]['stage']} "
                       f"(≈{prev[-1]['rps']} зап/с суммарно).** "
                       f"Проблемы с ступени {first_bad_stage}.")
        else:
            out.append("\nПроблемы уже на первой ступени: нужны более длинные паузы.")

    text = "\n".join(out)
    print(text)
    if a.summary:
        with open(a.summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    with open("speedtest-summary.json", "w", encoding="utf-8") as f:
        json.dump({"jobs": len(jobs), "unique_ips": len(ips), "stages": stage_summary},
                  f, ensure_ascii=False, indent=2)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    pp = sub.add_parser("probe")
    pp.add_argument("--job", type=int, required=True)
    pp.add_argument("--jobs", type=int, required=True)
    pp.add_argument("--start", type=float, required=True, help="unix-время общего старта")
    pp.add_argument("--ids", default="Rust.json")
    pp.add_argument("--endpoint", choices=["histogram", "search"], default="histogram")
    pp.add_argument("--intervals", default="4,3,2,1.5,1,0.7,0.5",
                    help="пауза между запросами на ОДИН сервер по ступеням, сек")
    pp.add_argument("--stage-len", dest="stage_len", type=float, default=60)
    pp.add_argument("--stop-after", dest="stop_after", type=int, default=5)
    pp.add_argument("--workers", type=int, default=24)
    pp.add_argument("--dry-run", dest="dry_run", action="store_true")
    pp.set_defaults(fn=cmd_probe)

    pr = sub.add_parser("report")
    pr.add_argument("--dir", default=".")
    pr.add_argument("--stage-len", dest="stage_len", type=float, default=60)
    pr.add_argument("--summary", default="")
    pr.set_defaults(fn=cmd_report)

    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
