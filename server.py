"""
Gift Watcher: следит за новыми улучшенными подарками сразу во многих коллекциях.

Запуск локально:  pip install flask requests gunicorn && python server.py
Продакшен:        gunicorn server:app --workers 1 --threads 4
(workers именно 1: фоновый опрос и память с событиями живут в одном процессе)

Диагностика:
  /api/test?slug=PlushPepe&n=1   что именно отдаёт t.me и как это разобралось
  /api/debug                     состояние каждой коллекции
"""
import html
import os
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, jsonify, request, send_from_directory

HERE = os.path.dirname(os.path.abspath(__file__))
COLL_FILE = os.path.join(HERE, "collections.txt")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
SLUG_RE = re.compile(r"[A-Za-z0-9]{1,64}")

POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", 8))
THREADS = int(os.environ.get("THREADS", 6))
MAX_EVENTS = 1000
MAX_COLLECTIONS = 200
RETRY_DEAD_AFTER = 1800

app = Flask(__name__)
lock = threading.Lock()
events = deque(maxlen=MAX_EVENTS)
counter = 0
nxt = {}          # slug -> следующий номер, который надо проверить
dead = {}         # slug -> когда пробовать снова (только если коллекции реально нет)
state = {}        # slug -> текст статуса для /api/debug
last_error = ""
last_error_ts = 0


class Throttled(Exception):
    pass


class BadStatus(Exception):
    pass


ROW_RE = re.compile(
    r"<t[hd][^>]*>\s*(?:<[^>]+>\s*)*(Owner|Model|Backdrop|Symbol|Quantity)\s*(?:<[^>]+>\s*)*</t[hd]>"
    r"\s*<td[^>]*>(.*?)</td>", re.S | re.I)


def clean(s):
    s = re.sub(r"<mark[^>]*>.*?</mark>", "", s, flags=re.S)   # проценты редкости
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(re.sub(r"\s+", " ", s)).strip()
    return re.sub(r"\s*[\d.,]+%$", "", s).strip()


def parse(slug, n, raw):
    """Разбор страницы подарка. Возвращает dict или None, если подарка нет."""
    rows = {}
    for key, val in ROW_RE.findall(raw):
        k = key.lower()
        if k not in rows:
            rows[k] = val

    model = clean(rows["model"]) if "model" in rows else None
    backdrop = clean(rows["backdrop"]) if "backdrop" in rows else None
    owner = clean(rows["owner"]) if "owner" in rows else None

    # ссылка на владельца: берём только из блока Owner, не из «Gifted by»
    owner_block = rows.get("owner")
    if owner_block is None:
        m = re.search(r">\s*Owner\s*<.*?</tr>", raw, re.S | re.I)
        owner_block = m.group(0) if m else ""
    owner_url = None
    m = re.search(r'href="(https?://t\.me/[^"]+)"', owner_block)
    if m:
        owner_url = html.unescape(m.group(1))

    # выпущено улучшенных подарков в коллекции: «2 825/2 861 issued»
    issued = None
    if "quantity" in rows:
        m = re.match(r"\s*([\d\s\u00a0\u202f,.]+?)\s*/", clean(rows["quantity"]))
        if m:
            digits = re.sub(r"\D", "", m.group(1))
            issued = int(digits) if digits else None

    if not (model or backdrop):
        # запасной вариант 1: og:description («Model: X / Backdrop: Y / Symbol: Z»)
        mb = re.search(r"Backdrop:\s*([^&\n\"<]+)", raw)
        mm = re.search(r"Model:\s*([^&\n\"<]+)", raw)
        backdrop = mb.group(1).strip() if mb else None
        model = mm.group(1).strip() if mm else None

    if not (backdrop or model):
        return None
    return {"slug": slug, "n": n, "url": f"https://t.me/nft/{slug}-{n}",
            "model": model, "backdrop": backdrop,
            "owner": owner, "owner_url": owner_url, "issued": issued}


def fetch(slug, n):
    """Данные подарка или None, если такого номера ещё нет."""
    r = requests.get(f"https://t.me/nft/{slug}-{n}", headers=HEADERS, timeout=15)
    if r.status_code in (429, 500, 502, 503):
        raise Throttled(f"HTTP {r.status_code}")
    if r.status_code != 200:
        raise BadStatus(f"HTTP {r.status_code}")
    return parse(slug, n, r.text)


def find_head(slug):
    """Последний существующий номер, либо None, если коллекции нет.

    На странице подарка есть строка Quantity «выпущено/всего», по ней сразу
    понятно, где конец коллекции (3 запроса вместо ~35). Если не получилось,
    ищем обычным двоичным поиском.
    """
    first = fetch(slug, 1)
    if first is None:
        return None
    est = first.get("issued")
    if est and est >= 1 and fetch(slug, est) is not None:
        n = est
        while n < est + 50 and fetch(slug, n + 1) is not None:
            n += 1
        if n < est + 50:
            return n
    lo, hi = 1, 2
    while hi < 5_000_000 and fetch(slug, hi) is not None:
        lo, hi = hi, hi * 2
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if fetch(slug, mid) is not None:
            lo = mid
        else:
            hi = mid
    return lo


def load_slugs():
    try:
        with open(COLL_FILE, encoding="utf-8") as f:
            seen, out = set(), []
            for line in f:
                s = line.strip()
                if SLUG_RE.fullmatch(s) and s not in seen:
                    seen.add(s)
                    out.append(s)
            return out
    except FileNotFoundError:
        return []


def poll_one(slug):
    global counter, last_error, last_error_ts
    try:
        if slug not in nxt:
            if time.time() < dead.get(slug, 0):
                return
            state[slug] = "ищу последний номер…"
            head = find_head(slug)
            if head is None:
                dead[slug] = time.time() + RETRY_DEAD_AFTER
                state[slug] = "не найдена (или страница не разобралась)"
                return
            nxt[slug] = head + 1
            state[slug] = f"слежу, последний номер {head}"
            return
        for _ in range(15):
            g = fetch(slug, nxt[slug])
            if g is None:
                break
            with lock:
                counter += 1
                g["id"] = counter
                g["ts"] = int(time.time())
                events.append(g)
            nxt[slug] += 1
        state[slug] = f"слежу, жду номер {nxt[slug]}"
    except (Throttled, BadStatus) as e:
        last_error, last_error_ts = f"{slug}: {e}", time.time()
        state[slug] = f"ошибка: {e}"
        time.sleep(5)
    except requests.RequestException as e:
        last_error, last_error_ts = f"{slug}: {e.__class__.__name__}", time.time()
        state[slug] = f"ошибка: {e.__class__.__name__}"


def worker():
    with ThreadPoolExecutor(THREADS) as ex:
        while True:
            slugs = load_slugs()[:MAX_COLLECTIONS]
            list(ex.map(poll_one, slugs))
            time.sleep(POLL_INTERVAL)


_started = False


def start_worker():
    global _started
    if not _started:
        _started = True
        threading.Thread(target=worker, daemon=True).start()


@app.get("/")
def index():
    return send_from_directory(HERE, "index.html")


@app.get("/api/feed")
def feed():
    try:
        since = int(request.args.get("since", "0"))
    except ValueError:
        since = 0
    with lock:
        items = [e for e in events if e["id"] > since]
        last = counter
    err = last_error if time.time() - last_error_ts < 300 else ""
    return jsonify(events=items, last=last, tracking=len(nxt),
                   total=len(load_slugs()), error=err)


@app.get("/api/debug")
def debug():
    return jsonify(tracking=len(nxt), seen_total=counter,
                   last_error=last_error, collections=state)


@app.get("/api/test")
def test():
    slug = request.args.get("slug", "PlushPepe")
    n = request.args.get("n", "1")
    if not SLUG_RE.fullmatch(slug) or not n.isdigit():
        return jsonify(error="bad args"), 400
    try:
        r = requests.get(f"https://t.me/nft/{slug}-{n}", headers=HEADERS, timeout=15)
    except requests.RequestException as e:
        return jsonify(error=str(e))
    return jsonify(http=r.status_code, parsed=parse(slug, int(n), r.text),
                   html_head=r.text[:1500],
                   html_has_table="tgme_gift_table" in r.text)


@app.post("/api/collections")
def add_collection():
    slug = ((request.get_json(silent=True) or {}).get("slug") or "").strip()
    if not SLUG_RE.fullmatch(slug):
        return jsonify(error="bad slug"), 400
    cur = load_slugs()
    if slug not in cur and len(cur) < MAX_COLLECTIONS:
        with open(COLL_FILE, "a", encoding="utf-8") as f:
            f.write(slug + "\n")
        dead.pop(slug, None)
    return jsonify(ok=True)


start_worker()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
