"""
Gift Watcher: следит за новыми улучшенными подарками сразу во многих коллекциях.

Запуск локально:  pip install flask requests gunicorn && python server.py
Продакшен:        gunicorn server:app --workers 1 --threads 4
(workers именно 1: фоновый опрос и память с событиями живут в одном процессе)

Список коллекций лежит в collections.txt (по одной на строку).
Несуществующие названия сервер сам пропускает.
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
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) gift-watcher/2.0"}
SLUG_RE = re.compile(r"[A-Za-z0-9]{1,64}")

POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", 8))   # пауза между кругами, сек
THREADS = int(os.environ.get("THREADS", 6))                  # параллельных запросов к t.me
MAX_EVENTS = 1000
MAX_COLLECTIONS = 200
RETRY_DEAD_AFTER = 1800                                      # через сколько повторять «пустые» коллекции

app = Flask(__name__)
lock = threading.Lock()
events = deque(maxlen=MAX_EVENTS)
counter = 0
nxt = {}          # slug -> следующий номер, который надо проверить
dead = {}         # slug -> когда пробовать снова
last_error = ""


class Throttled(Exception):
    pass


def fetch(slug, n):
    """Данные подарка или None, если такого номера ещё нет."""
    r = requests.get(f"https://t.me/nft/{slug}-{n}", headers=HEADERS, timeout=15)
    if r.status_code in (429, 500, 502, 503):
        raise Throttled(f"HTTP {r.status_code}")
    if r.status_code != 200:
        return None
    text = re.sub(r"<[^>]+>", " ", r.text)
    text = html.unescape(re.sub(r"\s+", " ", text))

    def field(name, nxt_):
        m = re.search(rf"{name}\s+(.+?)\s+(?:{nxt_})", text)
        return m.group(1).strip() if m else None

    backdrop = field("Backdrop", "Symbol|Owner|Model|Quantity")
    model = field("Model", "Backdrop|Symbol|Owner|Quantity")
    owner = field("Owner", "Model|Backdrop|Symbol|Quantity")
    if not (backdrop or model):
        return None
    return {"slug": slug, "n": n, "url": f"https://t.me/nft/{slug}-{n}",
            "model": model, "backdrop": backdrop, "owner": owner}


def find_head(slug):
    """Последний существующий номер (быстрый поиск), либо None."""
    if fetch(slug, 1) is None:
        return None
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
    global counter, last_error
    try:
        if slug not in nxt:
            if time.time() < dead.get(slug, 0):
                return
            head = find_head(slug)
            if head is None:
                dead[slug] = time.time() + RETRY_DEAD_AFTER
                return
            nxt[slug] = head + 1          # историю не трогаем, ждём только новые
            return
        for _ in range(15):               # догоняем, если за раз появилось много
            g = fetch(slug, nxt[slug])
            if g is None:
                break
            with lock:
                counter += 1
                g["id"] = counter
                g["ts"] = int(time.time())
                events.append(g)
            nxt[slug] += 1
    except Throttled as e:
        last_error = f"{slug}: {e}"
        time.sleep(5)
    except requests.RequestException as e:
        last_error = f"{slug}: {e.__class__.__name__}"


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
    return jsonify(events=items, last=last, tracking=len(nxt),
                   total=len(load_slugs()), error=last_error)


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
