"""
Flashscore schedule feed diagnostic — prints raw feed + parsed field map
"""

import time, random, re, logging
from typing import Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

FS_NINJA_HOST  = "global.flashscore.ninja"
FS_FEED_BASE   = f"https://{FS_NINJA_HOST}/2/x/feed/"
X_FSIGN_TOKEN  = "SW9D1eZo"
WC_TOURNAMENT_ID = "lvUBR5F8"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

def make_session():
    s = requests.Session()
    s.headers.update({
        "Accept":          "text/plain, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Referer":         "https://www.flashscore.com/",
        "Origin":          "https://www.flashscore.com",
        "User-Agent":      random.choice(USER_AGENTS),
        "X-Fsign":         X_FSIGN_TOKEN,
    })
    return s

session = make_session()

def fs_get(query: str) -> Optional[str]:
    url = f"{FS_FEED_BASE}{query}"
    for attempt in range(4):
        try:
            time.sleep(random.uniform(2, 4))
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                return r.text
            if r.status_code == 403:
                wait = (2 ** attempt) * random.uniform(4, 8)
                logger.warning(f"403 — backing off {wait:.0f}s")
                time.sleep(wait)
            else:
                logger.warning(f"HTTP {r.status_code}")
                time.sleep(5)
        except Exception as e:
            logger.warning(f"Error: {e}")
            time.sleep(8)
    return None

def parse_rows(raw: str):
    rows = []
    for row in raw.split("~"):
        row = row.strip()
        if not row:
            continue
        f = {}
        for part in row.split("¬"):
            if "÷" in part:
                k, _, v = part.partition("÷")
                f[k.strip()] = v.strip()
        if f:
            rows.append(f)
    return rows

# ── Step 1: get season_id + stage_id ────────────────────────────────────────
logger.info("Fetching tournament header...")
raw_header = fs_get(f"t_1_8_{WC_TOURNAMENT_ID}_3_en_1")

if not raw_header:
    logger.error("No response — check connectivity / token")
    exit(1)

print("\n=== RAW HEADER (first 800 chars) ===")
print(raw_header[:800])
print()

season_id = stage_id = None
for f in parse_rows(raw_header):
    if "ZA" in f:
        season_id = f.get("ZC", "").strip()
        stage_id  = f.get("ZE", "").strip()
        print(f"Found: season_id={season_id}  stage_id={stage_id}")
        print(f"Full ZA row fields: {f}")
        break

if not season_id or not stage_id:
    logger.error("Could not extract season_id/stage_id")
    exit(1)

# ── Step 2: fetch schedule page 1 ───────────────────────────────────────────
endpoint = f"to_{stage_id}_{season_id}_1"
logger.info(f"Fetching schedule: {endpoint}")
raw_sched = fs_get(endpoint)

if not raw_sched:
    logger.error("No schedule response")
    exit(1)

print("\n=== RAW SCHEDULE (first 1500 chars) ===")
print(raw_sched[:1500])
print()

# ── Step 3: print ALL fields for the first 3 match rows ─────────────────────
rows = parse_rows(raw_sched)
match_rows = [r for r in rows if "LME" in r]

print(f"=== FOUND {len(match_rows)} MATCH ROWS ===\n")
for i, r in enumerate(match_rows[:3]):
    print(f"--- Match row {i+1} ---")
    for k, v in sorted(r.items()):
        print(f"  {k} = {v!r}")
    print()