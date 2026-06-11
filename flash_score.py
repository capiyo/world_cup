"""
World Cup 2026 — Flashscore Scraper + Live Poller
===================================================
Drop-in replacement for the Sofascore-based worldcup_poller.py.

Why Flashscore?
  • Internal feed at global.flashscore.com/x/feed/ uses a static X-Fsign
    token embedded in every Flashscore page — no session warm-up needed.
  • No TLS fingerprinting arms-race: standard requests + Chrome UA is enough.
  • Pipe-delimited text protocol (not JSON) is trivially parsed and very stable.
  • No per-day endpoint hammering: one call fetches all fixtures for a date
    offset, another fetches a tournament's full fixture list.

Flashscore feed anatomy
  • Base:      https://global.flashscore.com/x/feed/
  • Fixtures:  ?_=<timestamp>&q=<sport>/<country>/<tournament>/fixtures/
  • Scores:    ?_=<timestamp>&q=<sport>/<country>/<tournament>/results/
  • Match:     ?_=<timestamp>&q=match/<match_id>/
  • Incidents: ?_=<timestamp>&q=match/<match_id>/incidents/
  • Lineups:   ?_=<timestamp>&q=match/<match_id>/lineups/
  • Stats:     ?_=<timestamp>&q=match/<match_id>/statistics/
  • Daily:     ?_=<timestamp>&q=<sport>/<YYYY-MM-DD>/

Response format — pipe-delimited rows, each row like:
    SA÷<id>÷<home>÷<away>÷<timestamp>÷...÷
Fields vary by endpoint; we parse by key prefix.

World Cup 2026 Flashscore path:  football/world/world-cup

Install:  pip install requests pymongo python-dotenv
Run:      python worldcup_poller_flashscore.py
"""

import time
import hashlib
import random
import logging
import os
import re
import threading
import queue as _queue
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

import requests as std_requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

WORLD_CUP_LABEL        = "World Cup 2026"
# Flashscore tournament path — football / country-slug / tournament-slug
WC_SPORT               = "football"
WC_COUNTRY             = "world"
WC_TOURNAMENT          = "world-cup"
WC_PATH                = f"{WC_SPORT}/{WC_COUNTRY}/{WC_TOURNAMENT}"

MATCH_DURATION_MINS    = 120
DATABASE_URL           = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME                = "clashdb"
COLLECTION_NAME        = "fixturesf"

NAIROBI_OFFSET         = timedelta(hours=3)

# ── Flashscore feed ─────────────────────────────────────────────────────────
FS_FEED_BASE           = "https://global.flashscore.com/x/feed/"
FS_HOME                = "https://www.flashscore.com"

# This token is hardcoded in the Flashscore frontend JS bundle.
# It authenticates the feed endpoint and rotates infrequently (months).
# If you get consistent 403s replace it by loading flashscore.com in a browser,
# opening DevTools → Network → filter "x/feed" and copy the X-Fsign header.
X_FSIGN_TOKEN          = "SW9D1eZo"

FANCLASH_API           = os.environ.get("FANCLASH_API")
DEFAULT_ODDS           = {"home_win": 2.50, "away_win": 2.80, "draw": 3.20}

POLL_INTERVAL_SEC        = 45
LINEUP_POLL_INTERVAL_SEC = 30
HOUR_CHECK_INTERVAL_SEC  = 3600
SCRAPE_INTERVAL_SEC      = 3600 * 6
LIVE_CHECK_INTERVAL_SEC  = 60
CLEANUP_INTERVAL_SEC     = 300

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL POLL TRACKING
# ─────────────────────────────────────────────────────────────────────────────

active_polls: set  = set()
polls_lock         = threading.Lock()
SOFASCORE_SEMAPHORE = threading.Semaphore(1)   # kept for API compat; guards FS too

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HEALTH SERVER
# ─────────────────────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/wakeup":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "waking up worldcup poller (flashscore)"}')
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"FanClash WorldCup Flashscore Poller OK")

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()

    def log_message(self, *args):
        pass


def start_health_server():
    port = int(os.environ.get("PORT", 8081))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"🌐 Health server on port {port}")


# ─────────────────────────────────────────────────────────────────────────────
# FLASHSCORE FEED CLIENT
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

_session: Optional[std_requests.Session] = None
_session_lock = threading.Lock()


def _make_session() -> std_requests.Session:
    s = std_requests.Session()
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


def get_session() -> std_requests.Session:
    global _session
    with _session_lock:
        if _session is None:
            _session = _make_session()
        return _session


def reset_session():
    global _session
    with _session_lock:
        _session = _make_session()
    logger.info("   🔄 Flashscore session reset")


def fs_get(
    query: str,
    retries: int = 5,
    base_delay: float = 2.0,
) -> Optional[str]:
    """
    Hit the Flashscore feed endpoint.
    Returns the raw pipe-delimited text or None on terminal failure.

    Back-off strategy:
      • 429  → wait 30 × (attempt+1) seconds, then retry
      • 403  → exponential back-off 2^attempt × 4-8s; reset session once
      • other → 5 s flat retry
    Requests are serialised through SOFASCORE_SEMAPHORE so only ONE
    feed call runs at a time across all threads.
    """
    ts = int(time.time() * 1000)
    url = f"{FS_FEED_BASE}?_={ts}&q={query}"
    session_reset_done = False

    for attempt in range(retries):
        try:
            with SOFASCORE_SEMAPHORE:
                time.sleep(random.uniform(base_delay, base_delay + 2.0))
                resp = get_session().get(url, timeout=20)

            if resp.status_code == 200:
                return resp.text

            if resp.status_code == 404:
                logger.debug(f"   FS 404 for {query}")
                return None

            if resp.status_code == 403:
                wait = (2 ** attempt) * random.uniform(4, 8)
                logger.warning(
                    f"   FS 403 attempt {attempt+1} — backing off {wait:.0f}s"
                )
                time.sleep(wait)
                if not session_reset_done:
                    reset_session()
                    session_reset_done = True
                continue

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"   FS 429 — waiting {wait}s")
                time.sleep(wait)
                continue

            logger.warning(f"   FS HTTP {resp.status_code} attempt {attempt+1} for {query}")
            time.sleep(5)

        except Exception as e:
            logger.warning(f"   FS request error attempt {attempt+1}: {e}")
            time.sleep(8)

    logger.error(f"   FS all retries exhausted for {query}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# FLASHSCORE FEED PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _split_row(row: str) -> List[str]:
    """Split a pipe-delimited FS row into fields, stripping empties at end."""
    return row.rstrip("÷").split("÷")


def parse_fixtures_feed(raw: str, upcoming_only: bool = True) -> List[Dict]:
    """
    Parse Flashscore fixture/results feed text into structured dicts.

    Feed rows for a match start with 'SA÷' and contain:
        SA÷<match_id>÷<home_id>÷<away_id>÷<home_name>÷<away_name>÷
           <start_ts>÷<status_code>÷<home_score>÷<away_score>÷...

    Status codes (numeric):
        0-5   → scheduled / upcoming
        6     → 1st half
        7     → half time
        8     → 2nd half
        9     → extra time
        10    → penalty shootout
        100   → finished
        110+  → finished (AET / AP)

    Other row types we care about:
        ZA÷  → tournament header (ignored — we already know it's WC)
        RO÷  → round separator  (can be used for round tagging)
    """
    docs: List[Dict] = []
    if not raw:
        return docs

    current_round = ""

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith("RO÷"):
            parts = _split_row(line[3:])
            current_round = parts[0] if parts else ""
            continue

        if not line.startswith("SA÷"):
            continue

        parts = _split_row(line[3:])
        # Minimum expected fields
        if len(parts) < 8:
            continue

        try:
            match_id   = parts[0]
            home_name  = _clean_name(parts[3]) if len(parts) > 3 else ""
            away_name  = _clean_name(parts[4]) if len(parts) > 4 else ""
            start_ts   = int(parts[6]) if parts[6].isdigit() else 0
            status_raw = int(parts[7]) if len(parts) > 7 and parts[7].isdigit() else 0
        except (ValueError, IndexError):
            continue

        if not home_name or not away_name or not match_id:
            continue

        home_score: Optional[int] = None
        away_score: Optional[int] = None
        try:
            if len(parts) > 8 and parts[8] not in ("", "-"):
                home_score = int(parts[8])
            if len(parts) > 9 and parts[9] not in ("", "-"):
                away_score = int(parts[9])
        except ValueError:
            pass

        status = _map_status(status_raw)

        if upcoming_only and status != "upcoming":
            continue

        date_iso = date_display = time_eat = ""
        if start_ts:
            dt = datetime.fromtimestamp(start_ts, tz=timezone.utc) + NAIROBI_OFFSET
            date_iso     = dt.strftime("%Y-%m-%d")
            date_display = dt.strftime("%d %b")
            time_eat     = dt.strftime("%H:%M")
        else:
            now          = datetime.now(timezone.utc)
            date_iso     = now.strftime("%Y-%m-%d")
            date_display = now.strftime("%d %b")
            time_eat     = "TBD"

        doc = {
            "_id":                  match_id,
            "match_id":             match_id,
            "flashscore_id":        match_id,
            "home_team":            home_name,
            "away_team":            away_name,
            "league":               WORLD_CUP_LABEL,
            "round":                current_round,
            "home_win":             float(DEFAULT_ODDS["home_win"]),
            "away_win":             float(DEFAULT_ODDS["away_win"]),
            "draw":                 float(DEFAULT_ODDS["draw"]),
            "date":                 date_display,
            "time":                 time_eat,
            "date_iso":             date_iso,
            "home_score":           home_score,
            "away_score":           away_score,
            "status":               status,
            "is_live":              status == "live",
            "available_for_voting": status == "upcoming",
            "time_elapsed":         0,
            "source":               "flashscore",
            "scraped_at":           datetime.now(timezone.utc),
            "votes":                0,
            "comments":             0,
            "voters":               [],
            "commentary":           [],
            "commentary_count":     0,
            "last_commentary_at":   None,
        }
        docs.append(doc)

    return docs


def parse_match_feed(raw: str) -> Optional[Dict]:
    """
    Parse a single-match feed response.
    Returns a dict with live state fields.
    """
    if not raw:
        return None

    result = {
        "home_score":   0,
        "away_score":   0,
        "status_raw":   0,
        "status":       "upcoming",
        "time_elapsed": 0,
        "time_extra":   0,
    }

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith("SA÷"):
            parts = _split_row(line[3:])
            try:
                result["status_raw"] = int(parts[7]) if len(parts) > 7 and parts[7].isdigit() else 0
                result["status"]     = _map_status(result["status_raw"])
                if len(parts) > 8 and parts[8] not in ("", "-"):
                    result["home_score"] = int(parts[8])
                if len(parts) > 9 and parts[9] not in ("", "-"):
                    result["away_score"] = int(parts[9])
            except (ValueError, IndexError):
                pass

        # Match time line: MT÷<elapsed>÷<extra>÷
        if line.startswith("MT÷"):
            parts = _split_row(line[3:])
            try:
                result["time_elapsed"] = int(parts[0]) if parts else 0
                result["time_extra"]   = int(parts[1]) if len(parts) > 1 else 0
            except ValueError:
                pass

    return result


def parse_incidents_feed(raw: str) -> List[Dict]:
    """
    Parse incident feed.  Row format:
        INC÷<id>÷<type>÷<minute>÷<extra>÷<is_home>÷<player>÷[<assist>]÷...
    type codes: G=goal, YC=yellow, RC=red, SB=substitution, MS=missed penalty
    """
    incidents: List[Dict] = []
    if not raw:
        return incidents

    for line in raw.splitlines():
        line = line.strip()
        if not line or not line.startswith("INC÷"):
            continue
        parts = _split_row(line[4:])
        if len(parts) < 6:
            continue
        try:
            inc = {
                "id":      parts[0],
                "type":    parts[1].upper(),   # G / YC / RC / SB / MS / CO
                "minute":  int(parts[2]) if parts[2].isdigit() else 0,
                "extra":   int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0,
                "is_home": parts[4] == "1",
                "player":  _clean_name(parts[5]) if len(parts) > 5 else "Unknown",
                "assist":  _clean_name(parts[6]) if len(parts) > 6 and parts[6] else None,
                "sub_out": _clean_name(parts[7]) if len(parts) > 7 and parts[7] else None,
            }
            incidents.append(inc)
        except (ValueError, IndexError):
            continue

    return incidents


def parse_lineups_feed(raw: str) -> Optional[Dict]:
    """
    Parse lineups feed.  Row prefixes:
        LU÷ → lineup header (formation)
        PL÷ → player row:  PL÷<id>÷<name>÷<jersey>÷<pos>÷<is_home>÷<is_starter>÷[captain]
        CO÷ → coach row:   CO÷<id>÷<name>÷<is_home>
    """
    if not raw:
        return None

    lineups: Dict = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith("LU÷"):
            parts = _split_row(line[3:])
            # LU÷<formation_home>÷<formation_away>÷
            if len(parts) >= 1:
                lineups["home"]["formation"] = parts[0] or "4-4-2"
            if len(parts) >= 2:
                lineups["away"]["formation"] = parts[1] or "4-4-2"
            continue

        if line.startswith("PL÷"):
            parts = _split_row(line[3:])
            if len(parts) < 6:
                continue
            try:
                jersey = int(parts[2]) if parts[2].isdigit() else 0
                side   = "home" if parts[4] == "1" else "away"
                is_starter = parts[5] == "1"
                player = {
                    "name":         _clean_name(parts[1]),
                    "position":     parts[3] or "Unknown",
                    "jerseyNumber": jersey,
                    "captain":      len(parts) > 7 and parts[7] == "1",
                    "lineup":       is_starter,
                }
                bucket = "players" if is_starter else "bench"
                lineups[side][bucket].append(player)
            except (ValueError, IndexError):
                continue
            continue

        if line.startswith("CO÷"):
            parts = _split_row(line[3:])
            if len(parts) >= 3:
                side = "home" if parts[2] == "1" else "away"
                lineups[side]["coach"]["name"] = _clean_name(parts[1]) or "Unknown"

    return lineups


def parse_statistics_feed(raw: str, time_elapsed: int, time_extra: int,
                           home_score: int, away_score: int) -> Optional[Dict]:
    """
    Parse statistics feed.  Rows like:
        ST÷<stat_key>÷<home_val>÷<away_val>÷
    """
    if not raw:
        return None

    stats: Dict[str, Any] = {}

    KEY_MAP = {
        "possession_home":    ("ball_possession_home",  "ball_possession_away"),
        "shots_total":        ("total_shots_home",       "total_shots_away"),
        "shots_on_target":    ("shots_on_target_home",   "shots_on_target_away"),
        "corner_kicks":       ("corners_home",           "corners_away"),
        "fouls":              ("fouls_home",              "fouls_away"),
        "offsides":           ("offsides_home",           "offsides_away"),
        "yellow_cards":       ("yellow_cards_home",       "yellow_cards_away"),
        "red_cards":          ("red_cards_home",          "red_cards_away"),
        "pass_accuracy":      ("pass_accuracy_home",      "pass_accuracy_away"),
    }

    for line in raw.splitlines():
        line = line.strip()
        if not line or not line.startswith("ST÷"):
            continue
        parts = _split_row(line[3:])
        if len(parts) < 3:
            continue
        key, hv, av = parts[0].lower(), parts[1], parts[2]
        mapped = KEY_MAP.get(key)
        if mapped:
            try:
                stats[mapped[0]] = int(str(hv).replace("%", "").strip())
                stats[mapped[1]] = int(str(av).replace("%", "").strip())
            except ValueError:
                pass

    minute_disp = f"{time_elapsed}" + (f"+{time_extra}" if time_extra else "")
    stats.update({
        "minute":         time_elapsed,
        "minute_display": minute_disp,
        "home_score":     home_score,
        "away_score":     away_score,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    })
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

_STRIP_TAGS = re.compile(r"<[^>]+>")

def _clean_name(s: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    return " ".join(_STRIP_TAGS.sub("", s).split())


def _map_status(code: int) -> str:
    if code in (100, 110, 111, 120, 121):
        return "completed"
    if code in (6, 7, 8, 9, 10, 41, 42):
        return "live"
    return "upcoming"


def eat_from_timestamp(ts: int) -> Tuple[str, str, str]:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + NAIROBI_OFFSET
    return dt.strftime("%Y-%m-%d"), dt.strftime("%d %b"), dt.strftime("%H:%M")


def is_match_over(date_iso: str, time_str: str) -> bool:
    if not date_iso or not time_str or time_str == "TBD":
        return False
    try:
        naive = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
        kickoff_utc = (naive - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        return (kickoff_utc + timedelta(minutes=MATCH_DURATION_MINS)) < datetime.now(timezone.utc)
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def run_scraper(col) -> List[Dict]:
    logger.info("\n" + "=" * 65)
    logger.info("📡 RUNNING WORLD CUP 2026 FLASHSCORE SCRAPER")
    logger.info("=" * 65)

    docs: List[Dict] = []
    seen: set        = set()

    # ── Strategy 1: dedicated tournament fixture page ────────────────────────
    logger.info(f"   Fetching {WC_PATH}/fixtures/")
    raw = fs_get(f"{WC_PATH}/fixtures/", base_delay=2.0)
    if raw:
        found = parse_fixtures_feed(raw, upcoming_only=True)
        for d in found:
            if d["_id"] not in seen and not is_match_over(d["date_iso"], d["time"]):
                seen.add(d["_id"])
                docs.append(d)
        logger.info(f"   Fixtures page → {len(found)} matches")
    else:
        logger.warning("   Fixtures page returned nothing")

    # ── Strategy 2: day-by-day scan as fallback ──────────────────────────────
    if not docs:
        logger.info("   Falling back to day-by-day World Cup scan...")
        today  = datetime.now(timezone.utc).date()
        cutoff = today + timedelta(days=60)
        day    = today
        consecutive_empty = 0
        consecutive_block = 0

        while day <= cutoff:
            if consecutive_empty >= 7 and len(docs) > 0:
                logger.info("   7 empty days in a row — stopping scan")
                break
            if consecutive_block >= 3:
                wait = random.uniform(90, 180)
                logger.warning(f"   3 blocked days — pausing {wait:.0f}s")
                time.sleep(wait)
                reset_session()
                consecutive_block = 0

            day_str = day.strftime("%Y-%m-%d")
            raw_day = fs_get(f"{WC_SPORT}/{day_str}/", base_delay=4.0)

            if raw_day is None:
                consecutive_block += 1
                consecutive_empty += 1
            else:
                consecutive_block = 0
                day_docs = []
                for d in parse_fixtures_feed(raw_day, upcoming_only=True):
                    if d["_id"] not in seen and not is_match_over(d["date_iso"], d["time"]):
                        seen.add(d["_id"])
                        day_docs.append(d)
                if day_docs:
                    consecutive_empty = 0
                    docs.extend(day_docs)
                    logger.info(f"   {day_str} → {len(day_docs)} World Cup matches")
                else:
                    consecutive_empty += 1

            day += timedelta(days=1)
            time.sleep(random.uniform(5.0, 9.0))

    if docs and col is not None:
        inserted = 0
        for d in docs:
            try:
                col.update_one({"_id": d["_id"]}, {"$set": d}, upsert=True)
                inserted += 1
            except Exception:
                pass
        logger.info(f"   💾 Saved {inserted} World Cup fixtures")

    if not docs:
        wait = random.uniform(60, 120)
        logger.warning(f"   ⚠️  No fixtures found — backing off {wait:.0f}s")
        time.sleep(wait)

    logger.info(f"\n📊 Scraper done: {len(docs)} World Cup fixtures")
    return docs


# ─────────────────────────────────────────────────────────────────────────────
# DB HELPERS  (identical interface to Sofascore version)
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    try:
        client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=15000)
        client.admin.command("ping")
        col = client[DB_NAME][COLLECTION_NAME]
        col.create_index("match_id",      unique=True)
        col.create_index("flashscore_id")
        col.create_index("status")
        col.create_index("league")
        col.create_index("date_iso")
        logger.info(f"✅ Connected to {DB_NAME}.{COLLECTION_NAME}")
        return client, col
    except Exception as e:
        logger.warning(f"⚠️ MongoDB failed: {e}")
        return None, None


def get_history_collection(client):
    if client is None:
        return None
    hcol = client[DB_NAME]["fixtures_history"]
    hcol.create_index("completed_at")
    hcol.create_index("match_id")
    hcol.create_index("status")
    return hcol


def move_completed_game_to_history(col, history_col, match_id: str) -> bool:
    if col is None or history_col is None:
        return False
    try:
        game = col.find_one({"match_id": match_id, "status": "completed"})
        if not game or game.get("moved_to_history"):
            return False
        game["completed_at"]     = datetime.now(timezone.utc)
        game["moved_to_history"] = True
        history_col.update_one({"match_id": match_id}, {"$set": game}, upsert=True)
        col.delete_one({"match_id": match_id})
        logger.info(f"📦 Moved {match_id} ({game['home_team']} vs {game['away_team']}) to history")
        return True
    except Exception as e:
        logger.error(f"Failed to move {match_id} to history: {e}")
        return False


def cleanup_all_completed_games(col, history_col):
    if col is None or history_col is None:
        return
    try:
        moved = 0
        for game in col.find({"status": "completed", "league": WORLD_CUP_LABEL}):
            mid = game.get("match_id")
            if mid and not game.get("moved_to_history"):
                if move_completed_game_to_history(col, history_col, mid):
                    moved += 1
        logger.info(f"🧹 Cleaned up {moved} completed World Cup games to history")
    except Exception as e:
        logger.error(f"Error cleaning up: {e}")


def load_fixtures_from_db(col) -> List[Dict[str, Any]]:
    if col is None:
        return []
    fixtures = []
    query = {"status": {"$ne": "completed"}, "league": WORLD_CUP_LABEL}
    for f in col.find(query):
        date_iso = f.get("date_iso", "")
        time_str = f.get("time", "00:00")
        kickoff_utc = None
        try:
            naive_eat   = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
            kickoff_utc = (naive_eat - NAIROBI_OFFSET).replace(tzinfo=timezone.utc)
        except Exception:
            pass
        fixtures.append({
            "match_id":          f.get("match_id"),
            "flashscore_id":     f.get("flashscore_id"),
            "home_team":         f.get("home_team"),
            "away_team":         f.get("away_team"),
            "home_score":        f.get("home_score", 0),
            "away_score":        f.get("away_score", 0),
            "status":            f.get("status", "upcoming"),
            "is_live":           f.get("is_live", False),
            "date_iso":          date_iso,
            "time":              time_str,
            "_kickoff_utc":      kickoff_utc,
            "_lineups_fetched":  f.get("lineups_fetched", False),
        })
    fixtures.sort(key=lambda x: x["_kickoff_utc"] or datetime.max.replace(tzinfo=timezone.utc))
    return fixtures


def mark_lineups_fetched(col, match_id: str):
    if col is None:
        return
    try:
        col.update_one({"match_id": match_id}, {"$set": {"lineups_fetched": True}})
    except Exception as e:
        logger.warning(f"Could not mark lineups_fetched: {e}")


def update_db_status(col, match_id: str, status: str, extra_fields: dict = None):
    if col is None:
        return
    fields = {
        "status":               status,
        "is_live":              status == "live",
        "available_for_voting": status in ("upcoming", "soon"),
    }
    if extra_fields:
        fields.update(extra_fields)
    try:
        col.update_one({"match_id": match_id}, {"$set": fields})
        logger.info(f"🗄️  DB status → '{status}' for {match_id}")
    except Exception as e:
        logger.warning(f"update_db_status error: {e}")


def get_live_fixtures(fixtures: List[Dict]) -> List[Dict]:
    now_utc = datetime.now(timezone.utc)
    live = []
    for f in fixtures:
        if f.get("status") == "live":
            live.append(f)
        else:
            ko = f.get("_kickoff_utc")
            if ko and (now_utc >= ko) and f.get("status") != "completed":
                live.append(f)
    return live


# ─────────────────────────────────────────────────────────────────────────────
# BACKEND API CALLS  (identical interface to Sofascore version)
# ─────────────────────────────────────────────────────────────────────────────

def update_fixture_status(match_id: str, status: str):
    if status == "finished":
        status = "completed"
    is_live = status == "live"
    try:
        r = std_requests.put(
            f"{FANCLASH_API}/games/{match_id}/status",
            json={"match_id": match_id, "status": status, "is_live": is_live},
            timeout=5,
        )
        if r.status_code == 200:
            logger.info(f"✅ Status updated → '{status}'")
        else:
            logger.warning(f"❌ Status update failed: {r.status_code}")
    except Exception as e:
        logger.error(f"update_fixture_status error: {e}")


def check_lineups_exist_in_backend(match_id: str) -> bool:
    try:
        r = std_requests.get(f"{FANCLASH_API}/games/{match_id}/lineups", timeout=5)
        if r.status_code == 200:
            data = r.json()
            hp = data.get("lineups", {}).get("home", {}).get("players", [])
            ap = data.get("lineups", {}).get("away", {}).get("players", [])
            return bool(hp or ap)
        return False
    except Exception:
        return False


def forward_event(fixture: dict, event_type: str, data: dict):
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    payload = {
        "fixture_id":     fixture["match_id"],
        "event_type":     event_type,
        "minute":         data.get("minute", 0),
        "minute_display": data.get("minute_display", f"{data.get('minute', 0)}'"),
        "home_score":     data.get("home_score", 0),
        "away_score":     data.get("away_score", 0),
        "timestamp":      {"$date": ts_ms},
        "player":         data.get("player"),
        "assist":         data.get("assist"),
        "team":           data.get("team"),
        "player_out":     data.get("player_out"),
        "player_in":      data.get("player_in"),
        "on_target":      data.get("on_target"),
        "blocked":        data.get("blocked"),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    try:
        r = std_requests.post(f"{FANCLASH_API}/games/live-update", json=payload, timeout=5)
        if r.status_code == 200:
            logger.debug(f"✅ Forwarded {event_type}")
        else:
            logger.warning(f"❌ Failed {event_type}: {r.status_code}")
    except Exception as e:
        logger.error(f"forward_event error: {e}")


def send_commentary(fixture: dict, commentary_data: dict):
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        entry = {
            "minute":         commentary_data.get("minute", 0),
            "minute_display": commentary_data.get("minute_display", ""),
            "text":           commentary_data.get("text", ""),
            "event_type":     commentary_data.get("event_type", ""),
            "home_score":     commentary_data.get("home_score", 0),
            "away_score":     commentary_data.get("away_score", 0),
            "team":           commentary_data.get("team"),
            "player":         commentary_data.get("player"),
            "created_at":     {"$date": ts_ms},
        }
        entry   = {k: v for k, v in entry.items() if v is not None}
        payload = {"match_id": fixture["match_id"], "entry": entry}
        r = std_requests.post(f"{FANCLASH_API}/games/commentary", json=payload, timeout=3)
        if r.status_code == 200:
            logger.debug("📝 Commentary sent")
        else:
            logger.warning(f"❌ Commentary failed: {r.status_code}")
    except Exception as e:
        logger.warning(f"⚠️ Commentary error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# LINEUP FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_forward_lineups(fixture: Dict, col) -> bool:
    fs_id    = fixture.get("flashscore_id") or fixture.get("match_id")
    match_id = fixture.get("match_id")
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"

    if not fs_id:
        logger.warning(f"⚠️  No flashscore_id for {label}")
        return False

    logger.info(f"📋 Fetching lineups for {label}")
    raw = fs_get(f"match/{fs_id}/lineups/", base_delay=3.0)
    lineups = parse_lineups_feed(raw)

    if not lineups:
        logger.info(f"   ⏳ Lineups not yet available for {label}")
        return False

    hp = lineups["home"]["players"]
    ap = lineups["away"]["players"]
    if not hp and not ap:
        logger.info(f"   ⏳ Lineups empty for {label}")
        return False

    payload = {
        "fixture_id": match_id,
        "lineups":    lineups,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }
    try:
        r = std_requests.post(f"{FANCLASH_API}/games/lineups", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Lineups stored for {label}")
            mark_lineups_fetched(col, match_id)
            return True
        else:
            logger.warning(f"❌ Backend rejected lineups: {r.status_code}")
            return False
    except Exception as e:
        logger.error(f"fetch_and_forward_lineups error: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# STATISTICS FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_forward_statistics(fixture: dict, live_data: dict):
    fs_id    = fixture.get("flashscore_id") or fixture.get("match_id")
    match_id = fixture["match_id"]
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"

    if not fs_id:
        return

    raw = fs_get(f"match/{fs_id}/statistics/", base_delay=2.0)
    if not raw:
        logger.warning(f"   No statistics for {label}")
        return

    payload = parse_statistics_feed(
        raw,
        time_elapsed=live_data.get("time_elapsed", 0),
        time_extra=live_data.get("time_extra", 0),
        home_score=live_data.get("home_score", 0),
        away_score=live_data.get("away_score", 0),
    )
    if not payload:
        return

    payload["match_id"] = match_id
    minute_disp = payload.get("minute_display", "")

    try:
        r = std_requests.post(f"{FANCLASH_API}/games/statistics", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"📊 Statistics forwarded for {label} ({minute_disp}')")
        else:
            logger.warning(f"❌ Statistics failed: {r.status_code}")
    except Exception as e:
        logger.error(f"fetch_and_forward_statistics error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# LIVE DATA FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_live_data(fs_id: str) -> Tuple[Optional[Dict], List[Dict]]:
    """
    Returns (live_state_dict, incidents_list).
    live_state_dict keys: home_score, away_score, status_raw, status,
                          time_elapsed, time_extra
    """
    raw_match = fs_get(f"match/{fs_id}/", base_delay=3.0)
    state     = parse_match_feed(raw_match) if raw_match else None

    incidents: List[Dict] = []
    raw_inc = fs_get(f"match/{fs_id}/incidents/", base_delay=2.0)
    if raw_inc:
        incidents = parse_incidents_feed(raw_inc)

    return state, incidents


# ─────────────────────────────────────────────────────────────────────────────
# LIVE POLLER
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_game(fixture: dict, col, history_col):
    fs_id    = fixture.get("flashscore_id") or fixture.get("match_id")
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"
    match_id = fixture["match_id"]

    if not fs_id:
        logger.error(f"❌ Cannot poll {label}: no flashscore_id")
        return

    # Check if already finished
    initial, _ = fetch_live_data(fs_id)
    if initial and initial["status"] == "completed":
        logger.info(f"⏭  {label} already completed")
        update_fixture_status(match_id, "completed")
        update_db_status(col, match_id, "completed")
        move_completed_game_to_history(col, history_col, match_id)
        return

    update_fixture_status(match_id, "live")
    update_db_status(col, match_id, "live")
    logger.info(f"🔴 LIVE POLLING: {label}")

    last_home        = 0
    last_away        = 0
    half_time_sent   = False
    full_time_sent   = False
    second_half_sent = False
    seen_incidents: set = set()
    poll_count       = 0

    while True:
        live, incidents = fetch_live_data(fs_id)
        if not live:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        home_score   = live["home_score"]
        away_score   = live["away_score"]
        status       = live["status"]
        status_raw   = live["status_raw"]
        time_elapsed = live["time_elapsed"]
        time_extra   = live.get("time_extra", 0)
        minute_disp  = f"{time_elapsed}" + (f"+{time_extra}" if time_extra else "")

        # ── Goals ─────────────────────────────────────────────────────────
        if home_score > last_home:
            scorer, assist = _find_goal_from_incidents(incidents, is_home=True)
            logger.info(f"⚽ GOAL! {fixture['home_team']} — {scorer} ({minute_disp}')")
            forward_event(fixture, "goal", {
                "minute": time_elapsed, "minute_display": minute_disp,
                "home_score": home_score, "away_score": away_score,
                "team": fixture["home_team"], "player": scorer, "assist": assist,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": minute_disp,
                "text": f"⚽ GOAL! {scorer} scores! ({home_score}-{away_score})",
                "event_type": "goal",
                "home_score": home_score, "away_score": away_score,
                "team": fixture["home_team"], "player": scorer,
            })
            last_home = home_score

        if away_score > last_away:
            scorer, assist = _find_goal_from_incidents(incidents, is_home=False)
            logger.info(f"⚽ GOAL! {fixture['away_team']} — {scorer} ({minute_disp}')")
            forward_event(fixture, "goal", {
                "minute": time_elapsed, "minute_display": minute_disp,
                "home_score": home_score, "away_score": away_score,
                "team": fixture["away_team"], "player": scorer, "assist": assist,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": minute_disp,
                "text": f"⚽ GOAL! {scorer} scores! ({home_score}-{away_score})",
                "event_type": "goal",
                "home_score": home_score, "away_score": away_score,
                "team": fixture["away_team"], "player": scorer,
            })
            last_away = away_score

        # ── Other incidents ────────────────────────────────────────────────
        for inc in incidents:
            inc_id = inc.get("id", "")
            if inc_id in seen_incidents:
                continue
            seen_incidents.add(inc_id)

            inc_type = inc["type"]
            is_home  = inc["is_home"]
            team     = fixture["home_team"] if is_home else fixture["away_team"]
            minute   = inc["minute"]
            extra    = inc.get("extra", 0)
            m_disp   = f"{minute}" + (f"+{extra}" if extra else "")
            player   = inc.get("player", "Unknown")

            commentary_text       = ""
            commentary_event_type = inc_type.lower()

            if inc_type == "G":
                continue  # handled above

            elif inc_type in ("YC",):
                icon = "🟨"
                commentary_text = f"{icon} YELLOW CARD - {player} ({team})"
                logger.info(f"{icon} YELLOW CARD — {team}: {player} ({m_disp}')")
                forward_event(fixture, "yellow_card", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })
                commentary_event_type = "yellow_card"

            elif inc_type in ("RC",):
                icon = "🟥"
                commentary_text = f"{icon} RED CARD - {player} ({team})"
                logger.info(f"{icon} RED CARD — {team}: {player} ({m_disp}')")
                forward_event(fixture, "red_card", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })
                commentary_event_type = "red_card"

            elif inc_type == "SB":
                p_out = inc.get("sub_out", "Unknown")
                p_in  = player
                commentary_text = f"🔄 SUBSTITUTION: {p_out} → {p_in} ({team})"
                logger.info(f"🔄 SUB — {team}: {p_out} → {p_in} ({m_disp}')")
                forward_event(fixture, "substitution", {
                    "minute": minute, "minute_display": m_disp,
                    "player_out": p_out, "player_in": p_in, "team": team,
                })
                commentary_event_type = "substitution"

            elif inc_type == "MS":
                commentary_text = f"❌ MISSED PENALTY - {player} ({team})"
                logger.info(f"❌ MISSED PEN — {team}: {player} ({m_disp}')")
                forward_event(fixture, "missed_penalty", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })
                commentary_event_type = "missed_penalty"

            elif inc_type == "PEN":
                commentary_text = f"🎯 PENALTY! {player} ({team})"
                logger.info(f"🎯 PENALTY — {team}: {player} ({m_disp}')")
                forward_event(fixture, "penalty", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })
                commentary_event_type = "penalty"

            if commentary_text:
                send_commentary(fixture, {
                    "minute":         minute,
                    "minute_display": m_disp,
                    "text":           commentary_text,
                    "event_type":     commentary_event_type,
                    "home_score":     home_score,
                    "away_score":     away_score,
                    "team":           team,
                    "player":         player if inc_type != "SB" else None,
                })

        # ── Match phase events ─────────────────────────────────────────────
        # status_raw 7 = half time pause
        if status_raw == 7 and not half_time_sent:
            logger.info(f"⏸  HALF TIME: {home_score}–{away_score}")
            forward_event(fixture, "half_time", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "home_score": home_score, "away_score": away_score,
            })
            update_db_status(col, match_id, "live", {"time_elapsed": time_elapsed, "half": 1})
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "text": (f"⏸ HALF TIME: {fixture['home_team']} "
                         f"{home_score}–{away_score} {fixture['away_team']}"),
                "event_type": "half_time",
                "home_score": home_score, "away_score": away_score,
            })
            fetch_and_forward_statistics(fixture, live)
            half_time_sent = True

        # status_raw 8 = second half
        if status_raw == 8 and half_time_sent and not second_half_sent:
            logger.info("▶️  SECOND HALF STARTED")
            forward_event(fixture, "second_half", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
            })
            update_db_status(col, match_id, "live", {"half": 2})
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "text": (f"▶️ SECOND HALF UNDERWAY! {fixture['home_team']} "
                         f"{home_score}–{away_score} {fixture['away_team']}"),
                "event_type": "second_half",
                "home_score": home_score, "away_score": away_score,
            })
            second_half_sent = True

        if status == "completed" and not full_time_sent:
            logger.info(f"🏁 FULL TIME: {label} — {home_score}–{away_score}")
            forward_event(fixture, "match_end", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "home_score": home_score, "away_score": away_score,
            })
            update_fixture_status(match_id, "completed")
            update_db_status(col, match_id, "completed", {
                "home_score": home_score, "away_score": away_score,
                "time_elapsed": time_elapsed,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "text": (f"🏁 FULL TIME: {fixture['home_team']} "
                         f"{home_score}–{away_score} {fixture['away_team']}"),
                "event_type": "full_time",
                "home_score": home_score, "away_score": away_score,
            })
            fetch_and_forward_statistics(fixture, live)
            move_completed_game_to_history(col, history_col, match_id)
            full_time_sent = True
            break

        # Periodic statistics every 5 polls (~225 s)
        poll_count += 1
        if poll_count % 5 == 0:
            logger.info(f"📊 Fetching statistics snapshot at {minute_disp}' for {label}")
            fetch_and_forward_statistics(fixture, live)

        time.sleep(POLL_INTERVAL_SEC)

    logger.info(f"✅ Done polling {label}")


def _find_goal_from_incidents(
    incidents: List[Dict], is_home: bool
) -> Tuple[str, Optional[str]]:
    for inc in reversed(incidents):
        if inc["type"] == "G" and inc["is_home"] == is_home:
            return inc.get("player", "Unknown"), inc.get("assist")
    return "Unknown", None


# ─────────────────────────────────────────────────────────────────────────────
# POLL QUEUE
# ─────────────────────────────────────────────────────────────────────────────

_poll_queue: _queue.Queue = _queue.Queue()
_queue_worker_started     = False
_queue_lock               = threading.Lock()


def _queue_worker():
    logger.info("🔁 World Cup poll queue worker started (Flashscore)")
    while True:
        try:
            task = _poll_queue.get(timeout=5)
            if task is None:
                break
            fixture, col, history_col = task
            match_id = fixture["match_id"]
            label    = f"{fixture['home_team']} vs {fixture['away_team']}"

            with polls_lock:
                if match_id in active_polls:
                    logger.info(f"⏭️  Already polling {label}")
                    _poll_queue.task_done()
                    continue
                active_polls.add(match_id)

            try:
                poll_live_game(fixture, col, history_col)
            except Exception as e:
                logger.error(f"Error polling {label}: {e}")
            finally:
                with polls_lock:
                    active_polls.discard(match_id)
                logger.info(f"✅ Done: {label}")
                _poll_queue.task_done()

        except _queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Queue worker error: {e}")


def _ensure_queue_worker():
    global _queue_worker_started
    with _queue_lock:
        if not _queue_worker_started:
            t = threading.Thread(
                target=_queue_worker, daemon=True, name="wc-poll-queue-worker"
            )
            t.start()
            _queue_worker_started = True


def start_polling_for_game(fixture: dict, col, history_col):
    match_id = fixture["match_id"]
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"

    with polls_lock:
        if match_id in active_polls:
            logger.info(f"⏭️  Already polling {label}")
            return

    _ensure_queue_worker()
    _poll_queue.put((fixture, col, history_col))
    logger.info(f"📥 Queued: {label}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash — World Cup 2026 Flashscore Poller")
    logger.info("=" * 65)

    start_health_server()
    mongo_client, col = connect_db()
    history_col       = get_history_collection(mongo_client)

    cleanup_all_completed_games(col, history_col)

    existing = load_fixtures_from_db(col)
    if existing:
        logger.info(f"📦 {len(existing)} fixture(s) in DB — skipping startup scrape")
        last_scrape_time = time.time()
    else:
        logger.info("📭 DB empty — running initial scrape...")
        run_scraper(col)
        last_scrape_time = time.time()

    last_cleanup_time    = time.time()
    lineups_fetched_set: set = set()

    while True:
        try:
            now_utc = datetime.now(timezone.utc)

            if time.time() - last_scrape_time >= SCRAPE_INTERVAL_SEC:
                logger.info("\n🔄 6-hour World Cup rescrape starting...")
                run_scraper(col)
                last_scrape_time = time.time()
                lineups_fetched_set.clear()

            if time.time() - last_cleanup_time >= CLEANUP_INTERVAL_SEC:
                cleanup_all_completed_games(col, history_col)
                last_cleanup_time = time.time()

            fixtures = load_fixtures_from_db(col)
            if not fixtures:
                logger.warning("📭 No fixtures in DB — running scraper now")
                run_scraper(col)
                last_scrape_time = time.time()
                fixtures = load_fixtures_from_db(col)

            live_fixtures = get_live_fixtures(fixtures)

            if live_fixtures:
                logger.info(f"\n🔴 {len(live_fixtures)} WORLD CUP LIVE GAME(S) DETECTED")
                for lf in live_fixtures:
                    mid   = lf["match_id"]
                    if lf.get("status") != "live":
                        update_fixture_status(mid, "live")
                        update_db_status(col, mid, "live")

                    if mid not in lineups_fetched_set and not lf.get("_lineups_fetched"):
                        if not check_lineups_exist_in_backend(mid):
                            fetch_and_forward_lineups(lf, col)
                        lineups_fetched_set.add(mid)

                    start_polling_for_game(lf, col, history_col)

                time.sleep(LIVE_CHECK_INTERVAL_SEC)
                continue

            upcoming_fixtures = []
            for f in fixtures:
                ko = f.get("_kickoff_utc")
                if not ko or f.get("status") == "completed":
                    continue
                mins = (ko - now_utc).total_seconds() / 60
                if 0 < mins <= 1440:
                    upcoming_fixtures.append((mins, f))

            if not upcoming_fixtures:
                logger.info("📭 No fixtures in next 24h. Sleeping 1h then rescraping...")
                time.sleep(3600)
                run_scraper(col)
                last_scrape_time = time.time()
                continue

            upcoming_fixtures.sort(key=lambda x: x[0])
            logger.info(f"📅 {len(upcoming_fixtures)} fixture(s) in next 24h:")
            for mins, f in upcoming_fixtures:
                ko_local    = (f["_kickoff_utc"] + NAIROBI_OFFSET).strftime("%H:%M")
                status_icon = "🔴" if f.get("status") == "soon" else "⏳"
                logger.info(
                    f"   {status_icon} {f['home_team']} vs {f['away_team']} "
                    f"at {ko_local} EAT ({int(mins)} mins)"
                )

            for mins_to_game, fixture in upcoming_fixtures:
                mid   = fixture["match_id"]
                label = f"{fixture['home_team']} vs {fixture['away_team']}"

                if 0 < mins_to_game <= 60:
                    if fixture.get("status") != "soon":
                        logger.info(f"⏰ {label} — {int(mins_to_game)} mins — setting SOON")
                        update_fixture_status(mid, "soon")
                        update_db_status(col, mid, "soon")

                    if mid not in lineups_fetched_set and not fixture.get("_lineups_fetched"):
                        if not check_lineups_exist_in_backend(mid):
                            logger.info(f"📋 Fetching lineups for {label}")
                            if fetch_and_forward_lineups(fixture, col):
                                lineups_fetched_set.add(mid)
                        else:
                            lineups_fetched_set.add(mid)
                            logger.info(f"   Lineups already in backend for {label}")

                elif mins_to_game <= 1440:
                    if fixture.get("status") not in ("upcoming", "soon"):
                        update_db_status(col, mid, "upcoming")

            closest_mins    = upcoming_fixtures[0][0]
            closest_fixture = upcoming_fixtures[0][1]

            if 0 < closest_mins <= 5:
                logger.info(
                    f"⚽ {closest_fixture['home_team']} vs {closest_fixture['away_team']} "
                    f"starting in {int(closest_mins)} mins"
                )
                start_polling_for_game(closest_fixture, col, history_col)
                time.sleep(POLL_INTERVAL_SEC)
                continue

            if closest_mins <= 60:
                sleep_secs = LINEUP_POLL_INTERVAL_SEC
                logger.info(f"⏳ Checking every {sleep_secs}s — {int(closest_mins)} mins to kickoff")
            elif closest_mins <= 1440:
                sleep_secs = HOUR_CHECK_INTERVAL_SEC
                logger.info(f"📅 Next game in {int(closest_mins/60)}h — waking hourly")
            else:
                sleep_secs = 3600
                logger.info("💤 Next game far away — sleeping 1h")

            time.sleep(sleep_secs)

        except KeyboardInterrupt:
            logger.info("🛑 Interrupted — shutting down")
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(60)

    if mongo_client:
        mongo_client.close()
        logger.info("🔌 MongoDB closed")


if __name__ == "__main__":
    main()