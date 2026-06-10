"""
World Cup 2026 — Scraper + Live Poller
=======================================
Full port of the multi-league live_poller.py architecture for the FIFA
World Cup 2026 (Sofascore unique-tournament ID 16).

Fixes carried over from live_poller.py:
  1. api_get returns (data, session) so refreshed sessions propagate
  2. scrape_via_daily / get_current_season update their local session on every call
  3. lineup fetch uses a fresh temp_session — no bare `session` reference
  4. Wider, randomised warm-up paths to reduce fingerprinting
  5. Exponential back-off when sustained 403s occur
  6. Single-threaded poll queue — only ONE Sofascore request pipeline at a time
  7. SOFASCORE_SEMAPHORE caps concurrent requests even if queue is bypassed

Install:  pip install curl_cffi pymongo requests python-dotenv
Run:      python worldcup_poller.py
"""

import time
import hashlib
import random
import logging
import os
import threading
import queue as _queue
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

from curl_cffi import requests as cffi_requests
import requests as std_requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

WORLD_CUP_TOURNAMENT_ID = 16        # Sofascore unique-tournament ID
WORLD_CUP_LABEL         = "World Cup 2026"
DAYS_AHEAD              = 60        # how far ahead to scan for fixtures

MATCH_DURATION_MINS     = 120
DAILY_MAX_DAYS          = 60
DAILY_MAX_MISSES        = 7
DATABASE_URL = os.getenv("MONGO_URI", "mongodb://localhost:27017")

DB_NAME         = "clashdb"
COLLECTION_NAME = "fixtures"        # World Cup goes into its own collection

NAIROBI_OFFSET = timedelta(hours=3)
SOFASCORE_API  = "https://api.sofascore.com/api/v1"
SOFASCORE_HOME = "https://www.sofascore.com"
DEFAULT_ODDS   = {"home_win": 2.50, "away_win": 2.80, "draw": 3.20}

FANCLASH_API = os.environ.get("FANCLASH_API")

POLL_INTERVAL_SEC        = 45
LINEUP_POLL_INTERVAL_SEC = 30
HOUR_CHECK_INTERVAL_SEC  = 3600
SCRAPE_INTERVAL_SEC      = 3600 * 6
LIVE_CHECK_INTERVAL_SEC  = 60
CLEANUP_INTERVAL_SEC     = 300

# ── Team name corrections ────────────────────────────────────────────────────
TEAM_NAME_CORRECTIONS = {
    # UEFA
    "Germany": "Germany", "Spain": "Spain", "France": "France",
    "England": "England", "Portugal": "Portugal", "Netherlands": "Netherlands",
    "Belgium": "Belgium", "Croatia": "Croatia", "Italy": "Italy",
    "Switzerland": "Switzerland", "Denmark": "Denmark", "Sweden": "Sweden",
    "Poland": "Poland", "Wales": "Wales", "Serbia": "Serbia",
    "Scotland": "Scotland", "Turkey": "Turkey", "Ukraine": "Ukraine",
    "Austria": "Austria", "Hungary": "Hungary", "Czech Republic": "Czech Republic",
    "Norway": "Norway", "Greece": "Greece",
    "Bosnia & Herzegovina": "Bosnia and Herzegovina",
    "Türkiye": "Turkey",
    # CONMEBOL
    "Brazil": "Brazil", "Argentina": "Argentina", "Uruguay": "Uruguay",
    "Colombia": "Colombia", "Chile": "Chile", "Peru": "Peru",
    "Ecuador": "Ecuador", "Paraguay": "Paraguay", "Venezuela": "Venezuela",
    # CAF
    "Morocco": "Morocco", "Senegal": "Senegal", "Tunisia": "Tunisia",
    "Algeria": "Algeria", "Nigeria": "Nigeria", "Cameroon": "Cameroon",
    "Egypt": "Egypt", "Ghana": "Ghana", "Ivory Coast": "Ivory Coast",
    "Côte d'Ivoire": "Ivory Coast",
    "Mali": "Mali", "DR Congo": "DR Congo", "Congo DR": "DR Congo",
    # AFC
    "Japan": "Japan", "South Korea": "South Korea", "Korea Republic": "South Korea",
    "Australia": "Australia", "Iran": "Iran", "Saudi Arabia": "Saudi Arabia",
    "Qatar": "Qatar", "Uzbekistan": "Uzbekistan", "Iraq": "Iraq", "Jordan": "Jordan",
    # CONCACAF
    "USA": "United States", "United States": "United States",
    "Mexico": "Mexico", "Canada": "Canada", "Panama": "Panama",
    "Costa Rica": "Costa Rica", "Haiti": "Haiti",
    "Curaçao": "Curacao", "Cabo Verde": "Cape Verde",
    # OFC
    "New Zealand": "New Zealand",
}

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL POLL TRACKING
# ─────────────────────────────────────────────────────────────────────────────

active_polls: set  = set()
polls_lock         = threading.Lock()

# Only ONE Sofascore request at a time — primary anti-ban measure
SOFASCORE_SEMAPHORE = threading.Semaphore(1)

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
            self.wfile.write(b'{"status": "waking up worldcup poller"}')
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"FanClash WorldCup Poller OK")

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
# SESSION FACTORY
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

WARMUP_URLS = [
    SOFASCORE_HOME,
    f"{SOFASCORE_HOME}/football",
    f"{SOFASCORE_HOME}/team/football/brazil/14",
    f"{SOFASCORE_HOME}/team/football/argentina/12",
    f"{SOFASCORE_HOME}/team/football/france/4481",
    f"{SOFASCORE_HOME}/team/football/england/3",
]


def make_session(warm_up: bool = True) -> cffi_requests.Session:
    """Create a fresh curl_cffi session impersonating Chrome 124."""
    session = cffi_requests.Session(impersonate="chrome124")
    session.headers.update({
        "Accept-Language":    "en-US,en;q=0.9",
        "Accept-Encoding":    "gzip, deflate, br",
        "Accept":             "application/json, text/plain, */*",
        "Connection":         "keep-alive",
        "Sec-Fetch-Dest":     "empty",
        "Sec-Fetch-Mode":     "cors",
        "Sec-Fetch-Site":     "same-site",
        "User-Agent":         random.choice(USER_AGENTS),
        "Referer":            "https://www.sofascore.com/",
        "Origin":             "https://www.sofascore.com",
        "Cache-Control":      "max-age=0",
        "Sec-Ch-Ua":          '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile":   "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    })

    if warm_up:
        try:
            time.sleep(random.uniform(2.0, 5.0))
            url = random.choice(WARMUP_URLS)
            r = session.get(url, timeout=15)
            logger.info(f"   Warm-up: HTTP {r.status_code} ({url})")
            time.sleep(random.uniform(1.5, 3.5))
        except Exception as e:
            logger.warning(f"   Warm-up failed: {e}")

    return session


# ─────────────────────────────────────────────────────────────────────────────
# API HELPER  — returns (data, session) so refreshed sessions propagate
# ─────────────────────────────────────────────────────────────────────────────

def api_get(
    session: cffi_requests.Session,
    path: str,
    retries: int = 5,
) -> Tuple[Optional[Dict], cffi_requests.Session]:
    url = f"{SOFASCORE_API}{path}"
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(1.5, 3.0))
            resp = session.get(url, timeout=25)

            if resp.status_code == 200:
                return resp.json(), session

            if resp.status_code == 404:
                return None, session

            if resp.status_code == 403:
                logger.warning(
                    f"   HTTP 403 for {path} (attempt {attempt + 1}) — refreshing session..."
                )
                session = make_session(warm_up=True)
                time.sleep(random.uniform(5, 10))
                continue

            if resp.status_code == 429:
                wait = 20 * (attempt + 1)
                logger.warning(f"   Rate limited — waiting {wait}s...")
                time.sleep(wait)
            else:
                logger.warning(
                    f"   HTTP {resp.status_code} for {path} (attempt {attempt + 1})"
                )
                time.sleep(5)

        except Exception as e:
            logger.warning(f"   Request error attempt {attempt + 1}: {e}")
            time.sleep(8)

    return None, session


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def correct_team_name(name: str) -> str:
    cleaned = " ".join(name.split())
    return TEAM_NAME_CORRECTIONS.get(cleaned, cleaned)


def eat_from_timestamp(ts: int) -> Tuple[str, str, str]:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + NAIROBI_OFFSET
    return dt.strftime("%Y-%m-%d"), dt.strftime("%d %b"), dt.strftime("%H:%M")


def generate_match_id(home: str, away: str, date_iso: str) -> str:
    s = "_".join([
        home.lower().replace(" ", "_").replace("&", "and"),
        away.lower().replace(" ", "_").replace("&", "and"),
        date_iso,
        "world_cup_2026",
    ])
    return hashlib.md5(s.encode()).hexdigest()[:12]


def event_status(event: Dict) -> str:
    type_ = (event.get("status") or {}).get("type", "")
    code  = (event.get("status") or {}).get("code", 0)
    if type_ == "inprogress":
        return "live"
    if code in (100, 110, 120):
        return "completed"
    return "upcoming"


def is_match_over(date_iso: str, time_str: str) -> bool:
    if not date_iso or not time_str or time_str == "TBD":
        return False
    try:
        naive = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
        kickoff_utc = (naive - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        return (kickoff_utc + timedelta(minutes=MATCH_DURATION_MINS)) < datetime.now(timezone.utc)
    except Exception:
        return False


def parse_event(event: Dict) -> Optional[Dict]:
    home_name = (event.get("homeTeam") or {}).get("name", "")
    away_name = (event.get("awayTeam") or {}).get("name", "")
    if not home_name or not away_name:
        return None

    home_team = correct_team_name(home_name)
    away_team = correct_team_name(away_name)

    ts = event.get("startTimestamp")
    if ts:
        date_iso, date_display, time_eat = eat_from_timestamp(ts)
    else:
        now = datetime.now(timezone.utc)
        date_iso     = now.strftime("%Y-%m-%d")
        date_display = now.strftime("%d %b")
        time_eat     = "TBD"

    status = event_status(event)
    home_score = away_score = None
    if status in ("completed", "live"):
        home_score = (event.get("homeScore") or {}).get("current")
        away_score = (event.get("awayScore") or {}).get("current")

    sofascore_id = event.get("id")
    match_id = (
        str(sofascore_id)
        if sofascore_id is not None
        else generate_match_id(home_team, away_team, date_iso)
    )

    return {
        "_id":                  match_id,
        "match_id":             match_id,
        "sofascore_id":         sofascore_id,
        "home_team":            home_team,
        "away_team":            away_team,
        "league":               WORLD_CUP_LABEL,
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
        "source":               "sofascore",
        "scraped_at":           datetime.now(timezone.utc),
        "votes":                0,
        "comments":             0,
        "voters":               [],
        "commentary":           [],
        "commentary_count":     0,
        "last_commentary_at":   None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPE VIA DAILY  — session propagated on every api_get call
# ─────────────────────────────────────────────────────────────────────────────

def scrape_via_daily(
    session: cffi_requests.Session,
) -> Tuple[List[Dict], cffi_requests.Session]:
    """Scan day-by-day for World Cup fixtures."""
    logger.info("   📆 Scanning day-by-day for World Cup fixtures...")
    docs: List[Dict]  = []
    seen: set         = set()
    matchdays_found   = 0
    consecutive_misses = 0

    today   = datetime.now(timezone.utc).date()
    cutoff  = today + timedelta(days=DAILY_MAX_DAYS)
    current = today

    while current <= cutoff:
        if consecutive_misses >= DAILY_MAX_MISSES and matchdays_found > 0:
            logger.info(f"   ⏹️  {DAILY_MAX_MISSES} consecutive empty days — stopping")
            break

        day_str = current.strftime("%Y-%m-%d")
        data, session = api_get(session, f"/sport/football/scheduled-events/{day_str}")

        if data:
            day_docs = []
            for ev in data.get("events", []):
                tid = (
                    (ev.get("tournament") or {})
                    .get("uniqueTournament", {})
                    .get("id")
                )
                if tid != WORLD_CUP_TOURNAMENT_ID:
                    continue
                doc = parse_event(ev)
                if not doc or doc["_id"] in seen:
                    continue
                if doc["status"] != "upcoming":
                    continue
                if is_match_over(doc["date_iso"], doc["time"]):
                    continue
                seen.add(doc["_id"])
                day_docs.append(doc)

            if day_docs:
                matchdays_found   += 1
                consecutive_misses = 0
                logger.info(f"   {day_str} → {len(day_docs)} World Cup matches")
                docs.extend(day_docs)
            else:
                consecutive_misses += 1
        else:
            consecutive_misses += 1

        current += timedelta(days=1)
        time.sleep(random.uniform(2.0, 3.5))

    return docs, session


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPE VIA ROUNDS  — session propagated on every api_get call
# ─────────────────────────────────────────────────────────────────────────────

def get_current_season(
    session: cffi_requests.Session,
) -> Tuple[Optional[int], cffi_requests.Session]:
    data, session = api_get(session, f"/unique-tournament/{WORLD_CUP_TOURNAMENT_ID}/seasons")
    if not data:
        return None, session
    seasons   = data.get("seasons", [])
    season_id = seasons[0].get("id") if seasons else None
    return season_id, session


def scrape_via_rounds(
    session: cffi_requests.Session,
    season_id: int,
) -> Tuple[List[Dict], cffi_requests.Session]:
    data, session = api_get(
        session,
        f"/unique-tournament/{WORLD_CUP_TOURNAMENT_ID}/season/{season_id}/rounds",
        retries=2,
    )
    if not data:
        return [], session

    all_rounds = sorted(
        set(r.get("round") for r in data.get("rounds", []) if r.get("round") is not None)
    )
    if not all_rounds:
        return [], session

    logger.info(f"   Found {len(all_rounds)} rounds in season {season_id}")

    docs: List[Dict] = []
    seen: set        = set()

    for rnd in all_rounds:
        rdata, session = api_get(
            session,
            f"/unique-tournament/{WORLD_CUP_TOURNAMENT_ID}/season/{season_id}/events/round/{rnd}",
        )
        if not rdata:
            continue

        round_docs = []
        for ev in rdata.get("events", []):
            doc = parse_event(ev)
            if not doc or doc["_id"] in seen:
                continue
            if doc["status"] != "upcoming":
                continue
            if is_match_over(doc["date_iso"], doc["time"]):
                continue
            seen.add(doc["_id"])
            round_docs.append(doc)

        if round_docs:
            logger.info(f"   Round {rnd:>3} → {len(round_docs)} upcoming")
            docs.extend(round_docs)

        time.sleep(random.uniform(2.0, 4.0))

    return docs, session


# ─────────────────────────────────────────────────────────────────────────────
# FULL SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def run_scraper(col) -> List[Dict]:
    logger.info("\n" + "=" * 65)
    logger.info("📡 RUNNING WORLD CUP SCRAPER")
    logger.info("=" * 65)

    session = make_session(warm_up=True)

    # Try rounds first; fall back to daily if no results
    season_id, session = get_current_season(session)
    docs: List[Dict]   = []

    if season_id:
        logger.info(f"   Season ID detected: {season_id}")
        docs, session = scrape_via_rounds(session, season_id)

    if not docs:
        logger.info("   Rounds returned nothing — falling back to day-by-day scan")
        docs, session = scrape_via_daily(session)

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
        wait = random.uniform(45, 90)
        logger.warning(f"   ⚠️  No fixtures found — backing off {wait:.0f}s")
        time.sleep(wait)

    logger.info(f"\n📊 Scraper done: {len(docs)} World Cup fixtures")
    return docs


# ─────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    try:
        client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=15000)
        client.admin.command("ping")
        col = client[DB_NAME][COLLECTION_NAME]
        col.create_index("match_id",    unique=True)
        col.create_index("sofascore_id")
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
    history_col = client[DB_NAME]["fixtures_history"]
    history_col.create_index("completed_at")
    history_col.create_index("match_id")
    history_col.create_index("status")
    return history_col


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
        logger.info(
            f"📦 Moved {match_id} "
            f"({game['home_team']} vs {game['away_team']}) to history"
        )
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
            "match_id":         f.get("match_id"),
            "sofascore_id":     f.get("sofascore_id"),
            "home_team":        f.get("home_team"),
            "away_team":        f.get("away_team"),
            "home_score":       f.get("home_score", 0),
            "away_score":       f.get("away_score", 0),
            "status":           f.get("status", "upcoming"),
            "is_live":          f.get("is_live", False),
            "date_iso":         date_iso,
            "time":             time_str,
            "_kickoff_utc":     kickoff_utc,
            "_lineups_fetched": f.get("lineups_fetched", False),
        })
    fixtures.sort(
        key=lambda x: x["_kickoff_utc"] or datetime.max.replace(tzinfo=timezone.utc)
    )
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
# BACKEND API CALLS
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
            home_players = data.get("lineups", {}).get("home", {}).get("players", [])
            away_players = data.get("lineups", {}).get("away", {}).get("players", [])
            return bool(home_players or away_players)
        return False
    except Exception:
        return False


def forward_event(fixture: dict, event_type: str, data: dict):
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    payload = {
        "fixture_id":     fixture["match_id"],
        "event_type":     event_type,
        "minute":         data.get("minute", 0),
        "minute_display": data.get("minute_display", f"{data.get('minute', 0)}'"),
        "home_score":     data.get("home_score", 0),
        "away_score":     data.get("away_score", 0),
        "timestamp":      {"$date": timestamp_ms},
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
        r = std_requests.post(
            f"{FANCLASH_API}/games/live-update", json=payload, timeout=5
        )
        if r.status_code == 200:
            logger.debug(f"✅ Forwarded {event_type}")
        else:
            logger.warning(f"❌ Failed {event_type}: {r.status_code}")
    except Exception as e:
        logger.error(f"forward_event error: {e}")


def send_commentary(fixture: dict, commentary_data: dict):
    created_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
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
            "created_at":     {"$date": created_at_ms},
        }
        entry   = {k: v for k, v in entry.items() if v is not None}
        payload = {"match_id": fixture["match_id"], "entry": entry}
        r = std_requests.post(
            f"{FANCLASH_API}/games/commentary", json=payload, timeout=3
        )
        if r.status_code == 200:
            logger.debug("📝 Commentary sent")
        else:
            logger.warning(f"❌ Commentary failed: {r.status_code}")
    except Exception as e:
        logger.warning(f"⚠️ Commentary error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# LINEUP FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_forward_lineups(
    session: cffi_requests.Session,
    fixture: Dict,
    col,
) -> bool:
    sofascore_id = fixture.get("sofascore_id")
    match_id     = fixture.get("match_id")
    label        = f"{fixture['home_team']} vs {fixture['away_team']}"

    if not sofascore_id:
        logger.warning(f"⚠️  No sofascore_id for {label}, cannot fetch lineups")
        return False

    logger.info(f"📋 Fetching lineups for {label} (sofascore_id={sofascore_id})")

    try:
        session.headers.update({
            "Referer":           f"{SOFASCORE_HOME}/event/{sofascore_id}",
            "X-Requested-With":  "XMLHttpRequest",
        })
        resp = session.get(
            f"{SOFASCORE_API}/event/{sofascore_id}/lineups", timeout=15
        )
        if resp.status_code != 200:
            logger.warning(f"   Lineups HTTP {resp.status_code} for {label}")
            return False

        lineups_data     = resp.json()
        home_players_raw = lineups_data.get("home", {}).get("players", [])
        away_players_raw = lineups_data.get("away", {}).get("players", [])

        if not home_players_raw and not away_players_raw:
            logger.info(f"   ⏳ Lineups not yet available for {label}")
            return False

        def get_name(player):
            for field in ("name", "fullName", "displayName", "shortName"):
                if player.get(field):
                    return str(player[field])
            if "player" in player:
                p = player["player"]
                for field in ("name", "fullName", "displayName", "shortName"):
                    if p.get(field):
                        return str(p[field])
            return f"Player #{player.get('jerseyNumber', '?')}"

        def safe_player(player):
            jersey = player.get("jerseyNumber", 0)
            if isinstance(jersey, str):
                jersey = int(jersey) if jersey.isdigit() else 0
            elif not isinstance(jersey, int):
                jersey = 0
            return {
                "name":         get_name(player),
                "position":     str(player.get("position") or "Unknown"),
                "jerseyNumber": jersey,
                "captain":      bool(player.get("captain", False)),
                "lineup":       bool(player.get("lineup", True)),
            }

        def split_players(raw, bench_raw):
            starters, bench = [], []
            for p in raw:
                (starters if p.get("lineup", True) else bench).append(safe_player(p))
            for p in bench_raw:
                bench.append(safe_player(p))
            return starters, bench

        home_starters, home_bench = split_players(
            home_players_raw,
            lineups_data.get("home", {}).get("bench", []),
        )
        away_starters, away_bench = split_players(
            away_players_raw,
            lineups_data.get("away", {}).get("bench", []),
        )

        payload = {
            "fixture_id": match_id,
            "lineups": {
                "home": {
                    "formation": str(lineups_data.get("home", {}).get("formation") or "4-2-3-1"),
                    "players":   home_starters,
                    "bench":     home_bench,
                    "coach":     {"name": str(
                        lineups_data.get("home", {}).get("coach", {}).get("name") or "Unknown"
                    )},
                },
                "away": {
                    "formation": str(lineups_data.get("away", {}).get("formation") or "4-2-3-1"),
                    "players":   away_starters,
                    "bench":     away_bench,
                    "coach":     {"name": str(
                        lineups_data.get("away", {}).get("coach", {}).get("name") or "Unknown"
                    )},
                },
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

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
# LIVE POLLER — per-game, driven by the poll queue
# ─────────────────────────────────────────────────────────────────────────────

def fetch_live_data(
    session: cffi_requests.Session,
    sofascore_id: int,
    retries: int = 4,
) -> Tuple[Optional[dict], cffi_requests.Session]:
    """
    Fetch live event data + incidents.
    Uses SOFASCORE_SEMAPHORE so only 1 request runs at a time.
    403 → exponential back-off WITHOUT spawning new sessions inside the loop.
    Session rebuilt ONCE after all retries fail.
    """
    url = f"{SOFASCORE_API}/event/{sofascore_id}"

    for attempt in range(retries):
        try:
            with SOFASCORE_SEMAPHORE:
                time.sleep(random.uniform(3.0, 7.0))
                session.headers.update({
                    "Referer":          f"{SOFASCORE_HOME}/event/{sofascore_id}",
                    "X-Requested-With": "XMLHttpRequest",
                })
                resp = session.get(url, timeout=15)

            if resp.status_code == 200:
                event = resp.json().get("event", {})

                incidents = []
                try:
                    with SOFASCORE_SEMAPHORE:
                        time.sleep(random.uniform(2.0, 5.0))
                        inc_resp = session.get(
                            f"{SOFASCORE_API}/event/{sofascore_id}/incidents",
                            timeout=15,
                        )
                    if inc_resp.status_code == 200:
                        incidents = inc_resp.json().get("incidents", [])
                    elif inc_resp.status_code == 403:
                        logger.warning(f"   Incidents 403 for {sofascore_id} — retry next poll")
                except Exception as ie:
                    logger.warning(f"   Incidents fetch error: {ie}")

                return {
                    "home_score":   (event.get("homeScore") or {}).get("current", 0),
                    "away_score":   (event.get("awayScore") or {}).get("current", 0),
                    "status_type":  (event.get("status") or {}).get("type", ""),
                    "status_code":  (event.get("status") or {}).get("code", 0),
                    "time_elapsed": event.get("time", {}).get("elapsed", 0),
                    "time_extra":   event.get("time", {}).get("extra", 0),
                    "incidents":    incidents,
                }, session

            elif resp.status_code == 403:
                wait = (2 ** attempt) * random.uniform(3, 6)
                logger.warning(
                    f"   fetch_live_data 403 for {sofascore_id} "
                    f"(attempt {attempt + 1}) — backing off {wait:.0f}s"
                )
                time.sleep(wait)
                continue

            else:
                logger.warning(
                    f"   fetch_live_data HTTP {resp.status_code} "
                    f"for {sofascore_id} (attempt {attempt + 1})"
                )
                time.sleep(5)

        except Exception as e:
            logger.warning(f"   fetch_live_data error attempt {attempt + 1}: {e}")
            time.sleep(5)

    logger.warning(f"   All retries failed for {sofascore_id} — rebuilding session")
    session = make_session(warm_up=True)
    return None, session


def _get_player_name(inc: dict) -> str:
    if "player" in inc:
        p = inc["player"]
        if isinstance(p, dict):
            return p.get("name") or p.get("shortName") or "Unknown"
        return str(p)
    return inc.get("name", "Unknown")


def _find_goal_scorer_and_assist(
    incidents: list, is_home: bool
) -> Tuple[str, Optional[str]]:
    for inc in incidents:
        if inc.get("incidentType", "").lower() == "goal" and inc.get("isHome") == is_home:
            scorer = _get_player_name(inc)
            assist = None
            if "assist" in inc:
                assist = _get_player_name(inc["assist"])
            elif "assistPlayer" in inc:
                assist = _get_player_name(inc["assistPlayer"])
            return scorer, assist
    return "Unknown", None


def poll_live_game(
    session: cffi_requests.Session,
    fixture: dict,
    col,
    history_col,
):
    sofascore_id = fixture.get("sofascore_id")
    label        = f"{fixture['home_team']} vs {fixture['away_team']}"
    match_id     = fixture["match_id"]

    if not sofascore_id:
        logger.error(f"❌ Cannot poll {label}: no sofascore_id")
        return

    # Skip if already finished
    initial, session = fetch_live_data(session, sofascore_id)
    if initial and initial["status_code"] in (100, 110, 120):
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

    while True:
        live, session = fetch_live_data(session, sofascore_id)
        if not live:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        home_score   = live["home_score"]
        away_score   = live["away_score"]
        status_code  = live["status_code"]
        status_type  = live["status_type"]
        time_elapsed = live["time_elapsed"]
        time_extra   = live.get("time_extra", 0)
        incidents    = live.get("incidents", [])
        minute_disp  = f"{time_elapsed}" + (f"+{time_extra}" if time_extra else "")

        # ── Goals ─────────────────────────────────────────────────────────
        if home_score > last_home:
            scorer, assist = _find_goal_scorer_and_assist(incidents, is_home=True)
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
            scorer, assist = _find_goal_scorer_and_assist(incidents, is_home=False)
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
            inc_id = str(inc.get("id", ""))
            if inc_id in seen_incidents:
                continue
            seen_incidents.add(inc_id)

            inc_type = inc.get("incidentType", "").lower()
            inc_cls  = inc.get("incidentClass", "").lower()
            is_home  = inc.get("isHome", True)
            team     = fixture["home_team"] if is_home else fixture["away_team"]
            minute   = inc.get("time", {}).get("elapsed", time_elapsed)
            extra    = inc.get("time", {}).get("extra", 0)
            m_disp   = f"{minute}" + (f"+{extra}" if extra else "")
            player   = _get_player_name(inc)

            commentary_text       = ""
            commentary_event_type = inc_type

            if inc_type == "goal":
                continue  # handled above

            elif inc_type == "card":
                card = "yellow_card" if inc_cls == "yellow" else "red_card"
                icon = "🟨" if inc_cls == "yellow" else "🟥"
                commentary_text = f"{icon} {inc_cls.upper()} CARD - {player} ({team})"
                logger.info(f"{icon} {inc_cls.upper()} CARD — {team}: {player} ({m_disp}')")
                forward_event(fixture, card, {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })

            elif inc_type == "substitution":
                p_out = _get_player_name(inc.get("playerOut", {}))
                p_in  = _get_player_name(inc.get("playerIn", {}))
                commentary_text = f"🔄 SUBSTITUTION: {p_out} → {p_in} ({team})"
                logger.info(f"🔄 SUB — {team}: {p_out} → {p_in} ({m_disp}')")
                forward_event(fixture, "substitution", {
                    "minute": minute, "minute_display": m_disp,
                    "player_out": p_out, "player_in": p_in, "team": team,
                })

            elif inc_type == "corner":
                commentary_text = f"🚩 CORNER - {team}"
                forward_event(fixture, "corner", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })

            elif inc_type == "penalty":
                commentary_text = f"🎯 PENALTY! {player} ({team})"
                logger.info(f"🎯 PENALTY — {team}: {player} ({m_disp}')")
                forward_event(fixture, "penalty", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })

            elif inc_type == "offside":
                commentary_text = f"🚩 OFFSIDE - {player} ({team})"
                forward_event(fixture, "offside", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })

            elif inc_type == "free_kick":
                commentary_text = f"🆓 FREE KICK - {team}"
                forward_event(fixture, "free_kick", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })

            elif inc_type == "throw_in":
                commentary_text = f"🏃 THROW-IN - {team}"
                forward_event(fixture, "throw_in", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })

            elif inc_type == "shot":
                on_target = inc.get("onTarget", False)
                blocked   = inc.get("blocked", False)
                if on_target:
                    commentary_text = f"🎯 SHOT ON TARGET - {player} ({team})"
                elif blocked:
                    commentary_text = f"🛡️ SHOT BLOCKED - {player} ({team})"
                else:
                    commentary_text = f"💨 SHOT OFF TARGET - {player} ({team})"
                forward_event(fixture, "shot", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                    "on_target": on_target, "blocked": blocked,
                })

            elif inc_type == "foul":
                commentary_text = f"⚠️ FOUL - {player} ({team})"
                forward_event(fixture, "foul", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                })

            if commentary_text:
                send_commentary(fixture, {
                    "minute":         minute,
                    "minute_display": m_disp,
                    "text":           commentary_text,
                    "event_type":     commentary_event_type,
                    "home_score":     home_score,
                    "away_score":     away_score,
                    "team":           team,
                    "player":         player if inc_type != "substitution" else None,
                })

        # ── Match phase events ─────────────────────────────────────────────
        if status_type == "pause" and not half_time_sent:
            logger.info(f"⏸  HALF TIME: {home_score}–{away_score}")
            forward_event(fixture, "half_time", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "home_score": home_score, "away_score": away_score,
            })
            update_db_status(col, match_id, "live", {"time_elapsed": time_elapsed, "half": 1})
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "text": (
                    f"⏸ HALF TIME: {fixture['home_team']} "
                    f"{home_score}–{away_score} {fixture['away_team']}"
                ),
                "event_type": "half_time",
                "home_score": home_score, "away_score": away_score,
            })
            half_time_sent = True

        if status_type == "inprogress" and half_time_sent and not second_half_sent:
            logger.info("▶️  SECOND HALF STARTED")
            forward_event(fixture, "second_half", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
            })
            update_db_status(col, match_id, "live", {"half": 2})
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "text": (
                    f"▶️ SECOND HALF UNDERWAY! {fixture['home_team']} "
                    f"{home_score}–{away_score} {fixture['away_team']}"
                ),
                "event_type": "second_half",
                "home_score": home_score, "away_score": away_score,
            })
            second_half_sent = True

        if status_code in (100, 110, 120) and not full_time_sent:
            logger.info(f"🏁 FULL TIME: {label} — {home_score}–{away_score}")
            forward_event(fixture, "match_end", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "home_score": home_score, "away_score": away_score,
            })
            update_fixture_status(match_id, "completed")
            update_db_status(col, match_id, "completed", {
                "home_score":   home_score,
                "away_score":   away_score,
                "time_elapsed": time_elapsed,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "text": (
                    f"🏁 FULL TIME: {fixture['home_team']} "
                    f"{home_score}–{away_score} {fixture['away_team']}"
                ),
                "event_type": "full_time",
                "home_score": home_score, "away_score": away_score,
            })
            move_completed_game_to_history(col, history_col, match_id)
            full_time_sent = True
            break

        time.sleep(POLL_INTERVAL_SEC)

    logger.info(f"✅ Done polling {label}")


# ─────────────────────────────────────────────────────────────────────────────
# POLL QUEUE  — single background worker, one request pipeline at a time
# ─────────────────────────────────────────────────────────────────────────────

_poll_queue: _queue.Queue = _queue.Queue()
_queue_worker_started     = False
_queue_lock               = threading.Lock()


def _queue_worker():
    logger.info("🔁 World Cup poll queue worker started")
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
                    logger.info(f"⏭️  Already polling {label} (queue)")
                    _poll_queue.task_done()
                    continue
                active_polls.add(match_id)

            logger.info(f"🔴 Polling (queued): {label}")
            thread_session = make_session(warm_up=True)
            try:
                poll_live_game(thread_session, fixture, col, history_col)
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
    logger.info("🏆 FanClash — World Cup 2026 Scraper + Live Poller")
    logger.info("=" * 65)

    start_health_server()
    mongo_client, col = connect_db()
    history_col       = get_history_collection(mongo_client)

    # Move any leftover completed games to history on startup
    cleanup_all_completed_games(col, history_col)

    # Skip scrape if DB already has usable fixtures
    existing_fixtures = load_fixtures_from_db(col)
    if existing_fixtures:
        logger.info(
            f"📦 {len(existing_fixtures)} World Cup fixture(s) in DB — "
            f"skipping startup scrape"
        )
        last_scrape_time = time.time()
    else:
        logger.info("📭 DB empty — running initial World Cup scrape...")
        run_scraper(col)
        last_scrape_time = time.time()

    last_cleanup_time    = time.time()
    lineups_fetched_set: set = set()

    while True:
        try:
            now_utc = datetime.now(timezone.utc)

            # ── Periodic full rescrape every 6h ───────────────────────────
            if time.time() - last_scrape_time >= SCRAPE_INTERVAL_SEC:
                logger.info("\n🔄 6-hour World Cup rescrape starting...")
                run_scraper(col)
                last_scrape_time = time.time()
                lineups_fetched_set.clear()

            # ── Periodic cleanup every 5 minutes ─────────────────────────
            if time.time() - last_cleanup_time >= CLEANUP_INTERVAL_SEC:
                cleanup_all_completed_games(col, history_col)
                last_cleanup_time = time.time()

            # ── Load fixtures from DB ──────────────────────────────────────
            fixtures = load_fixtures_from_db(col)

            if not fixtures:
                logger.warning("📭 No World Cup fixtures in DB — running scraper now")
                run_scraper(col)
                last_scrape_time = time.time()
                fixtures = load_fixtures_from_db(col)

            # ── Check for live / just-kicked-off games ─────────────────────
            live_fixtures = get_live_fixtures(fixtures)

            if live_fixtures:
                logger.info(f"\n🔴 {len(live_fixtures)} WORLD CUP LIVE GAME(S) DETECTED")

                for live_fixture in live_fixtures:
                    mid   = live_fixture["match_id"]
                    label = f"{live_fixture['home_team']} vs {live_fixture['away_team']}"

                    if live_fixture.get("status") != "live":
                        update_fixture_status(mid, "live")
                        update_db_status(col, mid, "live")

                    # Fresh temp_session — never a bare `session` reference
                    if mid not in lineups_fetched_set and not live_fixture.get("_lineups_fetched"):
                        if not check_lineups_exist_in_backend(mid):
                            temp_session = make_session(warm_up=False)
                            fetch_and_forward_lineups(temp_session, live_fixture, col)
                        lineups_fetched_set.add(mid)

                    start_polling_for_game(live_fixture, col, history_col)

                time.sleep(LIVE_CHECK_INTERVAL_SEC)
                continue

            # ── Upcoming fixtures within 24h ───────────────────────────────
            upcoming_fixtures = []
            for f in fixtures:
                ko = f.get("_kickoff_utc")
                if not ko or f.get("status") == "completed":
                    continue
                mins_to_game = (ko - now_utc).total_seconds() / 60
                if 0 < mins_to_game <= 1440:
                    upcoming_fixtures.append((mins_to_game, f))

            if not upcoming_fixtures:
                logger.info("📭 No World Cup fixtures in next 24h. Sleeping 1h then rescraping...")
                time.sleep(3600)
                run_scraper(col)
                last_scrape_time = time.time()
                continue

            upcoming_fixtures.sort(key=lambda x: x[0])

            logger.info(f"📅 {len(upcoming_fixtures)} World Cup fixture(s) in next 24h:")
            for mins, f in upcoming_fixtures:
                ko_local    = (f["_kickoff_utc"] + NAIROBI_OFFSET).strftime("%H:%M")
                status_icon = "🔴" if f.get("status") == "soon" else "⏳"
                logger.info(
                    f"   {status_icon} {f['home_team']} vs {f['away_team']} "
                    f"at {ko_local} EAT ({int(mins)} mins)"
                )

            # Process fixtures needing attention
            for mins_to_game, fixture in upcoming_fixtures:
                mid   = fixture["match_id"]
                label = f"{fixture['home_team']} vs {fixture['away_team']}"

                if 0 < mins_to_game <= 60:
                    if fixture.get("status") != "soon":
                        logger.info(
                            f"⏰ {label} — {int(mins_to_game)} mins to kickoff "
                            f"— setting SOON status"
                        )
                        update_fixture_status(mid, "soon")
                        update_db_status(col, mid, "soon")

                    # Fresh temp_session for lineup fetch
                    if mid not in lineups_fetched_set and not fixture.get("_lineups_fetched"):
                        if not check_lineups_exist_in_backend(mid):
                            logger.info(f"📋 Fetching lineups for {label}")
                            temp_session = make_session(warm_up=False)
                            fetched = fetch_and_forward_lineups(temp_session, fixture, col)
                            if fetched:
                                lineups_fetched_set.add(mid)
                        else:
                            lineups_fetched_set.add(mid)
                            logger.info(f"   Lineups already in backend for {label}")

                elif mins_to_game <= 1440:
                    if fixture.get("status") not in ("upcoming", "soon"):
                        update_db_status(col, mid, "upcoming")

            # ── Start polling for games kicking off in ≤ 5 mins ───────────
            closest_mins    = upcoming_fixtures[0][0]
            closest_fixture = upcoming_fixtures[0][1]

            if 0 < closest_mins <= 5:
                logger.info(
                    f"⚽ {closest_fixture['home_team']} vs "
                    f"{closest_fixture['away_team']} starting in {int(closest_mins)} mins"
                )
                start_polling_for_game(closest_fixture, col, history_col)
                time.sleep(POLL_INTERVAL_SEC)
                continue

            # ── Sleep duration based on how close the next game is ─────────
            if closest_mins <= 60:
                sleep_secs = LINEUP_POLL_INTERVAL_SEC
                logger.info(
                    f"⏳ Checking every {sleep_secs}s — {int(closest_mins)} mins to next kickoff"
                )
            elif closest_mins <= 1440:
                sleep_secs = HOUR_CHECK_INTERVAL_SEC
                logger.info(
                    f"📅 Next game in {int(closest_mins / 60)}h — waking hourly"
                )
            else:
                sleep_secs = 3600
                logger.info("💤 Next World Cup game far away — sleeping 1h")

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