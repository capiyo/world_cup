"""
World Cup 2026 — Sportscore.io Scraper + Live Poller
=====================================================
Sportscore.io Free API - No API key required!
Rate limit: ~10,000 requests per 24 hours per IP

Endpoints:
  - Fixtures: GET /soccer/fixtures?tournament_id=world-cup
  - Live scores: GET /soccer/livescores
  - Match details: GET /soccer/fixtures/{id}
  - Lineups: GET /soccer/fixtures/{id}/lineups
  - Statistics: GET /soccer/fixtures/{id}/statistics
  - Events: GET /soccer/fixtures/{id}/events

Install:  pip install requests pymongo python-dotenv
Run:      python worldcup_poller_sportscore.py
"""

import time
import random
import logging
import os
import re
import threading
import queue as _queue
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests as std_requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

WORLD_CUP_LABEL = "World Cup 2026"
SPORTSCORE_BASE = "https://api.sportscore.io/v1"

MATCH_DURATION_MINS = 120
DATABASE_URL = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "clashdb"
COLLECTION_NAME = "sports"
NAIROBI_OFFSET = timedelta(hours=3)

FANCLASH_API = os.environ.get("FANCLASH_API")
DEFAULT_ODDS = {"home_win": 2.50, "away_win": 2.80, "draw": 3.20}

POLL_INTERVAL_SEC = 45
LINEUP_POLL_INTERVAL_SEC = 30
HOUR_CHECK_INTERVAL_SEC = 3600
SCRAPE_INTERVAL_SEC = 3600 * 6
LIVE_CHECK_INTERVAL_SEC = 60
CLEANUP_INTERVAL_SEC = 300

# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────

active_polls: set = set()
polls_lock = threading.Lock()
SPORTSCORE_SEMAPHORE = threading.Semaphore(1)

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
        body = b'{"status":"ok"}' if self.path == "/wakeup" else b"FanClash WorldCup Poller OK"
        self.send_response(200)
        self.send_header("Content-Type", "application/json" if self.path == "/wakeup" else "text/plain")
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_):
        pass


def start_health_server():
    port = int(os.environ.get("PORT", 8081))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"🌐 Health server on port {port}")


# ─────────────────────────────────────────────────────────────────────────────
# SPORTSCORE HTTP CLIENT
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

_session: Optional[std_requests.Session] = None
_session_lock: threading.Lock = threading.Lock()


def _make_session() -> std_requests.Session:
    s = std_requests.Session()
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "User-Agent": random.choice(USER_AGENTS),
    })
    return s


def _get_session() -> std_requests.Session:
    global _session
    with _session_lock:
        if _session is None:
            _session = _make_session()
        return _session


def sportscore_get(endpoint: str, params: Dict = None, retries: int = 3) -> Optional[Dict]:
    """
    GET https://api.sportscore.io/v1/<endpoint>
    No API key required for free tier!
    """
    url = f"{SPORTSCORE_BASE}/{endpoint}"
    
    for attempt in range(retries):
        try:
            with SPORTSCORE_SEMAPHORE:
                time.sleep(random.uniform(1.0, 2.0))
                resp = _get_session().get(url, params=params, timeout=20)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 404:
                logger.debug(f"   Sportscore 404: {endpoint}")
                return None

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"   Sportscore rate limited — waiting {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = (2 ** attempt) * random.uniform(5, 10)
                logger.warning(f"   Sportscore {resp.status_code} — retry in {wait:.0f}s")
                time.sleep(wait)
                continue

            logger.warning(f"   Sportscore HTTP {resp.status_code}: {endpoint}")
            time.sleep(5)

        except Exception as e:
            logger.warning(f"   Sportscore error attempt {attempt+1}: {e}")
            time.sleep(8)

    logger.error(f"   Sportscore all retries exhausted: {endpoint}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

_STRIP_TAGS = re.compile(r"<[^>]+>")


def _clean(s: str) -> str:
    return " ".join(_STRIP_TAGS.sub("", s or "").split())


def _map_status(status: str) -> str:
    """Map Sportscore status to internal status."""
    status_map = {
        "NS": "upcoming",
        "1H": "live",
        "2H": "live",
        "HT": "live",
        "ET": "live",
        "PEN": "live",
        "FT": "completed",
        "AET": "completed",
        "PEN_FT": "completed",
        "ABD": "cancelled",
        "PST": "postponed",
    }
    return status_map.get(status, "upcoming")


def is_match_over(date_iso: str, time_str: str) -> bool:
    if not date_iso or not time_str or time_str == "TBD":
        return False
    try:
        naive = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
        kickoff_utc = (naive - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        return (kickoff_utc + timedelta(minutes=MATCH_DURATION_MINS)) < datetime.now(timezone.utc)
    except Exception:
        return False


def _ts_to_eat(ts: int) -> Tuple[str, str, str]:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + NAIROBI_OFFSET
    return dt.strftime("%Y-%m-%d"), dt.strftime("%d %b"), dt.strftime("%H:%M")


def _safe_int(v) -> Optional[int]:
    if v is None:
        return None
    v = str(v).strip()
    if v and v not in ("-", ""):
        try:
            return int(v)
        except ValueError:
            pass
    return None


def generate_match_id(home_team: str, away_team: str, date_iso: str) -> str:
    """Generate stable match ID from teams and date."""
    import hashlib
    key = f"{home_team}_{away_team}_{date_iso}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def _build_fixture_doc(
    match_id: str,
    home_team: str,
    away_team: str,
    ts: int,
    status: str,
    home_score: Optional[int],
    away_score: Optional[int],
    sportscore_id: Optional[str] = None,
) -> Dict:
    if ts:
        date_iso, date_display, time_eat = _ts_to_eat(ts)
    else:
        now = datetime.now(timezone.utc)
        date_iso = now.strftime("%Y-%m-%d")
        date_display = now.strftime("%d %b")
        time_eat = "TBD"

    doc = {
        "_id": match_id,
        "match_id": match_id,
        "sportscore_id": sportscore_id,
        "home_team": home_team,
        "away_team": away_team,
        "league": WORLD_CUP_LABEL,
        "home_win": float(DEFAULT_ODDS["home_win"]),
        "away_win": float(DEFAULT_ODDS["away_win"]),
        "draw": float(DEFAULT_ODDS["draw"]),
        "date": date_display,
        "time": time_eat,
        "date_iso": date_iso,
        "home_score": home_score,
        "away_score": away_score,
        "status": status,
        "is_live": status == "live",
        "available_for_voting": status == "upcoming",
        "time_elapsed": 0,
        "source": "sportscore",
        "scraped_at": datetime.now(timezone.utc),
        "votes": 0,
        "comments": 0,
        "voters": [],
        "commentary": [],
        "commentary_count": 0,
        "last_commentary_at": None,
    }
    return doc


# ─────────────────────────────────────────────────────────────────────────────
# SPORTSCORE PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def parse_fixtures(data: Dict) -> List[Dict]:
    """Parse Sportscore fixtures response."""
    docs: List[Dict] = []
    if not data:
        return docs

    fixtures = data.get("data", [])
    if not fixtures:
        return docs

    for fixture in fixtures:
        # Get teams
        home_team = fixture.get("home_team", {}).get("name", "")
        away_team = fixture.get("away_team", {}).get("name", "")
        
        if not home_team or not away_team:
            continue

        # Get timestamp
        start_time = fixture.get("starting_at")
        ts = 0
        if start_time:
            try:
                dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                ts = int(dt.timestamp())
            except:
                pass

        # Get status
        status_str = fixture.get("state", {}).get("status", "NS")
        status = _map_status(status_str)

        # Get scores
        scores = fixture.get("scores", {})
        home_score = scores.get("home", {}).get("current")
        away_score = scores.get("away", {}).get("current")

        # Get match ID
        sportscore_id = str(fixture.get("id", ""))

        # Generate stable ID
        if ts:
            date_iso, _, _ = _ts_to_eat(ts)
        else:
            date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        match_id = generate_match_id(home_team, away_team, date_iso)

        docs.append(_build_fixture_doc(
            match_id, home_team, away_team, ts, status, home_score, away_score, sportscore_id
        ))

    return docs


def parse_live_feed(data: Dict) -> Optional[Dict]:
    """Parse Sportscore live match data."""
    if not data:
        return None

    fixture = data.get("data", {})
    if not fixture:
        return None

    # Get scores
    scores = fixture.get("scores", {})
    home_score = scores.get("home", {}).get("current", 0)
    away_score = scores.get("away", {}).get("current", 0)

    # Get status
    status_str = fixture.get("state", {}).get("status", "NS")
    status = _map_status(status_str)

    # Get time elapsed
    time_elapsed = fixture.get("time", {}).get("minute", 0)
    time_extra = fixture.get("time", {}).get("extra_minute", 0)

    return {
        "home_score": home_score,
        "away_score": away_score,
        "status": status,
        "time_elapsed": time_elapsed,
        "time_extra": time_extra,
    }


def parse_incidents(data: Dict) -> List[Dict]:
    """Parse Sportscore incidents/events."""
    incidents: List[Dict] = []
    if not data:
        return incidents

    events = data.get("data", [])
    if not events:
        return incidents

    for ev in events:
        ev_type = ev.get("type", "").lower()
        if ev_type == "goal":
            inc_type = "G"
        elif ev_type == "yellowcard":
            inc_type = "YC"
        elif ev_type == "redcard":
            inc_type = "RC"
        elif ev_type == "substitution":
            inc_type = "SB"
        elif ev_type == "penalty_missed":
            inc_type = "MS"
        elif ev_type == "penalty_scored":
            inc_type = "G"
        else:
            continue

        incident = {
            "id": str(ev.get("id", "")),
            "type": inc_type,
            "minute": ev.get("minute", 0),
            "extra": ev.get("extra_minute", 0),
            "is_home": ev.get("team", {}).get("name") == "home",  # May need better mapping
            "player": ev.get("player", {}).get("name", "Unknown"),
        }

        if inc_type == "SB":
            incident["assist"] = ev.get("player_out", {}).get("name")

        incidents.append(incident)

    return incidents


def parse_lineups(data: Dict) -> Optional[Dict]:
    """Parse Sportscore lineups."""
    if not data:
        return None

    lineups = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }

    lineups_data = data.get("data", [])
    if not lineups_data:
        return None

    for lineup in lineups_data:
        side = "home" if lineup.get("team", {}).get("name") == "home" else "away"
        for player in lineup.get("players", []):
            p = {
                "name": player.get("name", "Unknown"),
                "position": player.get("position", {}).get("name", "Unknown"),
                "jerseyNumber": player.get("jersey_number", 0),
                "captain": player.get("captain", False),
                "lineup": player.get("type") == "starting",
            }
            if p["lineup"]:
                lineups[side]["players"].append(p)
            else:
                lineups[side]["bench"].append(p)

    return lineups


def parse_statistics(data: Dict, time_elapsed: int, home_score: int, away_score: int) -> Optional[Dict]:
    """Parse Sportscore statistics."""
    if not data:
        return None

    stats = {
        "ball_possession_home": 0,
        "ball_possession_away": 0,
        "total_shots_home": 0,
        "total_shots_away": 0,
        "shots_on_target_home": 0,
        "shots_on_target_away": 0,
        "corners_home": 0,
        "corners_away": 0,
        "fouls_home": 0,
        "fouls_away": 0,
        "offsides_home": 0,
        "offsides_away": 0,
        "yellow_cards_home": 0,
        "yellow_cards_away": 0,
        "red_cards_home": 0,
        "red_cards_away": 0,
    }

    stats_data = data.get("data", [])
    for stat in stats_data:
        stat_name = stat.get("type", {}).get("name", "").lower()
        home_val = stat.get("data", {}).get("home", 0)
        away_val = stat.get("data", {}).get("away", 0)

        if "possession" in stat_name:
            stats["ball_possession_home"] = home_val
            stats["ball_possession_away"] = away_val
        elif "shots total" in stat_name or "total shots" in stat_name:
            stats["total_shots_home"] = home_val
            stats["total_shots_away"] = away_val
        elif "shots on target" in stat_name:
            stats["shots_on_target_home"] = home_val
            stats["shots_on_target_away"] = away_val
        elif "corners" in stat_name:
            stats["corners_home"] = home_val
            stats["corners_away"] = away_val
        elif "fouls" in stat_name:
            stats["fouls_home"] = home_val
            stats["fouls_away"] = away_val
        elif "offsides" in stat_name:
            stats["offsides_home"] = home_val
            stats["offsides_away"] = away_val
        elif "yellow cards" in stat_name:
            stats["yellow_cards_home"] = home_val
            stats["yellow_cards_away"] = away_val
        elif "red cards" in stat_name:
            stats["red_cards_home"] = home_val
            stats["red_cards_away"] = away_val

    stats["minute"] = time_elapsed
    stats["minute_display"] = f"{time_elapsed}'" if time_elapsed else "0'"
    stats["home_score"] = home_score
    stats["away_score"] = away_score
    stats["timestamp"] = datetime.now(timezone.utc).isoformat()

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def run_scraper(col) -> List[Dict]:
    logger.info("\n" + "=" * 65)
    logger.info("📡 RUNNING WORLD CUP 2026 SPORTSCORE SCRAPER")
    logger.info("=" * 65)

    docs: List[Dict] = []
    seen: set = set()

    # Get World Cup fixtures
    data = sportscore_get("soccer/fixtures", params={"tournament_id": "world-cup"})
    
    if not data:
        # Try alternative endpoint
        data = sportscore_get("soccer/fixtures", params={"competition": "world-cup"})

    if not data:
        logger.warning("   Sportscore: No data returned")
        wait = random.uniform(60, 120)
        logger.warning(f"   ⚠️  No fixtures found — backing off {wait:.0f}s")
        time.sleep(wait)
        return []

    page_docs = parse_fixtures(data)
    new = [d for d in page_docs if d["_id"] not in seen]
    for d in new:
        seen.add(d["_id"])
    docs.extend(new)

    logger.info(f"   Sportscore: got {len(docs)} fixtures")

    # Save to MongoDB
    if docs and col is not None:
        saved = 0
        for d in docs:
            try:
                col.update_one({"_id": d["_id"]}, {"$set": d}, upsert=True)
                saved += 1
            except Exception as e:
                logger.warning(f"   DB save error: {e}")
        logger.info(f"   💾 Saved {saved} World Cup fixtures")

    if not docs:
        wait = random.uniform(60, 120)
        logger.warning(f"   ⚠️  No fixtures found — backing off {wait:.0f}s")
        time.sleep(wait)

    logger.info(f"\n📊 Scraper done: {len(docs)} fixtures total")
    return docs


# ─────────────────────────────────────────────────────────────────────────────
# DB HELPERS (Same as Flashscore version)
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    try:
        client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=15000)
        client.admin.command("ping")
        col = client[DB_NAME][COLLECTION_NAME]
        col.create_index("match_id", unique=True)
        col.create_index("sportscore_id")
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
    return hcol


def move_completed_game_to_history(col, history_col, match_id: str) -> bool:
    if col is None or history_col is None:
        return False
    try:
        game = col.find_one({"match_id": match_id, "status": "completed"})
        if not game or game.get("moved_to_history"):
            return False
        game["completed_at"] = datetime.now(timezone.utc)
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
        if moved:
            logger.info(f"🧹 Cleaned up {moved} completed World Cup games to history")
    except Exception as e:
        logger.error(f"Error cleaning up: {e}")


def load_fixtures_from_db(col) -> List[Dict[str, Any]]:
    if col is None:
        return []
    fixtures = []
    for f in col.find({"status": {"$ne": "completed"}, "league": WORLD_CUP_LABEL}):
        date_iso = f.get("date_iso", "")
        time_str = f.get("time", "00:00")
        kickoff_utc = None
        try:
            naive_eat = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
            kickoff_utc = (naive_eat - NAIROBI_OFFSET).replace(tzinfo=timezone.utc)
        except Exception:
            pass
        fixtures.append({
            "match_id": f.get("match_id"),
            "sportscore_id": f.get("sportscore_id"),
            "home_team": f.get("home_team"),
            "away_team": f.get("away_team"),
            "home_score": f.get("home_score", 0),
            "away_score": f.get("away_score", 0),
            "status": f.get("status", "upcoming"),
            "is_live": f.get("is_live", False),
            "date_iso": date_iso,
            "time": time_str,
            "_kickoff_utc": kickoff_utc,
            "_lineups_fetched": f.get("lineups_fetched", False),
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


def update_db_status(col, match_id: str, status: str, extra_fields: Optional[dict] = None):
    if col is None:
        return
    fields = {
        "status": status,
        "is_live": status == "live",
        "available_for_voting": status in ("upcoming", "soon"),
    }
    if extra_fields:
        fields.update(extra_fields)
    try:
        col.update_one({"match_id": match_id}, {"$set": fields})
        logger.info(f"🗄️  DB → '{status}' for {match_id}")
    except Exception as e:
        logger.warning(f"update_db_status error: {e}")


def get_live_fixtures(fixtures: List[Dict]) -> List[Dict]:
    now_utc = datetime.now(timezone.utc)
    return [
        f for f in fixtures
        if f.get("status") == "live"
        or (f.get("_kickoff_utc") and now_utc >= f["_kickoff_utc"] and f.get("status") != "completed")
    ]


# ─────────────────────────────────────────────────────────────────────────────
# BACKEND API (Same as Flashscore version)
# ─────────────────────────────────────────────────────────────────────────────

def update_fixture_status(match_id: str, status: str):
    if status == "finished":
        status = "completed"
    try:
        r = std_requests.put(
            f"{FANCLASH_API}/games/{match_id}/status",
            json={"match_id": match_id, "status": status, "is_live": status == "live"},
            timeout=5,
        )
        if r.status_code == 200:
            logger.info(f"✅ Backend status → '{status}'")
        else:
            logger.warning(f"❌ Backend status failed: {r.status_code}")
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
    payload = {k: v for k, v in {
        "fixture_id": fixture["match_id"],
        "event_type": event_type,
        "minute": data.get("minute", 0),
        "minute_display": data.get("minute_display", f"{data.get('minute', 0)}'"),
        "home_score": data.get("home_score", 0),
        "away_score": data.get("away_score", 0),
        "timestamp": {"$date": ts_ms},
        "player": data.get("player"),
        "assist": data.get("assist"),
        "team": data.get("team"),
        "player_out": data.get("player_out"),
        "player_in": data.get("player_in"),
        "on_target": data.get("on_target"),
        "blocked": data.get("blocked"),
    }.items() if v is not None}
    try:
        r = std_requests.post(f"{FANCLASH_API}/games/live-update", json=payload, timeout=5)
        if r.status_code != 200:
            logger.warning(f"❌ forward_event {event_type}: {r.status_code}")
    except Exception as e:
        logger.error(f"forward_event error: {e}")


def send_commentary(fixture: dict, data: dict):
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    entry = {k: v for k, v in {
        "minute": data.get("minute", 0),
        "minute_display": data.get("minute_display", ""),
        "text": data.get("text", ""),
        "event_type": data.get("event_type", ""),
        "home_score": data.get("home_score", 0),
        "away_score": data.get("away_score", 0),
        "team": data.get("team"),
        "player": data.get("player"),
        "created_at": {"$date": ts_ms},
    }.items() if v is not None}
    try:
        r = std_requests.post(
            f"{FANCLASH_API}/games/commentary",
            json={"match_id": fixture["match_id"], "entry": entry},
            timeout=3,
        )
        if r.status_code != 200:
            logger.warning(f"❌ send_commentary: {r.status_code}")
    except Exception as e:
        logger.warning(f"send_commentary error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# LINEUP FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_forward_lineups(fixture: Dict, col) -> bool:
    sportscore_id = fixture.get("sportscore_id")
    match_id = fixture.get("match_id")
    label = f"{fixture['home_team']} vs {fixture['away_team']}"

    if not sportscore_id:
        logger.warning(f"⚠️  No sportscore_id for {label}")
        return False

    logger.info(f"📋 Fetching lineups for {label}")
    data = sportscore_get(f"soccer/fixtures/{sportscore_id}/lineups")
    lineups = parse_lineups(data)

    if not lineups:
        logger.info(f"   ⏳ Lineups not yet available for {label}")
        return False

    payload = {
        "fixture_id": match_id,
        "lineups": lineups,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        r = std_requests.post(f"{FANCLASH_API}/games/lineups", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Lineups stored for {label}")
            mark_lineups_fetched(col, match_id)
            return True
        logger.warning(f"❌ Backend rejected lineups: {r.status_code}")
        return False
    except Exception as e:
        logger.error(f"fetch_and_forward_lineups error: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# STATISTICS FETCHER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_forward_statistics(fixture: dict, live_data: dict):
    sportscore_id = fixture.get("sportscore_id")
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"

    if not sportscore_id:
        return

    data = sportscore_get(f"soccer/fixtures/{sportscore_id}/statistics")
    if not data:
        logger.warning(f"   No statistics for {label}")
        return

    payload = parse_statistics(
        data,
        time_elapsed=live_data.get("time_elapsed", 0),
        home_score=live_data.get("home_score", 0),
        away_score=live_data.get("away_score", 0),
    )
    if not payload:
        return

    payload["match_id"] = match_id
    minute_disp = payload.get("minute_display", "?")

    try:
        r = std_requests.post(f"{FANCLASH_API}/games/statistics", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"📊 Stats forwarded for {label} ({minute_disp}')")
        else:
            logger.warning(f"❌ Stats failed: {r.status_code}")
    except Exception as e:
        logger.error(f"fetch_and_forward_statistics error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# LIVE POLLER
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_game(fixture: dict, col, history_col):
    sportscore_id = fixture.get("sportscore_id")
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    match_id = fixture["match_id"]

    if not sportscore_id:
        logger.error(f"❌ No sportscore_id for {label}")
        return

    # Check if already finished
    initial_data = sportscore_get(f"soccer/fixtures/{sportscore_id}")
    initial = parse_live_feed(initial_data)
    if initial and initial["status"] == "completed":
        logger.info(f"⏭  {label} already completed")
        update_fixture_status(match_id, "completed")
        update_db_status(col, match_id, "completed")
        move_completed_game_to_history(col, history_col, match_id)
        return

    update_fixture_status(match_id, "live")
    update_db_status(col, match_id, "live")
    logger.info(f"🔴 LIVE POLLING: {label}")

    last_home = 0
    last_away = 0
    half_time_sent = False
    full_time_sent = False
    second_half_sent = False
    seen_incidents: set = set()
    poll_count = 0

    while True:
        # Get live data
        live_data_raw = sportscore_get(f"soccer/fixtures/{sportscore_id}")
        live = parse_live_feed(live_data_raw)
        
        if not live:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        # Get incidents
        incidents_raw = sportscore_get(f"soccer/fixtures/{sportscore_id}/events")
        incidents = parse_incidents(incidents_raw)

        home_score = live["home_score"]
        away_score = live["away_score"]
        status = live["status"]
        time_elapsed = live["time_elapsed"]
        time_extra = live.get("time_extra", 0)
        minute_disp = f"{time_elapsed}" + (f"+{time_extra}" if time_extra else "")

        # Goals
        if home_score > last_home:
            logger.info(f"⚽ GOAL {fixture['home_team']} — {home_score}-{away_score} ({minute_disp}')")
            forward_event(fixture, "goal", {
                "minute": time_elapsed, "minute_display": minute_disp,
                "home_score": home_score, "away_score": away_score,
                "team": fixture["home_team"],
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": minute_disp,
                "text": f"⚽ GOAL! {fixture['home_team']} scores! ({home_score}-{away_score})",
                "event_type": "goal", "home_score": home_score, "away_score": away_score,
                "team": fixture["home_team"],
            })
            last_home = home_score

        if away_score > last_away:
            logger.info(f"⚽ GOAL {fixture['away_team']} — {home_score}-{away_score} ({minute_disp}')")
            forward_event(fixture, "goal", {
                "minute": time_elapsed, "minute_display": minute_disp,
                "home_score": home_score, "away_score": away_score,
                "team": fixture["away_team"],
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": minute_disp,
                "text": f"⚽ GOAL! {fixture['away_team']} scores! ({home_score}-{away_score})",
                "event_type": "goal", "home_score": home_score, "away_score": away_score,
                "team": fixture["away_team"],
            })
            last_away = away_score

        # Other incidents
        for inc in incidents:
            inc_id = inc.get("id", "")
            if inc_id in seen_incidents:
                continue
            seen_incidents.add(inc_id)

            inc_type = inc["type"]
            is_home = inc.get("is_home", False)
            team = fixture["home_team"] if is_home else fixture["away_team"]
            minute = inc.get("minute", time_elapsed)
            extra = inc.get("extra", 0)
            m_disp = f"{minute}" + (f"+{extra}" if extra else "")
            player = inc.get("player", "Unknown")

            if inc_type == "YC":
                logger.info(f"🟨 YELLOW — {team}: {player} ({m_disp}')")
                forward_event(fixture, "yellow_card", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })
                send_commentary(fixture, {
                    "minute": minute, "minute_display": m_disp,
                    "text": f"🟨 YELLOW CARD - {player} ({team})",
                    "event_type": "yellow_card", "home_score": home_score, "away_score": away_score,
                    "team": team, "player": player,
                })

            elif inc_type == "RC":
                logger.info(f"🟥 RED — {team}: {player} ({m_disp}')")
                forward_event(fixture, "red_card", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })
                send_commentary(fixture, {
                    "minute": minute, "minute_display": m_disp,
                    "text": f"🟥 RED CARD - {player} ({team})",
                    "event_type": "red_card", "home_score": home_score, "away_score": away_score,
                    "team": team, "player": player,
                })

            elif inc_type == "SB":
                p_out = inc.get("assist", "Unknown")
                logger.info(f"🔄 SUB — {team}: {p_out} → {player} ({m_disp}')")
                forward_event(fixture, "substitution", {
                    "minute": minute, "minute_display": m_disp,
                    "player_out": p_out, "player_in": player, "team": team,
                })
                send_commentary(fixture, {
                    "minute": minute, "minute_display": m_disp,
                    "text": f"🔄 SUB: {p_out} → {player} ({team})",
                    "event_type": "substitution", "home_score": home_score, "away_score": away_score,
                    "team": team,
                })

        # Half time
        if time_elapsed >= 45 and time_elapsed < 50 and not half_time_sent:
            logger.info(f"⏸  HALF TIME: {home_score}–{away_score}")
            forward_event(fixture, "half_time", {
                "minute": 45, "minute_display": "45'",
                "home_score": home_score, "away_score": away_score,
            })
            send_commentary(fixture, {
                "minute": 45, "minute_display": "45'",
                "text": f"⏸ HALF TIME: {fixture['home_team']} {home_score}–{away_score} {fixture['away_team']}",
                "event_type": "half_time", "home_score": home_score, "away_score": away_score,
            })
            update_db_status(col, match_id, "live", {"time_elapsed": time_elapsed, "half": 1})
            fetch_and_forward_statistics(fixture, live)
            half_time_sent = True

        # Second half
        if time_elapsed >= 50 and half_time_sent and not second_half_sent:
            logger.info("▶️  SECOND HALF STARTED")
            forward_event(fixture, "second_half", {"minute": 45, "minute_display": "45'"})
            send_commentary(fixture, {
                "minute": 45, "minute_display": "45'",
                "text": f"▶️ SECOND HALF UNDERWAY! {fixture['home_team']} {home_score}–{away_score} {fixture['away_team']}",
                "event_type": "second_half", "home_score": home_score, "away_score": away_score,
            })
            update_db_status(col, match_id, "live", {"half": 2})
            second_half_sent = True

        # Full time
        if status == "completed" and not full_time_sent:
            logger.info(f"🏁 FULL TIME: {label} — {home_score}–{away_score}")
            forward_event(fixture, "match_end", {
                "minute": time_elapsed, "minute_display": minute_disp,
                "home_score": home_score, "away_score": away_score,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": minute_disp,
                "text": f"🏁 FULL TIME: {fixture['home_team']} {home_score}–{away_score} {fixture['away_team']}",
                "event_type": "full_time", "home_score": home_score, "away_score": away_score,
            })
            update_fixture_status(match_id, "completed")
            update_db_status(col, match_id, "completed", {
                "home_score": home_score, "away_score": away_score, "time_elapsed": time_elapsed,
            })
            fetch_and_forward_statistics(fixture, live)
            move_completed_game_to_history(col, history_col, match_id)
            full_time_sent = True
            break

        poll_count += 1
        if poll_count % 5 == 0:
            logger.info(f"📊 Stats snapshot at {minute_disp}' for {label}")
            fetch_and_forward_statistics(fixture, live)

        time.sleep(POLL_INTERVAL_SEC)

    logger.info(f"✅ Done polling {label}")


# ─────────────────────────────────────────────────────────────────────────────
# POLL QUEUE
# ─────────────────────────────────────────────────────────────────────────────

_poll_queue: _queue.Queue = _queue.Queue()
_queue_worker_started: bool = False
_queue_lock: threading.Lock = threading.Lock()


def _queue_worker():
    logger.info("🔁 Poll queue worker started")
    while True:
        try:
            task = _poll_queue.get(timeout=5)
            if task is None:
                break
            fixture, col, history_col = task
            match_id = fixture["match_id"]
            label = f"{fixture['home_team']} vs {fixture['away_team']}"

            with polls_lock:
                if match_id in active_polls:
                    _poll_queue.task_done()
                    continue
                active_polls.add(match_id)

            try:
                poll_live_game(fixture, col, history_col)
            except Exception as e:
                logger.error(f"Poll error for {label}: {e}")
            finally:
                with polls_lock:
                    active_polls.discard(match_id)
                _poll_queue.task_done()

        except _queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Queue worker error: {e}")


def _ensure_queue_worker():
    global _queue_worker_started
    with _queue_lock:
        if not _queue_worker_started:
            threading.Thread(target=_queue_worker, daemon=True, name="wc-poll-worker").start()
            _queue_worker_started = True


def start_polling_for_game(fixture: dict, col, history_col):
    match_id = fixture["match_id"]
    with polls_lock:
        if match_id in active_polls:
            return
    _ensure_queue_worker()
    _poll_queue.put((fixture, col, history_col))
    logger.info(f"📥 Queued: {fixture['home_team']} vs {fixture['away_team']}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash — World Cup 2026 Sportscore Poller")
    logger.info("=" * 65)

    start_health_server()
    mongo_client, col = connect_db()
    history_col = get_history_collection(mongo_client)

    cleanup_all_completed_games(col, history_col)

    existing = load_fixtures_from_db(col)
    if existing:
        logger.info(f"📦 {len(existing)} fixture(s) in DB — skipping startup scrape")
        last_scrape_time = time.time()
    else:
        logger.info("📭 DB empty — running initial scrape...")
        run_scraper(col)
        last_scrape_time = time.time()

    last_cleanup_time = time.time()
    lineups_fetched_set: set = set()

    while True:
        try:
            now_utc = datetime.now(timezone.utc)

            if time.time() - last_scrape_time >= SCRAPE_INTERVAL_SEC:
                logger.info("\n🔄 6-hour rescrape starting...")
                run_scraper(col)
                last_scrape_time = time.time()
                lineups_fetched_set.clear()

            if time.time() - last_cleanup_time >= CLEANUP_INTERVAL_SEC:
                cleanup_all_completed_games(col, history_col)
                last_cleanup_time = time.time()

            fixtures = load_fixtures_from_db(col)
            if not fixtures:
                logger.warning("📭 No fixtures in DB — scraping now")
                run_scraper(col)
                last_scrape_time = time.time()
                fixtures = load_fixtures_from_db(col)

            # Live games
            live_fixtures = get_live_fixtures(fixtures)
            if live_fixtures:
                logger.info(f"\n🔴 {len(live_fixtures)} LIVE GAME(S)")
                for lf in live_fixtures:
                    mid = lf["match_id"]
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

            # Upcoming games
            upcoming = []
            for f in fixtures:
                ko = f.get("_kickoff_utc")
                if not ko or f.get("status") == "completed":
                    continue
                mins = (ko - now_utc).total_seconds() / 60
                if 0 < mins <= 1440:
                    upcoming.append((mins, f))

            if not upcoming:
                logger.info("📭 No fixtures in next 24h — sleeping 1h then rescraping")
                time.sleep(3600)
                run_scraper(col)
                last_scrape_time = time.time()
                continue

            upcoming.sort(key=lambda x: x[0])

            logger.info(f"📅 {len(upcoming)} fixture(s) in next 24h:")
            for mins, f in upcoming:
                ko_local = (f["_kickoff_utc"] + NAIROBI_OFFSET).strftime("%H:%M")
                icon = "🔴" if f.get("status") == "soon" else "⏳"
                logger.info(f"   {icon} {f['home_team']} vs {f['away_team']} at {ko_local} EAT ({int(mins)} mins)")

            for mins_to_game, fixture in upcoming:
                mid = fixture["match_id"]
                label = f"{fixture['home_team']} vs {fixture['away_team']}"

                if 0 < mins_to_game <= 60:
                    if fixture.get("status") != "soon":
                        logger.info(f"⏰ {label} — {int(mins_to_game)} mins — setting SOON")
                        update_fixture_status(mid, "soon")
                        update_db_status(col, mid, "soon")

                    if mid not in lineups_fetched_set and not fixture.get("_lineups_fetched"):
                        if not check_lineups_exist_in_backend(mid):
                            if fetch_and_forward_lineups(fixture, col):
                                lineups_fetched_set.add(mid)
                        else:
                            lineups_fetched_set.add(mid)
                            logger.info(f"   Lineups already in backend for {label}")

                elif mins_to_game <= 1440:
                    if fixture.get("status") not in ("upcoming", "soon"):
                        update_db_status(col, mid, "upcoming")

            closest_mins, closest_fixture = upcoming[0]
            if 0 < closest_mins <= 5:
                logger.info(f"⚽ {closest_fixture['home_team']} vs {closest_fixture['away_team']} in {int(closest_mins)} mins")
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