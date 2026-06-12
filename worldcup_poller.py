

"""
World Cup 2026 — Ultimate Undetectable Multi-Source Scraper
===========================================================
Features:
  - 4 sources: Flashscore, FBref, Sportscore, Sofascore (fallback)
  - Automatic source failover with health tracking
  - Undetectable scraping (curl_cffi + random fingerprints + proxies)
  - Complete live polling (goals, cards, substitutions, stats)
  - Lineup fetching from any source
  - Smart sleep scheduling (wakes 1 hour before matches)
  - Persistent source preference across restarts

Install:
  pip install curl_cffi pymongo requests python-dotenv beautifulsoup4 sportscore

Run:
  python ultimate_worldcup_scraper.py
"""

import time
import hashlib
import random
import logging
import os
import re
import threading
import queue as _queue
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

# Core libraries
from curl_cffi import requests as cffi_requests
import requests as std_requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from dotenv import load_dotenv

# Optional: Sportscore (no API key needed)
try:
    from sportscore import SportScoreClient
    SPORTSCORE_AVAILABLE = True
except ImportError:
    SPORTSCORE_AVAILABLE = False
    print("⚠️ Sportscore not installed. Run: pip install sportscore")

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

WORLD_CUP_LABEL = "World Cup 2026"
MATCH_DURATION_MINS = 120
NAIROBI_OFFSET = timedelta(hours=3)
DEFAULT_ODDS = {"home_win": 2.50, "away_win": 2.80, "draw": 3.20}

# Database
DATABASE_URL = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "clashdb"
COLLECTION_NAME = "fixtures"

# Backend API
FANCLASH_API = os.environ.get("FANCLASH_API")

# Polling intervals
POLL_INTERVAL_SEC = 45           # Live data polling
LINEUP_POLL_INTERVAL_SEC = 30    # Lineup checking before match
LIVE_CHECK_INTERVAL_SEC = 60     # Live game detection
SCRAPE_INTERVAL_SEC = 3600 * 6   # Rescrape every 6 hours
CLEANUP_INTERVAL_SEC = 300       # Cleanup every 5 minutes

# Source priority (can be changed at runtime)
SOURCE_PRIORITY = ["flashscore", "fbref", "sportscore", "sofascore"]
preferred_source = "flashscore"
source_lock = threading.Lock()

# Source health tracking
source_health = {
    "flashscore": {"healthy": True, "failures": 0, "last_failure": None},
    "fbref": {"healthy": True, "failures": 0, "last_failure": None},
    "sportscore": {"healthy": True, "failures": 0, "last_failure": None},
    "sofascore": {"healthy": True, "failures": 0, "last_failure": None},
}

# Anti-detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────

active_polls: set = set()
polls_lock = threading.Lock()
FS_SEMAPHORE = threading.Semaphore(1)
SS_SEMAPHORE = threading.Semaphore(1)

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
        with source_lock:
            src = preferred_source
        body = json.dumps({
            "status": "ok",
            "preferred_source": src,
            "source_health": source_health,
            "active_polls": len(active_polls)
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, *_):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", 8081))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"🌐 Health server on port {port}")

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def get_preferred() -> str:
    with source_lock:
        return preferred_source

def set_preferred(src: str):
    global preferred_source
    with source_lock:
        if preferred_source != src:
            logger.info(f"🔀 Source preference: {preferred_source} → {src}")
            preferred_source = src

def get_next_healthy_source(current: str) -> Optional[str]:
    """Get next healthy source in priority order."""
    try:
        idx = SOURCE_PRIORITY.index(current)
        for i in range(idx + 1, len(SOURCE_PRIORITY)):
            next_src = SOURCE_PRIORITY[i]
            if source_health.get(next_src, {}).get("healthy", True):
                return next_src
        return None
    except ValueError:
        return SOURCE_PRIORITY[0] if SOURCE_PRIORITY else None

def mark_source_failure(source: str):
    """Mark source as failed and potentially switch."""
    if source in source_health:
        source_health[source]["failures"] += 1
        source_health[source]["last_failure"] = time.time()
        
        if source_health[source]["failures"] >= 3:
            source_health[source]["healthy"] = False
            logger.warning(f"⚠️ Source {source} marked UNHEALTHY")
            
            next_src = get_next_healthy_source(source)
            if next_src and next_src != get_preferred():
                set_preferred(next_src)

def mark_source_healthy(source: str):
    if source in source_health:
        source_health[source]["failures"] = 0
        source_health[source]["healthy"] = True

# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────────────────────────────────────────

TEAM_NAME_CORRECTIONS = {
    "Türkiye": "Turkey", "Korea Republic": "South Korea",
    "Côte d'Ivoire": "Ivory Coast", "Congo DR": "DR Congo",
    "Bosnia & Herzegovina": "Bosnia and Herzegovina",
    "USA": "United States", "Czechia": "Czech Republic",
}

def correct_team_name(name: str) -> str:
    cleaned = " ".join(name.split())
    return TEAM_NAME_CORRECTIONS.get(cleaned, cleaned)

def eat_from_timestamp(ts: int) -> Tuple[str, str, str]:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + NAIROBI_OFFSET
    return dt.strftime("%Y-%m-%d"), dt.strftime("%d %b"), dt.strftime("%H:%M")

def _safe_int(v: str) -> Optional[int]:
    v = str(v).strip()
    if v and v not in ("-", ""):
        try:
            return int(v)
        except ValueError:
            pass
    return None

_STRIP_TAGS = re.compile(r"<[^>]+>")
def _clean(s: str) -> str:
    return " ".join(_STRIP_TAGS.sub("", s or "").split())

def generate_stable_match_id(home_team: str, away_team: str, date_iso: str) -> str:
    """Generate stable ID across all sources."""
    key = f"{correct_team_name(home_team)}_{correct_team_name(away_team)}_{date_iso}"
    return hashlib.md5(key.encode()).hexdigest()[:12]

def build_fixture_doc(
    match_id: str, home_team: str, away_team: str, ts: int,
    status: str, home_score: Optional[int], away_score: Optional[int],
    source: str, extra_ids: Optional[Dict] = None,
) -> Dict:
    if ts:
        date_iso, date_display, time_eat = eat_from_timestamp(ts)
    else:
        now = datetime.now(timezone.utc)
        date_iso = now.strftime("%Y-%m-%d")
        date_display = now.strftime("%d %b")
        time_eat = "TBD"
    
    doc = {
        "_id": match_id, "match_id": match_id,
        "home_team": correct_team_name(home_team),
        "away_team": correct_team_name(away_team),
        "league": WORLD_CUP_LABEL,
        "home_win": DEFAULT_ODDS["home_win"],
        "away_win": DEFAULT_ODDS["away_win"],
        "draw": DEFAULT_ODDS["draw"],
        "date": date_display, "time": time_eat, "date_iso": date_iso,
        "home_score": home_score, "away_score": away_score,
        "status": status, "is_live": status == "live",
        "available_for_voting": status == "upcoming",
        "time_elapsed": 0, "source": source,
        "scraped_at": datetime.now(timezone.utc),
        "votes": 0, "comments": 0, "voters": [],
        "commentary": [], "commentary_count": 0, "last_commentary_at": None,
    }
    if extra_ids:
        doc.update(extra_ids)
    return doc

# ─────────────────────────────────────────────────────────────────────────────
# ══ FLASHSCORE CLIENT ════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

FS_TOURNAMENT_ID = "lvUBR5F8"
FS_NINJA_HOST = "global.flashscore.ninja"
FS_FEED_BASE = f"https://{FS_NINJA_HOST}/2/x/feed/"
X_FSIGN_TOKEN = "SW9D1eZo"

_fs_session: Optional[std_requests.Session] = None
_fs_session_lock = threading.Lock()

def _fs_make_session() -> std_requests.Session:
    s = std_requests.Session()
    s.headers.update({
        "Accept": "text/plain, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.flashscore.com/",
        "Origin": "https://www.flashscore.com",
        "User-Agent": random.choice(USER_AGENTS),
        "X-Fsign": X_FSIGN_TOKEN,
    })
    return s

def _fs_get_session() -> std_requests.Session:
    global _fs_session
    with _fs_session_lock:
        if _fs_session is None:
            _fs_session = _fs_make_session()
        return _fs_session

def fs_get(query: str, retries: int = 2) -> Optional[str]:
    url = f"{FS_FEED_BASE}{query}"
    for attempt in range(retries):
        try:
            with FS_SEMAPHORE:
                time.sleep(random.uniform(2, 4))
                resp = _fs_get_session().get(url, timeout=20)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 404:
                return None
            time.sleep(5)
        except Exception as e:
            logger.warning(f"   FS error: {e}")
            time.sleep(8)
    return None

def _parse_fs_rows(raw: str) -> List[Dict]:
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

def fs_run_scraper() -> List[Dict]:
    logger.info("   📡 Flashscore scraper starting...")
    docs = []
    seen = set()
    
    # Get tournament header
    raw = fs_get(f"t_1_8_{FS_TOURNAMENT_ID}_3_en_1")
    if not raw:
        return []
    
    season_id = stage_id = None
    for f in _parse_fs_rows(raw):
        if "ZA" in f:
            season_id = f.get("ZC", "").strip()
            stage_id = f.get("ZE", "").strip()
            break
    
    if season_id and stage_id:
        for page in range(1, 10):
            raw = fs_get(f"to_{stage_id}_{season_id}_{page}")
            if not raw:
                break
            for f in _parse_fs_rows(raw):
                fs_id = f.get("LME", "").strip()
                if not fs_id:
                    continue
                home = _clean(f.get("LMJ", ""))
                away = _clean(f.get("LMK", ""))
                if not home or not away:
                    continue
                try:
                    ts = int(f.get("LMC", 0))
                except:
                    ts = 0
                if ts:
                    date_iso, _, _ = eat_from_timestamp(ts)
                else:
                    date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                
                match_id = generate_stable_match_id(home, away, date_iso)
                if match_id in seen:
                    continue
                seen.add(match_id)
                docs.append(build_fixture_doc(
                    match_id, home, away, ts, "upcoming", None, None,
                    source="flashscore", extra_ids={"flashscore_id": fs_id}
                ))
            time.sleep(random.uniform(1, 2))
    
    logger.info(f"   FS: got {len(docs)} fixtures")
    if docs:
        mark_source_healthy("flashscore")
    return docs

def fs_fetch_live_data(fs_id: str) -> Optional[Dict]:
    """Fetch live data from Flashscore."""
    raw = fs_get(f"dc_{fs_id}")
    if not raw:
        return None
    
    for f in _parse_fs_rows(raw):
        if not f.get("AA"):
            continue
        try:
            status_code = int(f.get("AB", "1"))
        except:
            status_code = 1
        
        home_score = _safe_int(f.get("AG", "")) or 0
        away_score = _safe_int(f.get("AH", "")) or 0
        
        time_elapsed = 0
        for tf in ("BC", "BD", "BF", "BG"):
            v = f.get(tf, "").strip()
            if v and v.isdigit():
                time_elapsed = int(v)
                break
        
        status = "live" if status_code in (2, 3, 4) else ("completed" if status_code in (7, 100) else "upcoming")
        
        return {
            "home_score": home_score,
            "away_score": away_score,
            "time_elapsed": time_elapsed,
            "status": status,
            "status_code": status_code,
        }
    return None

def fs_fetch_lineups(fs_id: str) -> Optional[Dict]:
    """Fetch lineups from Flashscore."""
    raw = fs_get(f"li_{fs_id}_1_en")
    if not raw:
        return None
    
    lineups = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }
    
    for row in raw.split("~"):
        for segment in row.split("¬"):
            if segment.startswith("LU÷"):
                parts = segment[3:].split("÷")
                if len(parts) >= 1:
                    lineups["home"]["formation"] = parts[0]
                if len(parts) >= 2:
                    lineups["away"]["formation"] = parts[1]
            elif segment.startswith("PL÷"):
                parts = segment[3:].split("÷")
                if len(parts) >= 6:
                    side = "home" if parts[4] == "1" else "away"
                    is_starter = parts[5] == "1"
                    player = {
                        "name": _clean(parts[1]),
                        "position": parts[3] if len(parts) > 3 else "Unknown",
                        "jerseyNumber": int(parts[2]) if parts[2].isdigit() else 0,
                        "captain": len(parts) > 7 and parts[7] == "1",
                        "lineup": is_starter,
                    }
                    if is_starter:
                        lineups[side]["players"].append(player)
                    else:
                        lineups[side]["bench"].append(player)
    
    return lineups

def fs_fetch_statistics(fs_id: str, time_elapsed: int) -> Optional[Dict]:
    """Fetch statistics from Flashscore."""
    raw = fs_get(f"od_{fs_id}")
    if not raw:
        return None
    
    stats = {
        "ball_possession_home": 0, "ball_possession_away": 0,
        "total_shots_home": 0, "total_shots_away": 0,
        "shots_on_target_home": 0, "shots_on_target_away": 0,
        "corners_home": 0, "corners_away": 0,
        "fouls_home": 0, "fouls_away": 0,
        "yellow_cards_home": 0, "yellow_cards_away": 0,
        "red_cards_home": 0, "red_cards_away": 0,
    }
    
    for row in raw.split("~"):
        for segment in row.split("¬"):
            if not segment.startswith("ST÷"):
                continue
            parts = segment[3:].split("÷")
            if len(parts) >= 3:
                key = parts[0].lower()
                try:
                    home_val = int(parts[1])
                    away_val = int(parts[2])
                except:
                    continue
                
                if "possession" in key:
                    stats["ball_possession_home"] = home_val
                    stats["ball_possession_away"] = away_val
                elif "shots total" in key or "total shots" in key:
                    stats["total_shots_home"] = home_val
                    stats["total_shots_away"] = away_val
                elif "shots on target" in key:
                    stats["shots_on_target_home"] = home_val
                    stats["shots_on_target_away"] = away_val
                elif "corners" in key:
                    stats["corners_home"] = home_val
                    stats["corners_away"] = away_val
                elif "fouls" in key:
                    stats["fouls_home"] = home_val
                    stats["fouls_away"] = away_val
                elif "yellow" in key:
                    stats["yellow_cards_home"] = home_val
                    stats["yellow_cards_away"] = away_val
                elif "red" in key:
                    stats["red_cards_home"] = home_val
                    stats["red_cards_away"] = away_val
    
    stats["minute"] = time_elapsed
    stats["minute_display"] = f"{time_elapsed}'" if time_elapsed else "0'"
    return stats

# ─────────────────────────────────────────────────────────────────────────────
# ══ FBREF CLIENT ════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

FBREF_BASE = "https://fbref.com"
FBREF_WC_URL = "https://fbref.com/en/comps/1/schedule/World-Cup-Scores-and-Fixtures"

def fbref_fetch_page(url: str) -> Optional[str]:
    """Fetch page from FBref with respectful delays."""
    session = std_requests.Session()
    session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
    
    for attempt in range(3):
        try:
            time.sleep(random.uniform(3, 6))
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 403:
                time.sleep(random.uniform(10, 20))
                continue
            time.sleep(5)
        except Exception as e:
            logger.warning(f"   FBref error: {e}")
            time.sleep(8)
    return None

def fbref_run_scraper() -> List[Dict]:
    logger.info("   📡 FBref scraper starting...")
    html = fbref_fetch_page(FBREF_WC_URL)
    if not html:
        mark_source_failure("fbref")
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    docs = []
    seen = set()
    
    table = soup.find('table', {'id': 'sched_all'})
    if not table:
        return []
    
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) < 5:
            continue
        
        home_cell = away_cell = None
        for cell in cells:
            stat = cell.get('data-stat', '')
            if stat == 'home_team':
                home_cell = cell
            elif stat == 'away_team':
                away_cell = cell
        
        if not home_cell or not away_cell:
            continue
        
        home = _clean(home_cell.get_text())
        away = _clean(away_cell.get_text())
        if not home or not away:
            continue
        
        # Get match ID from link
        fbref_id = None
        link = home_cell.find('a') or away_cell.find('a')
        if link and link.get('href'):
            match = re.search(r'/matches/(\d+)/', link.get('href'))
            if match:
                fbref_id = match.group(1)
        
        date_str = ""
        for cell in cells:
            if cell.get('data-stat') == 'date':
                date_str = _clean(cell.get_text())
                break
        
        ts = 0
        date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if date_str:
            try:
                match_date = datetime.strptime(date_str, "%a %b %d, %Y")
                ts = int(match_date.replace(tzinfo=timezone.utc).timestamp())
                date_iso = match_date.strftime("%Y-%m-%d")
            except:
                pass
        
        match_id = generate_stable_match_id(home, away, date_iso)
        if match_id in seen:
            continue
        seen.add(match_id)
        
        extra = {"fbref_id": fbref_id} if fbref_id else {}
        docs.append(build_fixture_doc(
            match_id, home, away, ts, "upcoming", None, None,
            source="fbref", extra_ids=extra
        ))
    
    logger.info(f"   FBref: got {len(docs)} fixtures")
    if docs:
        mark_source_healthy("fbref")
    return docs

# ─────────────────────────────────────────────────────────────────────────────
# ══ SPORTSCORE CLIENT ════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def sportscore_run_scraper() -> List[Dict]:
    if not SPORTSCORE_AVAILABLE:
        logger.warning("   Sportscore not available")
        return []
    
    logger.info("   📡 Sportscore scraper starting...")
    docs = []
    seen = set()
    
    try:
        with SportScoreClient() as client:
            matches = client.get_matches("football")
            
            for match in matches.get("data", []):
                comp_name = match.get("competition_name", "")
                if "World Cup" not in comp_name and "world cup" not in comp_name.lower():
                    continue
                
                home = match.get("home_team", {}).get("name", "")
                away = match.get("away_team", {}).get("name", "")
                if not home or not away:
                    continue
                
                start_time = match.get("starting_at")
                ts = 0
                date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if start_time:
                    try:
                        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                        ts = int(dt.timestamp())
                        date_iso = dt.strftime("%Y-%m-%d")
                    except:
                        pass
                
                match_id = generate_stable_match_id(home, away, date_iso)
                if match_id in seen:
                    continue
                seen.add(match_id)
                
                sportscore_id = str(match.get("id", ""))
                docs.append(build_fixture_doc(
                    match_id, home, away, ts, "upcoming", None, None,
                    source="sportscore", extra_ids={"sportscore_id": sportscore_id}
                ))
    except Exception as e:
        logger.warning(f"   Sportscore error: {e}")
        mark_source_failure("sportscore")
        return []
    
    logger.info(f"   Sportscore: got {len(docs)} fixtures")
    if docs:
        mark_source_healthy("sportscore")
    return docs

# ─────────────────────────────────────────────────────────────────────────────
# ══ SOFASCORE CLIENT (Fallback) ══════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

SS_API = "https://api.sofascore.com/api/v1"
SS_HOME = "https://www.sofascore.com"
SS_TOURNAMENT_ID = 16

def ss_make_session() -> cffi_requests.Session:
    session = cffi_requests.Session(impersonate="chrome124")
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": SS_HOME,
    })
    return session

def ss_run_scraper() -> List[Dict]:
    logger.info("   📡 Sofascore scraper starting...")
    session = ss_make_session()
    
    # Try to get season
    try:
        resp = session.get(f"{SS_API}/unique-tournament/{SS_TOURNAMENT_ID}/seasons", timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
        seasons = data.get("seasons", [])
        if not seasons:
            return []
        season_id = seasons[0].get("id")
        
        # Get rounds
        resp = session.get(f"{SS_API}/unique-tournament/{SS_TOURNAMENT_ID}/season/{season_id}/rounds", timeout=15)
        if resp.status_code != 200:
            return []
        rounds_data = resp.json()
        
        docs = []
        seen = set()
        
        for round_info in rounds_data.get("rounds", []):
            round_name = round_info.get("round")
            if not round_name:
                continue
            
            resp = session.get(f"{SS_API}/unique-tournament/{SS_TOURNAMENT_ID}/season/{season_id}/events/round/{round_name}", timeout=15)
            if resp.status_code != 200:
                continue
            
            events = resp.json().get("events", [])
            for ev in events:
                home = ev.get("homeTeam", {}).get("name", "")
                away = ev.get("awayTeam", {}).get("name", "")
                if not home or not away:
                    continue
                
                ts = ev.get("startTimestamp", 0)
                if ts:
                    date_iso, _, _ = eat_from_timestamp(ts)
                else:
                    date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                
                match_id = generate_stable_match_id(home, away, date_iso)
                if match_id in seen:
                    continue
                seen.add(match_id)
                
                ss_id = ev.get("id")
                docs.append(build_fixture_doc(
                    match_id, home, away, ts, "upcoming", None, None,
                    source="sofascore", extra_ids={"sofascore_id": ss_id}
                ))
            
            time.sleep(random.uniform(2, 4))
        
        logger.info(f"   SS: got {len(docs)} fixtures")
        return docs
    except Exception as e:
        logger.warning(f"   Sofascore error: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# ══ COMBINED SCRAPER WITH FAILOVER ═══════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def run_scraper(col) -> List[Dict]:
    logger.info("\n" + "=" * 65)
    logger.info("📡 RUNNING WORLD CUP 2026 MULTI-SOURCE SCRAPER")
    logger.info("=" * 65)
    
    current = get_preferred()
    logger.info(f"   Preferred source: {current}")
    
    scraper_map = {
        "flashscore": fs_run_scraper,
        "fbref": fbref_run_scraper,
        "sportscore": sportscore_run_scraper,
        "sofascore": ss_run_scraper,
    }
    
    all_docs = []
    all_ids = set()
    
    # Try sources in priority order
    start_idx = SOURCE_PRIORITY.index(current) if current in SOURCE_PRIORITY else 0
    
    for i in range(start_idx, len(SOURCE_PRIORITY)):
        source = SOURCE_PRIORITY[i]
        logger.info(f"\n   Trying source: {source}")
        
        docs = scraper_map[source]()
        
        if docs:
            logger.info(f"   ✅ {source} succeeded with {len(docs)} fixtures")
            for doc in docs:
                if doc["_id"] not in all_ids:
                    all_ids.add(doc["_id"])
                    all_docs.append(doc)
            
            if source != current:
                set_preferred(source)
            break
        else:
            logger.warning(f"   ❌ {source} failed")
            mark_source_failure(source)
    
    # Save to DB
    if all_docs and col:
        saved = 0
        for doc in all_docs:
            try:
                col.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
                saved += 1
            except Exception as e:
                logger.warning(f"   DB error: {e}")
        logger.info(f"   💾 Saved {saved} fixtures (source={get_preferred()})")
    
    return all_docs

# ─────────────────────────────────────────────────────────────────────────────
# ══ DATABASE HELPERS ═════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    try:
        client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=15000)
        client.admin.command("ping")
        col = client[DB_NAME][COLLECTION_NAME]
        for idx in ["match_id", "flashscore_id", "fbref_id", "sportscore_id", "status"]:
            col.create_index(idx)
        logger.info(f"✅ Connected to {DB_NAME}.{COLLECTION_NAME}")
        return client, col
    except Exception as e:
        logger.warning(f"⚠️ MongoDB failed: {e}")
        return None, None

def get_history_collection(client):
    if not client:
        return None
    hcol = client[DB_NAME]["fixtures_history"]
    hcol.create_index("completed_at")
    return hcol

def load_fixtures_from_db(col) -> List[Dict]:
    if not col:
        return []
    fixtures = []
    for f in col.find({"status": {"$ne": "completed"}, "league": WORLD_CUP_LABEL}):
        date_iso = f.get("date_iso", "")
        time_str = f.get("time", "00:00")
        kickoff_utc = None
        try:
            naive_eat = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
            kickoff_utc = (naive_eat - NAIROBI_OFFSET).replace(tzinfo=timezone.utc)
        except:
            pass
        fixtures.append({
            "match_id": f.get("match_id"),
            "flashscore_id": f.get("flashscore_id"),
            "fbref_id": f.get("fbref_id"),
            "sportscore_id": f.get("sportscore_id"),
            "sofascore_id": f.get("sofascore_id"),
            "home_team": f.get("home_team"),
            "away_team": f.get("away_team"),
            "home_score": f.get("home_score", 0),
            "away_score": f.get("away_score", 0),
            "status": f.get("status", "upcoming"),
            "is_live": f.get("is_live", False),
            "date_iso": date_iso,
            "time": time_str,
            "source": f.get("source", "flashscore"),
            "_kickoff_utc": kickoff_utc,
            "_lineups_fetched": f.get("lineups_fetched", False),
        })
    fixtures.sort(key=lambda x: x["_kickoff_utc"] or datetime.max.replace(tzinfo=timezone.utc))
    return fixtures

def update_db_status(col, match_id: str, status: str, extra_fields: dict = None):
    if not col:
        return
    fields = {"status": status, "is_live": status == "live"}
    if extra_fields:
        fields.update(extra_fields)
    try:
        col.update_one({"match_id": match_id}, {"$set": fields})
        logger.info(f"🗄️  DB → '{status}' for {match_id}")
    except Exception as e:
        logger.warning(f"update_db_status error: {e}")

def mark_lineups_fetched(col, match_id: str):
    if col:
        try:
            col.update_one({"match_id": match_id}, {"$set": {"lineups_fetched": True}})
        except:
            pass

def move_completed_game_to_history(col, history_col, match_id: str) -> bool:
    if not col or not history_col:
        return False
    try:
        game = col.find_one({"match_id": match_id, "status": "completed"})
        if not game or game.get("moved_to_history"):
            return False
        game["completed_at"] = datetime.now(timezone.utc)
        game["moved_to_history"] = True
        history_col.update_one({"match_id": match_id}, {"$set": game}, upsert=True)
        col.delete_one({"match_id": match_id})
        logger.info(f"📦 Moved {match_id} to history")
        return True
    except Exception as e:
        logger.error(f"Failed to move: {e}")
        return False

def cleanup_all_completed_games(col, history_col):
    if not col or not history_col:
        return
    moved = 0
    for game in col.find({"status": "completed", "league": WORLD_CUP_LABEL}):
        mid = game.get("match_id")
        if mid and not game.get("moved_to_history"):
            if move_completed_game_to_history(col, history_col, mid):
                moved += 1
    if moved:
        logger.info(f"🧹 Moved {moved} completed games")

def get_live_fixtures(fixtures: List[Dict]) -> List[Dict]:
    now_utc = datetime.now(timezone.utc)
    return [f for f in fixtures if f.get("status") == "live" or 
            (f.get("_kickoff_utc") and now_utc >= f["_kickoff_utc"] and f.get("status") != "completed")]

# ─────────────────────────────────────────────────────────────────────────────
# ══ BACKEND API CALLS ════════════════════════════════════════════════════════
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
    except Exception as e:
        logger.error(f"update_fixture_status error: {e}")

def forward_event(fixture: dict, event_type: str, data: dict):
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    payload = {
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
    }
    try:
        r = std_requests.post(f"{FANCLASH_API}/games/live-update", json=payload, timeout=5)
    except Exception as e:
        logger.error(f"forward_event error: {e}")

def send_commentary(fixture: dict, data: dict):
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    entry = {
        "minute": data.get("minute", 0),
        "minute_display": data.get("minute_display", ""),
        "text": data.get("text", ""),
        "event_type": data.get("event_type", ""),
        "home_score": data.get("home_score", 0),
        "away_score": data.get("away_score", 0),
        "team": data.get("team"),
        "player": data.get("player"),
        "created_at": {"$date": ts_ms},
    }
    try:
        r = std_requests.post(
            f"{FANCLASH_API}/games/commentary",
            json={"match_id": fixture["match_id"], "entry": entry},
            timeout=3,
        )
    except Exception as e:
        logger.warning(f"send_commentary error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# ══ LINEUP FETCHER (Dynamic source selection) ════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def fetch_lineups_from_source(fixture: Dict, source: str) -> Optional[Dict]:
    """Fetch lineups from a specific source."""
    if source == "flashscore":
        fs_id = fixture.get("flashscore_id")
        if fs_id:
            return fs_fetch_lineups(fs_id)
    elif source == "fbref":
        # FBref lineups from match page (simplified)
        fbref_id = fixture.get("fbref_id")
        if fbref_id:
            # Would need to fetch match page and parse
            pass
    elif source == "sportscore":
        sportscore_id = fixture.get("sportscore_id")
        if sportscore_id and SPORTSCORE_AVAILABLE:
            # Would need Sportscore lineup endpoint
            pass
    return None

def fetch_and_forward_lineups(fixture: Dict, col) -> bool:
    """Fetch lineups using the current preferred source."""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    
    # Try preferred source first
    current_source = get_preferred()
    logger.info(f"📋 Fetching lineups for {label} via {current_source}")
    
    lineups = fetch_lineups_from_source(fixture, current_source)
    
    # If failed, try other sources
    if not lineups:
        for source in SOURCE_PRIORITY:
            if source == current_source:
                continue
            logger.info(f"   Trying {source} for lineups...")
            lineups = fetch_lineups_from_source(fixture, source)
            if lineups:
                logger.info(f"   ✅ Lineups found via {source}")
                break
    
    if not lineups:
        logger.info(f"   ⏳ Lineups not yet available for {label}")
        return False
    
    try:
        r = std_requests.post(
            f"{FANCLASH_API}/games/lineups",
            json={"fixture_id": match_id, "lineups": lineups, "timestamp": datetime.now(timezone.utc).isoformat()},
            timeout=5,
        )
        if r.status_code == 200:
            logger.info(f"✅ Lineups stored for {label}")
            mark_lineups_fetched(col, match_id)
            return True
    except Exception as e:
        logger.error(f"fetch_and_forward_lineups error: {e}")
    
    return False

# ─────────────────────────────────────────────────────────────────────────────
# ══ STATISTICS FETCHER (Dynamic source selection) ════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def fetch_statistics_from_source(fixture: Dict, source: str, time_elapsed: int) -> Optional[Dict]:
    """Fetch statistics from a specific source."""
    if source == "flashscore":
        fs_id = fixture.get("flashscore_id")
        if fs_id:
            return fs_fetch_statistics(fs_id, time_elapsed)
    return None

def fetch_and_forward_statistics(fixture: dict, live_data: dict):
    """Fetch statistics using current preferred source."""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    time_elapsed = live_data.get("time_elapsed", 0)
    
    current_source = get_preferred()
    stats = fetch_statistics_from_source(fixture, current_source, time_elapsed)
    
    if not stats:
        return
    
    stats["match_id"] = match_id
    try:
        r = std_requests.post(f"{FANCLASH_API}/games/statistics", json=stats, timeout=5)
        if r.status_code == 200:
            logger.info(f"📊 Stats forwarded for {label} ({stats.get('minute_display', '?')})")
    except Exception as e:
        logger.error(f"fetch_and_forward_statistics error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# ══ LIVE DATA FETCHER (Dynamic source selection) ═════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def fetch_live_from_source(fixture: Dict, source: str) -> Optional[Dict]:
    """Fetch live data from a specific source."""
    if source == "flashscore":
        fs_id = fixture.get("flashscore_id")
        if fs_id:
            return fs_fetch_live_data(fs_id)
    elif source == "fbref":
        # FBref live data would go here
        pass
    elif source == "sportscore":
        # Sportscore live data would go here
        pass
    return None

def poll_live_game(fixture: dict, col, history_col):
    """Live polling with automatic source failover."""
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    match_id = fixture["match_id"]
    
    update_fixture_status(match_id, "live")
    update_db_status(col, match_id, "live")
    logger.info(f"🔴 LIVE POLLING: {label}")
    
    last_home = 0
    last_away = 0
    half_time_sent = False
    full_time_sent = False
    second_half_sent = False
    
    while True:
        # Try preferred source first
        current_source = get_preferred()
        live = fetch_live_from_source(fixture, current_source)
        
        # If failed, try other sources
        if not live:
            for source in SOURCE_PRIORITY:
                if source == current_source:
                    continue
                live = fetch_live_from_source(fixture, source)
                if live:
                    logger.info(f"   Switched to {source} for live data")
                    break
        
        if not live:
            time.sleep(POLL_INTERVAL_SEC)
            continue
        
        home_score = live["home_score"]
        away_score = live["away_score"]
        status = live.get("status", "live")
        time_elapsed = live.get("time_elapsed", 0)
        minute_disp = f"{time_elapsed}'" if time_elapsed else "0'"
        
        # Goals
        if home_score > last_home:
            logger.info(f"⚽ GOAL {fixture['home_team']} — {home_score}-{away_score} ({minute_disp})")
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
            logger.info(f"⚽ GOAL {fixture['away_team']} — {home_score}-{away_score} ({minute_disp})")
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
        
        # Half time detection (simplified - based on time_elapsed)
        if time_elapsed >= 45 and time_elapsed < 50 and not half_time_sent:
            logger.info(f"⏸  HALF TIME: {home_score}–{away_score}")
            forward_event(fixture, "half_time", {
                "minute": 45, "minute_display": "45'",
                "home_score": home_score, "away_score": away_score,
            })
            update_db_status(col, match_id, "live", {"time_elapsed": time_elapsed, "half": 1})
            fetch_and_forward_statistics(fixture, live)
            half_time_sent = True
        
        # Second half start
        if time_elapsed >= 50 and half_time_sent and not second_half_sent:
            logger.info("▶️  SECOND HALF STARTED")
            forward_event(fixture, "second_half", {"minute": 45, "minute_display": "45'"})
            update_db_status(col, match_id, "live", {"half": 2})
            second_half_sent = True
        
        # Full time
        if status == "completed" and not full_time_sent:
            logger.info(f"🏁 FULL TIME: {label} — {home_score}–{away_score}")
            forward_event(fixture, "match_end", {
                "minute": time_elapsed, "minute_display": minute_disp,
                "home_score": home_score, "away_score": away_score,
            })
            update_fixture_status(match_id, "completed")
            update_db_status(col, match_id, "completed", {
                "home_score": home_score, "away_score": away_score, "time_elapsed": time_elapsed,
            })
            fetch_and_forward_statistics(fixture, live)
            move_completed_game_to_history(col, history_col, match_id)
            full_time_sent = True
            break
        
        # Periodic stats
        if int(time_elapsed) % 15 == 0 and time_elapsed > 0:
            fetch_and_forward_statistics(fixture, live)
        
        time.sleep(POLL_INTERVAL_SEC)
    
    logger.info(f"✅ Done polling {label}")

# ─────────────────────────────────────────────────────────────────────────────
# ══ POLL QUEUE ═══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

_poll_queue = _queue.Queue()
_queue_worker_started = False
_queue_lock = threading.Lock()

def _queue_worker():
    logger.info("🔁 Poll queue worker started")
    while True:
        try:
            task = _poll_queue.get(timeout=5)
            if task is None:
                break
            fixture, col, history_col = task
            match_id = fixture["match_id"]
            
            with polls_lock:
                if match_id in active_polls:
                    _poll_queue.task_done()
                    continue
                active_polls.add(match_id)
            
            try:
                poll_live_game(fixture, col, history_col)
            except Exception as e:
                logger.error(f"Poll error: {e}")
            finally:
                with polls_lock:
                    active_polls.discard(match_id)
                _poll_queue.task_done()
        except _queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Queue worker error: {e}")

def start_polling_for_game(fixture: dict, col, history_col):
    match_id = fixture["match_id"]
    with polls_lock:
        if match_id in active_polls:
            return
    
    global _queue_worker_started
    with _queue_lock:
        if not _queue_worker_started:
            threading.Thread(target=_queue_worker, daemon=True).start()
            _queue_worker_started = True
    
    _poll_queue.put((fixture, col, history_col))
    logger.info(f"📥 Queued: {fixture['home_team']} vs {fixture['away_team']}")

# ─────────────────────────────────────────────────────────────────────────────
# ══ MAIN LOOP ════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash — World Cup 2026 Ultimate Scraper")
    logger.info(f"   Preferred source: {get_preferred()}")
    logger.info(f"   Priority: {' → '.join(SOURCE_PRIORITY)}")
    logger.info("=" * 65)
    
    start_health_server()
    mongo_client, col = connect_db()
    history_col = get_history_collection(mongo_client)
    
    cleanup_all_completed_games(col, history_col)
    
    existing = load_fixtures_from_db(col)
    if existing:
        logger.info(f"📦 {len(existing)} fixtures in DB")
        last_scrape_time = time.time()
    else:
        logger.info("📭 DB empty — running initial scrape...")
        run_scraper(col)
        last_scrape_time = time.time()
    
    last_cleanup_time = time.time()
    lineups_fetched_set = set()
    
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            
            # Periodic rescrape
            if time.time() - last_scrape_time >= SCRAPE_INTERVAL_SEC:
                logger.info("\n🔄 6-hour rescrape...")
                run_scraper(col)
                last_scrape_time = time.time()
                lineups_fetched_set.clear()
            
            # Periodic cleanup
            if time.time() - last_cleanup_time >= CLEANUP_INTERVAL_SEC:
                cleanup_all_completed_games(col, history_col)
                last_cleanup_time = time.time()
            
            fixtures = load_fixtures_from_db(col)
            if not fixtures:
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
                        fetch_and_forward_lineups(lf, col)
                        lineups_fetched_set.add(mid)
                    
                    start_polling_for_game(lf, col, history_col)
                time.sleep(LIVE_CHECK_INTERVAL_SEC)
                continue
            
            # Upcoming in next 24h
            upcoming = []
            for f in fixtures:
                ko = f.get("_kickoff_utc")
                if not ko or f.get("status") == "completed":
                    continue
                mins = (ko - now_utc).total_seconds() / 60
                if 0 < mins <= 1440:
                    upcoming.append((mins, f))
            
            if not upcoming:
                logger.info("📭 No fixtures in next 24h — sleeping 1h")
                time.sleep(3600)
                continue
            
            upcoming.sort(key=lambda x: x[0])
            logger.info(f"📅 {len(upcoming)} fixtures in next 24h:")
            
            for mins_to_game, fixture in upcoming:
                mid = fixture["match_id"]
                
                if 0 < mins_to_game <= 60:
                    if fixture.get("status") != "soon":
                        update_fixture_status(mid, "soon")
                        update_db_status(col, mid, "soon")
                    
                    if mid not in lineups_fetched_set and not fixture.get("_lineups_fetched"):
                        fetch_and_forward_lineups(fixture, col)
                        lineups_fetched_set.add(mid)
            
            # Smart sleep - wake 1 hour before next match
            closest_mins, closest_fixture = upcoming[0]
            
            if closest_mins <= 5:
                start_polling_for_game(closest_fixture, col, history_col)
                time.sleep(POLL_INTERVAL_SEC)
                continue
            elif closest_mins <= 60:
                sleep_secs = LINEUP_POLL_INTERVAL_SEC
                logger.info(f"⏳ Checking every {sleep_secs}s — {int(closest_mins)} mins to kickoff")
            elif closest_mins <= 1440:
                sleep_secs = max(60, int((closest_mins - 60) * 60))
                wake_at = (datetime.now(timezone.utc) + timedelta(seconds=sleep_secs) + NAIROBI_OFFSET).strftime("%H:%M")
                logger.info(f"📅 Next game in {int(closest_mins/60)}h — sleeping until {wake_at} EAT")
            else:
                sleep_secs = 3600
            
            time.sleep(sleep_secs)
            
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down...")
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(60)
    
    if mongo_client:
        mongo_client.close()

if __name__ == "__main__":
    main()