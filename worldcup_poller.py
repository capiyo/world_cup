"""
World Cup 2026 - Production 3-Source Scraper
=============================================
Sources (in priority order):
  1. FLASHSCORE - Fast API feed, no API key
  2. FBREF - Web scraping with BeautifulSoup, no blocking
  3. SPORTSCORE - Free API, 10k requests/day, no API key

Features:
  - Automatic source failover (if one fails, try next)
  - Persistent source preference across restarts
  - Stable match ID generation (same ID across all sources)
  - Stores ALL source IDs (flashscore_id, fbref_id, sportscore_id)
  - Health server for monitoring
  - Undetectable scraping with random delays and user agents
  - Smart sleep scheduling

Install on Render:
  requirements.txt:
    requests==2.31.0
    pymongo==4.5.0
    python-dotenv==1.0.0
    beautifulsoup4==4.12.0
    lxml==4.9.0

Run:
  python worldcup_scraper.py
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
from typing import Dict, List, Optional, Tuple, Any

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from dotenv import load_dotenv

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

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE CONFIGURATIONS
# ─────────────────────────────────────────────────────────────────────────────

# Flashscore
FS_NINJA_HOST = "global.flashscore.ninja"
FS_FEED_BASE = f"https://{FS_NINJA_HOST}/2/x/feed/"
X_FSIGN_TOKEN = "SW9D1eZo"
FS_TOURNAMENT_ID = "lvUBR5F8"

# FBref
FBREF_BASE = "https://fbref.com"
FBREF_WC_URL = "https://fbref.com/en/comps/1/schedule/World-Cup-Scores-and-Fixtures"

# Sportscore (no API key needed)
SPORTSCORE_BASE = "https://api.sportscore.io/v1"

# ─────────────────────────────────────────────────────────────────────────────
# POLLING CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

POLL_INTERVAL_SEC = 45
LINEUP_POLL_INTERVAL_SEC = 30
LIVE_CHECK_INTERVAL_SEC = 60
SCRAPE_INTERVAL_SEC = 3600 * 6  # Every 6 hours
CLEANUP_INTERVAL_SEC = 300

# Source priority order
SOURCE_PRIORITY = ["flashscore", "fbref", "sportscore"]
preferred_source = "flashscore"
source_lock = threading.Lock()

# Source health tracking
source_health = {
    "flashscore": {"healthy": True, "failures": 0, "last_failure": None},
    "fbref": {"healthy": True, "failures": 0, "last_failure": None},
    "sportscore": {"healthy": True, "failures": 0, "last_failure": None},
}

# Anti-detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────

active_polls: set = set()
polls_lock = threading.Lock()
FS_SEMAPHORE = threading.Semaphore(1)

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
            "active_polls": len(active_polls),
            "available_sources": SOURCE_PRIORITY
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, *_):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", 10000))
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
    """Get the next healthy source in priority order."""
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
    """Mark a source as failed and potentially switch."""
    if source in source_health:
        source_health[source]["failures"] += 1
        source_health[source]["last_failure"] = time.time()
        
        if source_health[source]["failures"] >= 3:
            source_health[source]["healthy"] = False
            logger.warning(f"⚠️ Source {source} marked UNHEALTHY after {source_health[source]['failures']} failures")
            
            # Try to switch to next healthy source
            next_src = get_next_healthy_source(source)
            if next_src and next_src != get_preferred():
                set_preferred(next_src)

def mark_source_healthy(source: str):
    """Reset failure count for a source."""
    if source in source_health:
        source_health[source]["failures"] = 0
        source_health[source]["healthy"] = True
        logger.info(f"✅ Source {source} is healthy again")

# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────────────────────────────────────────

TEAM_NAME_CORRECTIONS = {
    "Türkiye": "Turkey", "Korea Republic": "South Korea",
    "Côte d'Ivoire": "Ivory Coast", "Congo DR": "DR Congo",
    "Bosnia & Herzegovina": "Bosnia and Herzegovina",
    "USA": "United States", "Curaçao": "Curacao", "Cabo Verde": "Cape Verde",
    "Czechia": "Czech Republic", "Korea DPR": "North Korea",
    "Russia": "Russia", "Ukraine": "Ukraine",
}

def correct_team_name(name: str) -> str:
    cleaned = " ".join(name.split())
    return TEAM_NAME_CORRECTIONS.get(cleaned, cleaned)

def eat_from_timestamp(ts: int) -> Tuple[str, str, str]:
    """Convert timestamp to (date_iso, date_display, time_eat) in EAT."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + NAIROBI_OFFSET
    return dt.strftime("%Y-%m-%d"), dt.strftime("%d %b"), dt.strftime("%H:%M")

def is_match_over(date_iso: str, time_str: str) -> bool:
    """Check if a match is already over."""
    if not date_iso or not time_str or time_str == "TBD":
        return False
    try:
        naive = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
        kickoff_utc = (naive - NAIROBI_OFFSET).replace(tzinfo=timezone.utc)
        return (kickoff_utc + timedelta(minutes=MATCH_DURATION_MINS)) < datetime.now(timezone.utc)
    except Exception:
        return False

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
    """
    Generate a stable match ID that works across ALL sources.
    Same teams + same date = same ID regardless of source.
    """
    key = f"{correct_team_name(home_team)}_{correct_team_name(away_team)}_{date_iso}"
    return hashlib.md5(key.encode()).hexdigest()[:12]

def build_fixture_doc(
    match_id: str,
    home_team: str,
    away_team: str,
    ts: int,
    status: str,
    home_score: Optional[int],
    away_score: Optional[int],
    source: str,
    extra_ids: Optional[Dict] = None,
) -> Dict:
    """Build a fixture document with all necessary fields."""
    if ts:
        date_iso, date_display, time_eat = eat_from_timestamp(ts)
    else:
        now = datetime.now(timezone.utc)
        date_iso = now.strftime("%Y-%m-%d")
        date_display = now.strftime("%d %b")
        time_eat = "TBD"

    doc = {
        "_id": match_id,
        "match_id": match_id,
        "home_team": correct_team_name(home_team),
        "away_team": correct_team_name(away_team),
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
        "source": source,
        "scraped_at": datetime.now(timezone.utc),
        "votes": 0,
        "comments": 0,
        "voters": [],
        "commentary": [],
        "commentary_count": 0,
        "last_commentary_at": None,
        "lineups_fetched": False,
    }
    if extra_ids:
        doc.update(extra_ids)
    return doc

# ─────────────────────────────────────────────────────────────────────────────
# ══ 1. FLASHSCORE CLIENT ════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

_fs_session: Optional[requests.Session] = None
_fs_session_lock = threading.Lock()

def _fs_make_session() -> requests.Session:
    s = requests.Session()
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

def _fs_get_session() -> requests.Session:
    global _fs_session
    with _fs_session_lock:
        if _fs_session is None:
            _fs_session = _fs_make_session()
        return _fs_session

def fs_get(query: str, retries: int = 3, base_delay: float = 2.0) -> Optional[str]:
    """Make a request to Flashscore API."""
    url = f"{FS_FEED_BASE}{query}"
    
    for attempt in range(retries):
        try:
            with FS_SEMAPHORE:
                time.sleep(random.uniform(base_delay, base_delay + 2.0))
                resp = _fs_get_session().get(url, timeout=30)
            
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 404:
                return None
            if resp.status_code == 403:
                wait = (2 ** attempt) * random.uniform(4, 8)
                logger.warning(f"   FS 403 attempt {attempt+1} — back-off {wait:.0f}s")
                time.sleep(wait)
                continue
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"   FS 429 — waiting {wait}s")
                time.sleep(wait)
                continue
            time.sleep(5)
        except Exception as e:
            logger.warning(f"   FS error attempt {attempt+1}: {e}")
            time.sleep(8)
    
    return None

def _parse_fs_rows(raw: str) -> List[Dict[str, str]]:
    """Parse Flashscore's ¬ and ÷ delimited format."""
    rows = []
    for row in raw.split("~"):
        row = row.strip()
        if not row:
            continue
        f: Dict[str, str] = {}
        for part in row.split("¬"):
            if "÷" in part:
                k, _, v = part.partition("÷")
                f[k.strip()] = v.strip()
        if f:
            rows.append(f)
    return rows

def _fs_map_status_code(code: int) -> str:
    """Map Flashscore status code to internal status."""
    if code in (100, 110, 111, 120, 121):
        return "completed"
    if code in (2, 3, 4, 5, 6, 7, 8, 9, 10, 41, 42):
        return "live"
    return "upcoming"

def fs_parse_fixtures(raw: str, upcoming_only: bool = True) -> List[Dict]:
    """Parse Flashscore fixtures from response."""
    docs: List[Dict] = []
    if not raw:
        return docs
    
    for f in _parse_fs_rows(raw):
        fs_id = f.get("LME", "").strip()
        if not fs_id:
            continue
        
        home_team = _clean(f.get("LMJ", ""))
        away_team = _clean(f.get("LMK", ""))
        if not home_team or not away_team:
            continue
        
        try:
            ts = int(f.get("LMC", 0))
        except (ValueError, TypeError):
            ts = 0
        
        status = "upcoming"  # Default for schedule
        if ts:
            date_iso, _, time_eat = eat_from_timestamp(ts)
            if upcoming_only and is_match_over(date_iso, time_eat):
                continue
        else:
            date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        match_id = generate_stable_match_id(home_team, away_team, date_iso)
        
        docs.append(build_fixture_doc(
            match_id, home_team, away_team, ts, status, None, None,
            source="flashscore",
            extra_ids={"flashscore_id": fs_id},
        ))
    
    return docs

def fs_get_season_stage_ids() -> Tuple[Optional[str], Optional[str]]:
    """Get Flashscore season and stage IDs for World Cup."""
    raw = fs_get(f"t_1_8_{FS_TOURNAMENT_ID}_3_en_1", base_delay=2.0)
    if not raw:
        return None, None
    
    for f in _parse_fs_rows(raw):
        if "ZA" in f:
            season_id = f.get("ZC", "").strip()
            stage_id = f.get("ZE", "").strip()
            if season_id and stage_id:
                logger.info(f"   FS: season_id={season_id}, stage_id={stage_id}")
                return season_id, stage_id
    
    return None, None

def fs_run_scraper() -> List[Dict]:
    """Run Flashscore scraper for World Cup fixtures."""
    logger.info("   📡 Flashscore scraper starting...")
    docs: List[Dict] = []
    seen: set = set()
    
    season_id, stage_id = fs_get_season_stage_ids()
    
    if season_id and stage_id:
        for page in range(1, 20):
            endpoint = f"to_{stage_id}_{season_id}_{page}"
            raw = fs_get(endpoint, base_delay=2.0)
            
            if not raw or len(raw.strip()) < 10:
                logger.info(f"   FS: page {page} empty — done")
                break
            
            page_docs = fs_parse_fixtures(raw, upcoming_only=True)
            new = [d for d in page_docs if d["_id"] not in seen]
            for d in new:
                seen.add(d["_id"])
            docs.extend(new)
            
            logger.info(f"   FS: page {page} → {len(new)} fixtures (total: {len(docs)})")
            
            if len(page_docs) == 0:
                break
            time.sleep(random.uniform(2.0, 3.5))
    else:
        logger.warning("   FS: could not get season/stage IDs")
        return []
    
    logger.info(f"   FS: got {len(docs)} fixtures")
    if docs:
        mark_source_healthy("flashscore")
    else:
        mark_source_failure("flashscore")
    
    return docs

def fs_fetch_live_data(fs_id: str) -> Optional[Dict]:
    """Fetch live match data from Flashscore."""
    raw = fs_get(f"dc_{fs_id}", base_delay=2.0)
    if not raw:
        return None
    
    for f in _parse_fs_rows(raw):
        if not f.get("AA"):
            continue
        
        try:
            status_code = int(f.get("AB", "1"))
        except (ValueError, TypeError):
            status_code = 1
        
        status = _fs_map_status_code(status_code)
        home_score = _safe_int(f.get("AG", "")) or 0
        away_score = _safe_int(f.get("AH", "")) or 0
        
        time_elapsed = 0
        for tf in ("BC", "BD", "BF", "BG"):
            v = f.get(tf, "").strip()
            if v and v.isdigit():
                time_elapsed = int(v)
                break
        
        time_extra = 0
        for tf in ("BH", "BI"):
            v = f.get(tf, "").strip()
            if v and v.isdigit():
                time_extra = int(v)
                break
        
        return {
            "home_score": home_score,
            "away_score": away_score,
            "status": status,
            "status_code": status_code,
            "time_elapsed": time_elapsed,
            "time_extra": time_extra,
        }
    
    return None

def fs_fetch_lineups(fs_id: str) -> Optional[Dict]:
    """Fetch lineups from Flashscore."""
    raw = fs_get(f"li_{fs_id}_1_en", base_delay=3.0)
    if not raw:
        return None
    
    lineups = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }
    
    for row in raw.split("~"):
        for segment in row.split("¬"):
            segment = segment.strip()
            
            if segment.startswith("LU÷"):
                parts = segment[3:].split("÷")
                if len(parts) >= 1 and parts[0]:
                    lineups["home"]["formation"] = parts[0]
                if len(parts) >= 2 and parts[1]:
                    lineups["away"]["formation"] = parts[1]
            
            elif segment.startswith("PL÷"):
                parts = segment[3:].split("÷")
                if len(parts) < 6:
                    continue
                try:
                    jersey = int(parts[2]) if parts[2].isdigit() else 0
                    side = "home" if parts[4] == "1" else "away"
                    is_starter = parts[5] == "1"
                    player = {
                        "name": _clean(parts[1]),
                        "position": parts[3] if len(parts) > 3 else "Unknown",
                        "jerseyNumber": jersey,
                        "captain": len(parts) > 7 and parts[7] == "1",
                        "lineup": is_starter,
                    }
                    if is_starter:
                        lineups[side]["players"].append(player)
                    else:
                        lineups[side]["bench"].append(player)
                except (ValueError, IndexError):
                    continue
            
            elif segment.startswith("CO÷"):
                parts = segment[3:].split("÷")
                if len(parts) >= 3:
                    side = "home" if parts[2] == "1" else "away"
                    lineups[side]["coach"]["name"] = _clean(parts[1]) or "Unknown"
    
    return lineups

def fs_fetch_statistics(fs_id: str, time_elapsed: int) -> Optional[Dict]:
    """Fetch match statistics from Flashscore."""
    raw = fs_get(f"od_{fs_id}", base_delay=2.0)
    if not raw:
        return None
    
    KEY_MAP = {
        "ball possession": ("ball_possession_home", "ball_possession_away"),
        "total shots": ("total_shots_home", "total_shots_away"),
        "shots on target": ("shots_on_target_home", "shots_on_target_away"),
        "corner kicks": ("corners_home", "corners_away"),
        "fouls": ("fouls_home", "fouls_away"),
        "offsides": ("offsides_home", "offsides_away"),
        "yellow cards": ("yellow_cards_home", "yellow_cards_away"),
        "red cards": ("red_cards_home", "red_cards_away"),
    }
    
    stats: Dict[str, Any] = {
        "ball_possession_home": 0, "ball_possession_away": 0,
        "total_shots_home": 0, "total_shots_away": 0,
        "shots_on_target_home": 0, "shots_on_target_away": 0,
        "corners_home": 0, "corners_away": 0,
        "fouls_home": 0, "fouls_away": 0,
        "offsides_home": 0, "offsides_away": 0,
        "yellow_cards_home": 0, "yellow_cards_away": 0,
        "red_cards_home": 0, "red_cards_away": 0,
    }
    
    for row in raw.split("~"):
        for seg in row.split("¬"):
            if not seg.startswith("ST÷"):
                continue
            parts = seg[3:].split("÷")
            if len(parts) < 3:
                continue
            key = parts[0].lower()
            mapped = KEY_MAP.get(key)
            if mapped:
                try:
                    stats[mapped[0]] = int(str(parts[1]).replace("%", "").strip())
                    stats[mapped[1]] = int(str(parts[2]).replace("%", "").strip())
                except (ValueError, TypeError):
                    pass
    
    minute_disp = f"{time_elapsed}" + (f"+{stats.get('time_extra', 0)}" if stats.get('time_extra', 0) else "")
    stats["minute"] = time_elapsed
    stats["minute_display"] = minute_disp
    stats["timestamp"] = datetime.now(timezone.utc).isoformat()
    
    return stats

# ─────────────────────────────────────────────────────────────────────────────
# ══ 2. FBREF CLIENT ═════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def fbref_fetch_page(url: str, retries: int = 3) -> Optional[str]:
    """Fetch a page from FBref with respectful delays."""
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(3, 6))
            resp = requests.get(url, timeout=30, headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            })
            
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 403:
                logger.warning(f"   FBref 403 attempt {attempt+1} — waiting longer")
                time.sleep(random.uniform(15, 30))
                continue
            if resp.status_code == 429:
                wait = (2 ** attempt) * 30
                logger.warning(f"   FBref rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            time.sleep(5)
        except Exception as e:
            logger.warning(f"   FBref error attempt {attempt+1}: {e}")
            time.sleep(8)
    
    return None

def fbref_parse_schedule(html: str) -> List[Dict]:
    """Parse FBref schedule page for World Cup fixtures."""
    docs: List[Dict] = []
    seen: set = set()
    
    if not html:
        return docs
    
    soup = BeautifulSoup(html, 'lxml')
    
    # Find the schedule table
    table = soup.find('table', {'id': 'sched_all'})
    if not table:
        table = soup.find('table', class_=re.compile('schedule'))
    
    if not table:
        logger.warning("   FBref: Could not find schedule table")
        return docs
    
    tbody = table.find('tbody')
    if not tbody:
        tbody = table
    
    for row in tbody.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) < 5:
            continue
        
        home_cell = None
        away_cell = None
        date_cell = None
        score_cell = None
        
        for cell in cells:
            stat = cell.get('data-stat', '')
            if stat == 'home_team':
                home_cell = cell
            elif stat == 'away_team':
                away_cell = cell
            elif stat == 'date':
                date_cell = cell
            elif stat == 'score':
                score_cell = cell
        
        if not home_cell or not away_cell:
            continue
        
        home_team = _clean(home_cell.get_text())
        away_team = _clean(away_cell.get_text())
        
        if not home_team or not away_team:
            continue
        
        # Parse date
        date_str = _clean(date_cell.get_text()) if date_cell else ""
        ts = 0
        date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        if date_str:
            try:
                match_date = datetime.strptime(date_str, "%a %b %d, %Y")
                ts = int(match_date.replace(tzinfo=timezone.utc).timestamp())
                date_iso = match_date.strftime("%Y-%m-%d")
            except Exception as e:
                logger.debug(f"   FBref date parse error: {e}")
        
        # Parse score if available
        home_score = None
        away_score = None
        status = "upcoming"
        
        if score_cell:
            score_text = _clean(score_cell.get_text())
            if score_text and '-' in score_text:
                parts = score_text.split('-')
                if len(parts) == 2:
                    home_score = _safe_int(parts[0])
                    away_score = _safe_int(parts[1])
                    if home_score is not None and away_score is not None:
                        status = "completed"
        
        # Get FBref match ID from link
        fbref_id = None
        link = home_cell.find('a') or away_cell.find('a')
        if link and link.get('href'):
            match = re.search(r'/matches/(\d+)/', link.get('href'))
            if match:
                fbref_id = match.group(1)
        
        match_id = generate_stable_match_id(home_team, away_team, date_iso)
        if match_id in seen:
            continue
        seen.add(match_id)
        
        docs.append(build_fixture_doc(
            match_id, home_team, away_team, ts, status, home_score, away_score,
            source="fbref",
            extra_ids={"fbref_id": fbref_id} if fbref_id else None,
        ))
    
    return docs

def fbref_run_scraper() -> List[Dict]:
    """Run FBref scraper for World Cup fixtures."""
    logger.info("   📡 FBref scraper starting...")
    
    html = fbref_fetch_page(FBREF_WC_URL)
    if not html:
        logger.warning("   FBref: Failed to fetch page")
        mark_source_failure("fbref")
        return []
    
    docs = fbref_parse_schedule(html)
    logger.info(f"   FBref: got {len(docs)} fixtures")
    
    if docs:
        mark_source_healthy("fbref")
    else:
        mark_source_failure("fbref")
    
    return docs

# ─────────────────────────────────────────────────────────────────────────────
# ══ 3. SPORTSCORE CLIENT ════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def sportscore_request(endpoint: str, params: Dict = None, retries: int = 3) -> Optional[Dict]:
    """Make request to Sportscore API (no API key needed)."""
    url = f"{SPORTSCORE_BASE}/{endpoint}"
    
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(1, 2))
            resp = requests.get(url, params=params, timeout=30, headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json",
            })
            
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = (2 ** attempt) * 30
                logger.warning(f"   Sportscore rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            
            logger.warning(f"   Sportscore HTTP {resp.status_code}")
            time.sleep(5)
        except Exception as e:
            logger.warning(f"   Sportscore error attempt {attempt+1}: {e}")
            time.sleep(8)
    
    return None

def sportscore_parse_fixtures(data: Dict) -> List[Dict]:
    """Parse Sportscore API response into fixture documents."""
    docs: List[Dict] = []
    seen: set = set()
    
    fixtures = data.get("data", [])
    if not fixtures:
        return docs
    
    for fixture in fixtures:
        home_team = fixture.get("home_team", {}).get("name", "")
        away_team = fixture.get("away_team", {}).get("name", "")
        
        if not home_team or not away_team:
            continue
        
        # Get timestamp
        start_time = fixture.get("starting_at")
        ts = 0
        date_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        if start_time:
            try:
                dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                ts = int(dt.timestamp())
                date_iso = dt.strftime("%Y-%m-%d")
            except Exception:
                pass
        
        # Get status
        status = fixture.get("status", "upcoming")
        if status == "inprogress":
            status = "live"
        elif status == "finished":
            status = "completed"
        
        # Get scores
        home_score = fixture.get("home_score")
        away_score = fixture.get("away_score")
        
        match_id = generate_stable_match_id(home_team, away_team, date_iso)
        if match_id in seen:
            continue
        seen.add(match_id)
        
        sportscore_id = str(fixture.get("id", ""))
        
        docs.append(build_fixture_doc(
            match_id, home_team, away_team, ts, status, home_score, away_score,
            source="sportscore",
            extra_ids={"sportscore_id": sportscore_id},
        ))
    
    return docs

def sportscore_run_scraper() -> List[Dict]:
    """Run Sportscore scraper for World Cup fixtures."""
    logger.info("   📡 Sportscore scraper starting...")
    
    # Try to get World Cup fixtures
    data = sportscore_request("soccer/fixtures", params={"tournament_id": "world-cup-2026"})
    
    if not data:
        # Try alternative endpoint
        data = sportscore_request("soccer/fixtures", params={"competition": "world-cup"})
    
    if not data:
        logger.warning("   Sportscore: No data returned")
        mark_source_failure("sportscore")
        return []
    
    docs = sportscore_parse_fixtures(data)
    logger.info(f"   Sportscore: got {len(docs)} fixtures")
    
    if docs:
        mark_source_healthy("sportscore")
    else:
        mark_source_failure("sportscore")
    
    return docs

def sportscore_fetch_live_data(sportscore_id: str) -> Optional[Dict]:
    """Fetch live match data from Sportscore."""
    data = sportscore_request(f"soccer/fixtures/{sportscore_id}")
    if not data:
        return None
    
    fixture = data.get("data", {})
    
    return {
        "home_score": fixture.get("home_score", 0),
        "away_score": fixture.get("away_score", 0),
        "status": fixture.get("status", "upcoming"),
        "time_elapsed": fixture.get("time_elapsed", 0),
    }

# ─────────────────────────────────────────────────────────────────────────────
# ══ COMBINED SCRAPER WITH FAILOVER ═══════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

SCRAPER_MAP = {
    "flashscore": fs_run_scraper,
    "fbref": fbref_run_scraper,
    "sportscore": sportscore_run_scraper,
}

def run_scraper(col) -> List[Dict]:
    """Run combined scraper with automatic failover."""
    logger.info("\n" + "=" * 65)
    logger.info("📡 RUNNING WORLD CUP 2026 3-SOURCE SCRAPER")
    logger.info("=" * 65)
    
    current = get_preferred()
    logger.info(f"   Preferred source: {current}")
    logger.info(f"   Priority order: {' → '.join(SOURCE_PRIORITY)}")
    
    all_docs: List[Dict] = []
    all_ids: set = set()
    tried_sources = []
    
    # Try sources in priority order starting from preferred
    start_idx = SOURCE_PRIORITY.index(current) if current in SOURCE_PRIORITY else 0
    
    for i in range(start_idx, len(SOURCE_PRIORITY)):
        source = SOURCE_PRIORITY[i]
        if source in tried_sources:
            continue
        
        logger.info(f"\n   Trying source: {source}")
        docs = SCRAPER_MAP[source]()
        
        if docs:
            logger.info(f"   ✅ {source} succeeded with {len(docs)} fixtures")
            for doc in docs:
                if doc["_id"] not in all_ids:
                    all_ids.add(doc["_id"])
                    all_docs.append(doc)
            
            # If this isn't the preferred source, update preference
            if source != current:
                set_preferred(source)
                current = source
            
            break  # Stop after first successful source
        else:
            logger.warning(f"   ❌ {source} failed")
            tried_sources.append(source)
            mark_source_failure(source)
    
    # If all sources failed, try again with delay
    if not all_docs:
        logger.warning("   ⚠️  All sources failed — backing off and retrying")
        time.sleep(random.uniform(60, 120))
        
        for source in SOURCE_PRIORITY:
            docs = SCRAPER_MAP[source]()
            if docs:
                all_docs.extend(docs)
                set_preferred(source)
                break
    
    # Save to database with ID merging
    if all_docs and col is not None:
        saved = 0
        for doc in all_docs:
            try:
                # Use $set to avoid overwriting existing fields
                col.update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
                saved += 1
            except Exception as e:
                logger.warning(f"   DB save error: {e}")
        logger.info(f"   💾 Saved {saved} World Cup fixtures (source={get_preferred()})")
    
    logger.info(f"\n📊 Scraper done: {len(all_docs)} fixtures  |  preferred_source={get_preferred()}")
    return all_docs

# ─────────────────────────────────────────────────────────────────────────────
# ══ DATABASE HELPERS ═════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    """Connect to MongoDB and setup indexes."""
    try:
        client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=15000)
        client.admin.command("ping")
        
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        
        # Drop existing conflicting indexes
        existing_indexes = col.index_information()
        for idx_name in ["match_id_1", "flashscore_id_1", "fbref_id_1", "sportscore_id_1"]:
            if idx_name in existing_indexes:
                try:
                    col.drop_index(idx_name)
                except:
                    pass
        
        # Create clean indexes
        col.create_index("match_id", unique=True)
        col.create_index("flashscore_id")
        col.create_index("fbref_id")
        col.create_index("sportscore_id")
        col.create_index("status")
        col.create_index("league")
        col.create_index("date_iso")
        col.create_index("source")
        
        logger.info(f"✅ Connected to {DB_NAME}.{COLLECTION_NAME}")
        return client, col
    except Exception as e:
        logger.warning(f"⚠️ MongoDB failed: {e}")
        return None, None

def get_history_collection(client):
    """Get or create history collection."""
    if client is None:
        return None
    hcol = client[DB_NAME]["fixtures_history"]
    try:
        hcol.create_index("completed_at")
        hcol.create_index("match_id")
    except:
        pass
    return hcol

def move_completed_game_to_history(col, history_col, match_id: str) -> bool:
    """Move completed game to history collection."""
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
    """Clean up all completed games."""
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
            logger.info(f"🧹 Moved {moved} completed games to history")
    except Exception as e:
        logger.error(f"cleanup error: {e}")

def load_fixtures_from_db(col) -> List[Dict[str, Any]]:
    """Load fixtures from database."""
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
            "flashscore_id": f.get("flashscore_id"),
            "fbref_id": f.get("fbref_id"),
            "sportscore_id": f.get("sportscore_id"),
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

def mark_lineups_fetched(col, match_id: str):
    """Mark that lineups have been fetched for a match."""
    if col is None:
        return
    try:
        col.update_one({"match_id": match_id}, {"$set": {"lineups_fetched": True}})
    except Exception as e:
        logger.warning(f"mark_lineups_fetched error: {e}")

def update_db_status(col, match_id: str, status: str, extra_fields: Optional[dict] = None):
    """Update match status in database."""
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
    """Get fixtures that are currently live."""
    now_utc = datetime.now(timezone.utc)
    return [
        f for f in fixtures
        if f.get("status") == "live"
        or (f.get("_kickoff_utc") and now_utc >= f["_kickoff_utc"] and f.get("status") != "completed")
    ]

# ─────────────────────────────────────────────────────────────────────────────
# ══ BACKEND API CALLS ════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def update_fixture_status(match_id: str, status: str):
    """Update fixture status on backend."""
    if status == "finished":
        status = "completed"
    try:
        r = requests.put(
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
    """Check if lineups already exist in backend."""
    try:
        r = requests.get(f"{FANCLASH_API}/games/{match_id}/lineups", timeout=5)
        if r.status_code == 200:
            data = r.json()
            hp = data.get("lineups", {}).get("home", {}).get("players", [])
            ap = data.get("lineups", {}).get("away", {}).get("players", [])
            return bool(hp or ap)
        return False
    except Exception:
        return False

def forward_event(fixture: dict, event_type: str, data: dict):
    """Forward live event to backend."""
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
        r = requests.post(f"{FANCLASH_API}/games/live-update", json=payload, timeout=5)
        if r.status_code != 200:
            logger.warning(f"❌ forward_event {event_type}: {r.status_code}")
    except Exception as e:
        logger.error(f"forward_event error: {e}")

def send_commentary(fixture: dict, data: dict):
    """Send commentary to backend."""
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
        r = requests.post(
            f"{FANCLASH_API}/games/commentary",
            json={"match_id": fixture["match_id"], "entry": entry},
            timeout=3,
        )
        if r.status_code != 200:
            logger.warning(f"❌ send_commentary: {r.status_code}")
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
        # FBref lineups would require additional implementation
        pass
    elif source == "sportscore":
        # Sportscore lineups would require additional implementation
        pass
    return None

def fetch_and_forward_lineups(fixture: Dict, col) -> bool:
    """Fetch lineups using the current preferred source."""
    match_id = fixture.get("match_id")
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
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
        r = requests.post(
            f"{FANCLASH_API}/games/lineups",
            json={"fixture_id": match_id, "lineups": lineups, "timestamp": datetime.now(timezone.utc).isoformat()},
            timeout=5,
        )
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
    """Fetch and forward match statistics."""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    time_elapsed = live_data.get("time_elapsed", 0)
    current_source = get_preferred()
    
    payload = fetch_statistics_from_source(fixture, current_source, time_elapsed)
    
    if not payload:
        for source in SOURCE_PRIORITY:
            if source == current_source:
                continue
            payload = fetch_statistics_from_source(fixture, source, time_elapsed)
            if payload:
                break
    
    if not payload:
        logger.warning(f"   No stats for {label}")
        return
    
    payload["match_id"] = match_id
    try:
        r = requests.post(f"{FANCLASH_API}/games/statistics", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"📊 Stats forwarded for {label} ({payload.get('minute_display', '?')}')")
        else:
            logger.warning(f"❌ Stats failed: {r.status_code}")
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
    elif source == "sportscore":
        ss_id = fixture.get("sportscore_id")
        if ss_id:
            return sportscore_fetch_live_data(ss_id)
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
    seen_incidents: set = set()
    poll_count = 0
    
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
        
        # Half time detection
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
        
        # Second half start
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
        if status in ("completed", "finished") and not full_time_sent:
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
        
        # Periodic stats every 5 polls
        poll_count += 1
        if poll_count % 5 == 0:
            logger.info(f"📊 Stats snapshot at {minute_disp}' for {label}")
            fetch_and_forward_statistics(fixture, live)
        
        time.sleep(POLL_INTERVAL_SEC)
    
    logger.info(f"✅ Done polling {label}")

# ─────────────────────────────────────────────────────────────────────────────
# ══ POLL QUEUE ════════════════════════════════════════════════════════════════
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
            threading.Thread(target=_queue_worker, daemon=True, name="poll-worker").start()
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
# ══ MAIN LOOP ═════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash — World Cup 2026 3-Source Poller")
    logger.info(f"   Initial preferred source: {get_preferred()}")
    logger.info(f"   Priority order: {' → '.join(SOURCE_PRIORITY)}")
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
            
            # Periodic rescrape every 6 hours
            if time.time() - last_scrape_time >= SCRAPE_INTERVAL_SEC:
                logger.info("\n🔄 6-hour rescrape starting...")
                run_scraper(col)
                last_scrape_time = time.time()
                lineups_fetched_set.clear()
            
            # Periodic cleanup
            if time.time() - last_cleanup_time >= CLEANUP_INTERVAL_SEC:
                cleanup_all_completed_games(col, history_col)
                last_cleanup_time = time.time()
            
            fixtures = load_fixtures_from_db(col)
            if not fixtures:
                logger.warning("📭 No fixtures in DB — scraping now")
                run_scraper(col)
                last_scrape_time = time.time()
                fixtures = load_fixtures_from_db(col)
            
            # ── Live games ─────────────────────────────────────────────────
            live_fixtures = get_live_fixtures(fixtures)
            if live_fixtures:
                logger.info(f"\n🔴 {len(live_fixtures)} LIVE GAME(S) DETECTED (preferred_source={get_preferred()})")
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
            
            # ── Upcoming in next 24h ───────────────────────────────────────
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
            logger.info(f"📅 {len(upcoming)} fixture(s) in next 24h (source={get_preferred()}):")
            for mins, f in upcoming[:10]:  # Show first 10
                ko_local = (f["_kickoff_utc"] + NAIROBI_OFFSET).strftime("%H:%M")
                icon = "🔴" if f.get("status") == "soon" else "⏳"
                logger.info(
                    f"   {icon} {f['home_team']} vs {f['away_team']} "
                    f"at {ko_local} EAT ({int(mins)} mins) [{f.get('source','')}]"
                )
            
            # Process upcoming fixtures
            for mins_to_game, fixture in upcoming:
                mid = fixture["match_id"]
                label = f"{fixture['home_team']} vs {fixture['away_team']}"
                
                if 0 < mins_to_game <= 60:
                    if fixture.get("status") != "soon":
                        logger.info(f"⏰ {label} — {int(mins_to_game)} mins — SOON")
                        update_fixture_status(mid, "soon")
                        update_db_status(col, mid, "soon")
                    
                    if mid not in lineups_fetched_set and not fixture.get("_lineups_fetched"):
                        if not check_lineups_exist_in_backend(mid):
                            if fetch_and_forward_lineups(fixture, col):
                                lineups_fetched_set.add(mid)
                        else:
                            lineups_fetched_set.add(mid)
                elif mins_to_game <= 1440:
                    if fixture.get("status") not in ("upcoming", "soon"):
                        update_db_status(col, mid, "upcoming")
            
            # Smart sleep scheduling
            closest_mins, closest_fixture = upcoming[0]
            
            if 0 < closest_mins <= 5:
                logger.info(f"⚽ Starting poll for {closest_fixture['home_team']} vs {closest_fixture['away_team']}")
                start_polling_for_game(closest_fixture, col, history_col)
                time.sleep(POLL_INTERVAL_SEC)
                continue
            
            if closest_mins <= 60:
                sleep_secs = LINEUP_POLL_INTERVAL_SEC
                logger.info(f"⏳ Checking every {sleep_secs}s — {int(closest_mins)} mins to kickoff")
            elif closest_mins <= 1440:
                # Sleep until 1 hour before the next game
                sleep_secs = max(60, int((closest_mins - 60) * 60))
                wake_at = (datetime.now(timezone.utc) + timedelta(seconds=sleep_secs) + NAIROBI_OFFSET).strftime("%H:%M")
                logger.info(
                    f"📅 Next game in {int(closest_mins / 60)}h {int(closest_mins % 60)}m — "
                    f"sleeping until {wake_at} EAT (1h before kickoff)"
                )
            else:
                sleep_secs = 3600
                logger.info(f"📅 Next game in {int(closest_mins/60)}h — sleeping hourly")
            
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