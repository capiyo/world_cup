"""
World Cup 2026 — Hybrid Poller (API-Football + Football-Data.org)
====================================================================
Primary: API-Football.com (100 requests/day)
Fallback: Football-Data.org (10 requests/minute)

Features:
- Auto-switch between APIs when limits reached
- Saves both API IDs for cross-referencing
- Smart request tracking and caching
- Full compatibility with Rust backend
"""

import time
import random
import logging
import os
import threading
import queue as _queue
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import OrderedDict

import requests as std_requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

class MatchStatus(Enum):
    UPCOMING = "upcoming"
    SOON = "soon"
    LIVE = "live"
    COMPLETED = "completed"

@dataclass
class Config:
    world_cup_label: str = "World Cup 2026"
    tournament_id: str = "lvUBR5F8"
    
    # API-Football.com (Primary)
    api_football_key: str = os.getenv("API_FOOTBALL_KEY", "68a72cb4152bc5052f0daaee7cb8714e")
    api_football_base: str = "https://v3.football.api-sports.io"
    wc_league_id: int = 1  # World Cup
    wc_season: int = 2026
    
    # Football-Data.org (Fallback)
    fd_api_key: str = os.getenv("FD_API_KEY", "d6016bacb4a44b41a554cf1aa7973d72")
    fd_base: str = "https://api.football-data.org/v4"
    fd_wc_code: str = "WC"
    
    # Database
    database_url: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name: str = "clashdb"
    collection_name: str = "fixtures"
    
    # Backend
    fanclash_api: str = os.environ.get("FANCLASH_API", "http://localhost:5000/api")
    
    # Optimized polling intervals
    poll_interval_sec: int = 60
    lineup_poll_interval_sec: int = 120
    live_check_interval_sec: int = 60
    fixture_refresh_interval_min: int = 30
    
    # Lineup polling window (start 90 minutes before kickoff)
    lineup_poll_start_min: int = 90
    
    # Nairobi offset (UTC+3)
    nairobi_offset: timedelta = timedelta(hours=3)
    
    # API usage tracking
    api_requests: Dict[str, int] = field(default_factory=lambda: {"apifootball": 0, "footballdata": 0})
    api_limit: int = 100
    api_switched: bool = False
    request_lock: threading.Lock = threading.Lock()

CONFIG = Config()

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
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────

active_polls: set = set()
polls_lock = threading.Lock()
api_semaphore = threading.Semaphore(1)
lineup_polling_active: Dict[str, bool] = {}
lineup_polling_lock = threading.Lock()
poll_queue = _queue.Queue()
queue_worker_started = False
queue_lock = threading.Lock()

# Simple cache
class APICache:
    def __init__(self, max_size=50, ttl=60):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.ttl = ttl
    
    def get(self, key):
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            del self.cache[key]
        return None
    
    def set(self, key, value):
        if len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        self.cache[key] = (value, time.time())

api_cache = APICache()

# ─────────────────────────────────────────────────────────────────────────────
# HEALTH SERVER
# ─────────────────────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            with CONFIG.request_lock:
                import json
                body = {
                    "status": "ok",
                    "service": "worldcup-poller",
                    "api_status": {
                        "apifootball": CONFIG.api_requests["apifootball"],
                        "footballdata": CONFIG.api_requests["footballdata"],
                        "total": sum(CONFIG.api_requests.values()),
                        "limit": CONFIG.api_limit,
                        "switched": CONFIG.api_switched
                    }
                }
                body = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = b"FanClash WorldCup Poller Running"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body)
    
    def log_message(self, *args, **kwargs):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", 8081))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"🌐 Health server on port {port}")

# ─────────────────────────────────────────────────────────────────────────────
# API-FOOTBALL CLIENT (Primary)
# ─────────────────────────────────────────────────────────────────────────────

af_session = None
af_session_lock = threading.Lock()

def _make_af_session():
    s = std_requests.Session()
    s.headers.update({
        "x-apisports-key": CONFIG.api_football_key,
        "Accept": "application/json",
    })
    return s

def _get_af_session():
    global af_session
    with af_session_lock:
        if af_session is None:
            af_session = _make_af_session()
        return af_session

def api_football_get(endpoint: str, params: Dict = None, retries: int = 2) -> Optional[Dict]:
    """Fetch from API-Football with rate limiting"""
    # Check if we've hit the limit
    with CONFIG.request_lock:
        if CONFIG.api_requests["apifootball"] >= CONFIG.api_limit:
            logger.warning(f"⚠️ API-Football limit reached ({CONFIG.api_limit} requests)")
            CONFIG.api_switched = True
            return None
    
    url = f"{CONFIG.api_football_base}{endpoint}"
    cache_key = f"af:{endpoint}:{str(params)}"
    
    # Check cache
    cached = api_cache.get(cache_key)
    if cached:
        return cached
    
    for attempt in range(retries):
        try:
            with api_semaphore:
                time.sleep(random.uniform(2.0, 3.0))
                resp = _get_af_session().get(url, params=params, timeout=15)
            
            if resp.status_code == 200:
                data = resp.json()
                if data.get("errors"):
                    logger.warning(f"API-Football errors: {data['errors']}")
                    if "rate limit" in str(data['errors']).lower():
                        time.sleep(30)
                        continue
                    return None
                
                # Track request
                with CONFIG.request_lock:
                    CONFIG.api_requests["apifootball"] += 1
                    logger.info(f"📊 API-Football requests: {CONFIG.api_requests['apifootball']}/{CONFIG.api_limit}")
                
                # Cache response
                api_cache.set(cache_key, data)
                return data
            
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                logger.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            
            logger.warning(f"API-Football HTTP {resp.status_code} attempt {attempt+1}")
            time.sleep(5)
            
        except Exception as e:
            logger.warning(f"API-Football error attempt {attempt+1}: {e}")
            time.sleep(8)
    
    return None

# ─────────────────────────────────────────────────────────────────────────────
# FOOTBALL-DATA.ORG CLIENT (Fallback)
# ─────────────────────────────────────────────────────────────────────────────

fd_session = None
fd_session_lock = threading.Lock()

def _make_fd_session():
    s = std_requests.Session()
    s.headers.update({
        "X-Auth-Token": CONFIG.fd_api_key,
        "Accept": "application/json",
    })
    return s

def _get_fd_session():
    global fd_session
    with fd_session_lock:
        if fd_session is None:
            fd_session = _make_fd_session()
        return fd_session

def football_data_get(endpoint: str, params: Dict = None, retries: int = 2) -> Optional[Dict]:
    """Fetch from Football-Data.org as fallback"""
    url = f"{CONFIG.fd_base}{endpoint}"
    cache_key = f"fd:{endpoint}:{str(params)}"
    
    # Check cache
    cached = api_cache.get(cache_key)
    if cached:
        return cached
    
    for attempt in range(retries):
        try:
            with api_semaphore:
                time.sleep(random.uniform(1.0, 2.0))
                resp = _get_fd_session().get(url, params=params, timeout=15)
            
            if resp.status_code == 200:
                data = resp.json()
                
                # Track request
                with CONFIG.request_lock:
                    CONFIG.api_requests["footballdata"] += 1
                    logger.info(f"📊 Football-Data.org requests: {CONFIG.api_requests['footballdata']}")
                
                # Cache response
                api_cache.set(cache_key, data)
                return data
            
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"FD rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            
            logger.warning(f"FD HTTP {resp.status_code} attempt {attempt+1}")
            time.sleep(5)
            
        except Exception as e:
            logger.warning(f"FD error attempt {attempt+1}: {e}")
            time.sleep(8)
    
    return None

# ─────────────────────────────────────────────────────────────────────────────
# HYBRID API CLIENT
# ─────────────────────────────────────────────────────────────────────────────

def hybrid_get(endpoint: str, params: Dict = None, use_primary: bool = True, 
               retries: int = 2) -> Optional[Dict]:
    """
    Hybrid API client - tries primary (API-Football) first, falls back to FD
    """
    if use_primary and not CONFIG.api_switched:
        result = api_football_get(endpoint, params, retries)
        if result:
            return result
        logger.info("🔄 Falling back to Football-Data.org...")
        CONFIG.api_switched = True
    
    # Use Football-Data.org as fallback
    return football_data_get(endpoint, params, retries)

# ─────────────────────────────────────────────────────────────────────────────
# DATA PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def parse_fixtures_response_af(data: Dict) -> List[Dict]:
    """Parse API-Football /fixtures response"""
    fixtures = []
    
    if not data or "response" not in data:
        return fixtures
    
    for f in data["response"]:
        fixture = f.get("fixture", {})
        league = f.get("league", {})
        teams = f.get("teams", {})
        goals = f.get("goals", {})
        status = fixture.get("status", {})
        
        match_id = str(fixture.get("id", ""))
        if not match_id:
            continue
        
        date_str = fixture.get("date", "")
        kickoff_utc = None
        try:
            if date_str:
                kickoff_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            pass
        
        status_short = status.get("short", "NS")
        internal_status = "live" if status_short in ["1H", "2H", "HT", "ET"] else "completed" if status_short in ["FT", "AET", "PEN"] else "upcoming"
        
        fixtures.append({
            "match_id": match_id,
            "af_id": match_id,
            "home_team": teams.get("home", {}).get("name", "Unknown"),
            "home_team_id": str(teams.get("home", {}).get("id", "")),
            "away_team": teams.get("away", {}).get("name", "Unknown"),
            "away_team_id": str(teams.get("away", {}).get("id", "")),
            "status": internal_status,
            "status_short": status_short,
            "kickoff_utc": kickoff_utc,
            "date_iso": date_str[:10] if date_str else "",
            "time": date_str[11:16] if date_str else "00:00",
            "home_score": goals.get("home") if goals else 0,
            "away_score": goals.get("away") if goals else 0,
            "venue": fixture.get("venue", {}).get("name", ""),
            "round": league.get("round", ""),
            "source": "apifootball"
        })
    
    return fixtures

def parse_fixtures_response_fd(data: Dict) -> List[Dict]:
    """Parse Football-Data.org matches response"""
    fixtures = []
    
    if not data or "matches" not in data:
        return fixtures
    
    for match in data["matches"]:
        match_id = str(match.get("id", ""))
        if not match_id:
            continue
        
        date_str = match.get("utcDate", "")
        kickoff_utc = None
        try:
            if date_str:
                kickoff_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            pass
        
        status_text = match.get("status", "SCHEDULED")
        status_map = {
            "SCHEDULED": "upcoming", "TIMED": "upcoming",
            "LIVE": "live", "IN_PLAY": "live", "PAUSED": "live",
            "FINISHED": "completed", "POSTPONED": "upcoming", "CANCELLED": "upcoming"
        }
        internal_status = status_map.get(status_text, "upcoming")
        
        home_team = match.get("homeTeam", {})
        away_team = match.get("awayTeam", {})
        score = match.get("score", {})
        full_time = score.get("fullTime", {})
        
        fixtures.append({
            "match_id": match_id,
            "fd_id": match_id,
            "home_team": home_team.get("name", "Unknown"),
            "home_team_id": str(home_team.get("id", "")),
            "away_team": away_team.get("name", "Unknown"),
            "away_team_id": str(away_team.get("id", "")),
            "status": internal_status,
            "status_text": status_text,
            "kickoff_utc": kickoff_utc,
            "date_iso": date_str[:10] if date_str else "",
            "time": date_str[11:16] if date_str else "00:00",
            "home_score": full_time.get("home") if full_time else 0,
            "away_score": full_time.get("away") if full_time else 0,
            "venue": match.get("venue", ""),
            "stage": match.get("stage", ""),
            "group": match.get("group", ""),
            "source": "footballdata"
        })
    
    return fixtures

def parse_lineups_response_af(data: Dict) -> Optional[Dict]:
    """Parse API-Football lineups"""
    if not data or "response" not in data:
        return None
    
    lineups = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }
    
    side_index = 0
    for lineup in data["response"]:
        side = "home" if side_index == 0 else "away"
        side_index += 1
        
        team = lineup.get("team", {})
        formation = lineup.get("formation", "4-4-2")
        lineups[side]["formation"] = formation
        
        coach = lineup.get("coach", {})
        lineups[side]["coach"] = {"name": coach.get("name", "Unknown")}
        
        for player in lineup.get("startXI", []):
            p = player.get("player", {})
            lineups[side]["players"].append({
                "name": p.get("name", "Unknown"),
                "position": p.get("pos", "Unknown"),
                "jerseyNumber": p.get("number", 0),
                "captain": False,
                "lineup": True,
                "playerId": str(p.get("id", "")),
            })
        
        for player in lineup.get("substitutes", []):
            p = player.get("player", {})
            lineups[side]["bench"].append({
                "name": p.get("name", "Unknown"),
                "position": p.get("pos", "Unknown"),
                "jerseyNumber": p.get("number", 0),
                "captain": False,
                "lineup": False,
                "playerId": str(p.get("id", "")),
            })
    
    return lineups

def parse_lineups_response_fd(data: Dict) -> Optional[Dict]:
    """Parse Football-Data.org lineups"""
    if not data or "lineups" not in data:
        return None
    
    lineups = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }
    
    side_index = 0
    for lineup in data.get("lineups", []):
        side = "home" if side_index == 0 else "away"
        side_index += 1
        
        team = lineup.get("team", {})
        formation = lineup.get("formation", "4-4-2")
        lineups[side]["formation"] = formation
        
        coach = lineup.get("coach", {})
        lineups[side]["coach"] = {"name": coach.get("name", "Unknown")}
        
        for player in lineup.get("startingXI", []):
            p = player.get("player", {})
            lineups[side]["players"].append({
                "name": p.get("name", "Unknown"),
                "position": p.get("position", "Unknown"),
                "jerseyNumber": p.get("shirtNumber", 0),
                "captain": False,
                "lineup": True,
                "playerId": str(p.get("id", "")),
            })
        
        for player in lineup.get("substitutes", []):
            p = player.get("player", {})
            lineups[side]["bench"].append({
                "name": p.get("name", "Unknown"),
                "position": p.get("position", "Unknown"),
                "jerseyNumber": p.get("shirtNumber", 0),
                "captain": False,
                "lineup": False,
                "playerId": str(p.get("id", "")),
            })
    
    return lineups

# ─────────────────────────────────────────────────────────────────────────────
# BACKEND API COMMUNICATION
# ─────────────────────────────────────────────────────────────────────────────

def send_live_update(fixture_id: str, event_type: str, data: Dict) -> bool:
    """Send live update to backend"""
    payload = {
        "fixture_id": fixture_id,
        "event_type": event_type,
        "home_score": data.get("home_score", 0),
        "away_score": data.get("away_score", 0),
        "minute": data.get("minute", 0),
        "minute_display": data.get("minute_display", f"{data.get('minute', 0)}'"),
        "player": data.get("player"),
        "assist": data.get("assist"),
        "team": data.get("team"),
        "player_out": data.get("player_out"),
        "player_in": data.get("player_in"),
    }
    
    try:
        r = std_requests.post(f"{CONFIG.fanclash_api}/games/live-update", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Live update sent: {event_type}")
            return True
        else:
            logger.warning(f"❌ Live update failed: {r.status_code}")
            return False
    except Exception as e:
        logger.error(f"send_live_update error: {e}")
        return False

def send_commentary(match_id: str, entry: Dict) -> bool:
    """Send commentary to backend"""
    payload = {"match_id": match_id, "entry": entry}
    try:
        r = std_requests.post(f"{CONFIG.fanclash_api}/games/commentary", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Commentary sent: {entry.get('event_type')}")
            return True
        return False
    except Exception as e:
        logger.error(f"send_commentary error: {e}")
        return False

def update_game_status(match_id: str, status: str, is_live: bool = False) -> bool:
    """Update game status in backend"""
    payload = {"match_id": match_id, "status": status, "is_live": is_live}
    try:
        r = std_requests.put(f"{CONFIG.fanclash_api}/games/{match_id}/status", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Status updated: {status}")
            return True
        return False
    except Exception as e:
        logger.error(f"update_game_status error: {e}")
        return False

def send_lineups(fixture_id: str, lineups: Dict) -> bool:
    """Send lineups to backend"""
    payload = {
        "fixture_id": fixture_id,
        "lineups": lineups,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    try:
        r = std_requests.post(f"{CONFIG.fanclash_api}/games/lineups", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Lineups sent for {fixture_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"send_lineups error: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# SLEEP CALCULATION
# ─────────────────────────────────────────────────────────────────────────────

def calculate_sleep_duration(closest_match: Dict) -> float:
    """Calculate optimal sleep duration based on closest match"""
    if not closest_match:
        return 3600
    
    kickoff = closest_match.get("kickoff_utc")
    if not kickoff:
        return 3600
    
    now = datetime.now(timezone.utc)
    minutes_until = (kickoff - now).total_seconds() / 60
    
    # More than 3 hours away -> sleep until exactly 3 hours before
    if minutes_until > 180:
        sleep_until = kickoff - timedelta(hours=3)
        sleep_seconds = (sleep_until - now).total_seconds()
        return max(60, sleep_seconds)
    
    # 1-3 hours away -> sleep 1 hour
    elif minutes_until > 60:
        return 3600
    
    # Less than 1 hour -> sleep 30 seconds
    else:
        return 30

# ─────────────────────────────────────────────────────────────────────────────
# LINEUP POLLING
# ─────────────────────────────────────────────────────────────────────────────

def poll_lineups_continuous(fixture: Dict, kickoff_utc: datetime):
    """Poll for lineups continuously until found"""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    poll_count = 0
    
    logger.info(f"🔍 Starting continuous lineup polling for {label}")
    
    with lineup_polling_lock:
        lineup_polling_active[match_id] = True
    
    try:
        while True:
            poll_count += 1
            
            # Try API-Football first
            data = api_football_get(f"/fixtures/lineups", {"fixture": match_id})
            if data:
                lineups = parse_lineups_response_af(data)
                if lineups and (lineups["home"]["players"] or lineups["away"]["players"]):
                    logger.info(f"✅✅✅ LINEUPS FOUND (API-Football) for {label}!")
                    if send_lineups(match_id, lineups):
                        if fixture.get("_col"):
                            fixture["_col"].update_one(
                                {"match_id": match_id},
                                {"$set": {"lineups_fetched": True, "lineups_source": "apifootball"}}
                            )
                        return
            
            # Fallback to Football-Data.org
            data = football_data_get(f"/matches/{match_id}/lineups")
            if data:
                lineups = parse_lineups_response_fd(data)
                if lineups and (lineups["home"]["players"] or lineups["away"]["players"]):
                    logger.info(f"✅✅✅ LINEUPS FOUND (Football-Data.org) for {label}!")
                    if send_lineups(match_id, lineups):
                        if fixture.get("_col"):
                            fixture["_col"].update_one(
                                {"match_id": match_id},
                                {"$set": {"lineups_fetched": True, "lineups_source": "footballdata"}}
                            )
                        return
            
            logger.warning(f"⚠️ No lineups found for {label}, attempt {poll_count}")
            time.sleep(CONFIG.lineup_poll_interval_sec)
            
    finally:
        with lineup_polling_lock:
            lineup_polling_active.pop(match_id, None)

# ─────────────────────────────────────────────────────────────────────────────
# LIVE MATCH POLLING
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_match(fixture: Dict):
    """Main live match polling loop"""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    
    logger.info(f"🔴 Starting live poll for {label}")
    
    # Get initial state
    data = hybrid_get(f"/fixtures", {"id": match_id}, use_primary=not CONFIG.api_switched)
    if data:
        fixtures = parse_fixtures_response_af(data)
        initial = fixtures[0] if fixtures else None
    else:
        data = football_data_get(f"/matches/{match_id}")
        fixtures = parse_fixtures_response_fd(data)
        initial = fixtures[0] if fixtures else None
    
    if initial and initial["status"] == "completed":
        logger.info(f"Match already completed: {label}")
        update_game_status(match_id, "completed")
        return
    
    # Set live status
    update_game_status(match_id, "live", is_live=True)
    
    last_home = initial["home_score"] if initial else 0
    last_away = initial["away_score"] if initial else 0
    half_time_sent = False
    full_time_sent = False
    second_half_sent = False
    
    while True:
        try:
            # Try API-Football first
            data = hybrid_get(f"/fixtures", {"id": match_id}, use_primary=not CONFIG.api_switched)
            
            if data:
                fixtures_data = parse_fixtures_response_af(data)
                if fixtures_data:
                    live = fixtures_data[0]
                    home_score = live["home_score"]
                    away_score = live["away_score"]
                    status = live["status"]
                    status_short = live.get("status_short", "")
                    
                    logger.info(f"📊 {label}: {home_score}-{away_score} ({status_short})")
                    
                    # Check goals
                    if home_score > last_home:
                        logger.info(f"⚽ GOAL! {fixture['home_team']} scores! ({home_score}-{away_score})")
                        send_live_update(match_id, "goal", {
                            "minute": 0,
                            "minute_display": "?",
                            "home_score": home_score,
                            "away_score": away_score,
                            "team": fixture["home_team"],
                            "player": "Unknown",
                        })
                        last_home = home_score
                    
                    if away_score > last_away:
                        logger.info(f"⚽ GOAL! {fixture['away_team']} scores! ({home_score}-{away_score})")
                        send_live_update(match_id, "goal", {
                            "minute": 0,
                            "minute_display": "?",
                            "home_score": home_score,
                            "away_score": away_score,
                            "team": fixture["away_team"],
                            "player": "Unknown",
                        })
                        last_away = away_score
                    
                    # Half Time
                    if status_short == "HT" and not half_time_sent:
                        logger.info(f"⏸ HALF TIME: {home_score}-{away_score}")
                        half_time_sent = True
                    
                    # Full Time
                    if status == "completed" and not full_time_sent:
                        logger.info(f"🏁 FULL TIME: {home_score}-{away_score}")
                        update_game_status(match_id, "completed")
                        full_time_sent = True
                        break
            else:
                # Fallback to Football-Data.org
                data = football_data_get(f"/matches/{match_id}")
                if data:
                    fixtures_data = parse_fixtures_response_fd(data)
                    if fixtures_data:
                        live = fixtures_data[0]
                        home_score = live["home_score"]
                        away_score = live["away_score"]
                        status_text = live.get("status_text", "")
                        
                        # Update similarly...
                        if home_score > last_home:
                            last_home = home_score
                        if away_score > last_away:
                            last_away = away_score
                        if live["status"] == "completed" and not full_time_sent:
                            update_game_status(match_id, "completed")
                            full_time_sent = True
                            break
            
            time.sleep(CONFIG.poll_interval_sec)
            
        except Exception as e:
            logger.error(f"Error in poll loop: {e}")
            time.sleep(CONFIG.poll_interval_sec)
    
    logger.info(f"✅ Done polling {label}")

# ─────────────────────────────────────────────────────────────────────────────
# POLL QUEUE
# ─────────────────────────────────────────────────────────────────────────────

def queue_worker():
    """Background worker for processing match polls"""
    logger.info("🔁 Poll queue worker started")
    
    while True:
        try:
            task = poll_queue.get(timeout=5)
            if task is None:
                break
            
            fixture = task
            match_id = fixture["match_id"]
            label = f"{fixture['home_team']} vs {fixture['away_team']}"
            
            with polls_lock:
                if match_id in active_polls:
                    logger.info(f"Already polling {label}, skipping")
                    poll_queue.task_done()
                    continue
                active_polls.add(match_id)
            
            try:
                poll_live_match(fixture)
            except Exception as e:
                logger.error(f"Poll error for {label}: {e}")
            finally:
                with polls_lock:
                    active_polls.discard(match_id)
                poll_queue.task_done()
                
        except _queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Queue worker error: {e}")

def start_polling(fixture: Dict):
    """Add match to polling queue"""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    
    with polls_lock:
        if match_id in active_polls:
            logger.info(f"Already polling {label}")
            return
    
    global queue_worker_started
    with queue_lock:
        if not queue_worker_started:
            threading.Thread(target=queue_worker, daemon=True, name="poll-worker").start()
            queue_worker_started = True
    
    poll_queue.put(fixture)
    logger.info(f"📥 Queued: {label}")

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    try:
        client = MongoClient(CONFIG.database_url, serverSelectionTimeoutMS=15000)
        client.admin.command("ping")
        col = client[CONFIG.db_name][CONFIG.collection_name]
        logger.info(f"✅ Connected to {CONFIG.db_name}.{CONFIG.collection_name}")
        return client, col
    except Exception as e:
        logger.warning(f"⚠️ MongoDB failed: {e}")
        return None, None

def load_fixtures(col) -> List[Dict]:
    if col is None:
        return []
    
    fixtures = []
    for f in col.find({"status": {"$ne": "completed"}, "league": CONFIG.world_cup_label}):
        kickoff_utc = f.get("kickoff_utc")
        if isinstance(kickoff_utc, str):
            try:
                kickoff_utc = datetime.fromisoformat(kickoff_utc.replace("Z", "+00:00"))
            except Exception:
                kickoff_utc = None
        
        fixtures.append({
            "match_id": f.get("match_id"),
            "af_id": f.get("af_id"),
            "fd_id": f.get("fd_id"),
            "home_team": f.get("home_team"),
            "away_team": f.get("away_team"),
            "status": f.get("status", "upcoming"),
            "date_iso": f.get("date_iso", ""),
            "time": f.get("time", "00:00"),
            "kickoff_utc": kickoff_utc,
            "lineups_fetched": f.get("lineups_fetched", False),
            "_col": col,
        })
    
    fixtures.sort(key=lambda x: x["kickoff_utc"] or datetime.max.replace(tzinfo=timezone.utc))
    return fixtures

def update_fixtures_from_api(col):
    """Fetch fixtures from both APIs and merge"""
    logger.info("📡 Fetching fixtures from APIs...")
    
    # Try API-Football first
    data = api_football_get("/fixtures", {"league": CONFIG.wc_league_id, "season": CONFIG.wc_season})
    if data:
        fixtures = parse_fixtures_response_af(data)
        logger.info(f"✅ API-Football: Found {len(fixtures)} fixtures")
    else:
        # Fallback to Football-Data.org
        data = football_data_get(f"/competitions/{CONFIG.fd_wc_code}/matches")
        if data:
            fixtures = parse_fixtures_response_fd(data)
            logger.info(f"✅ Football-Data.org: Found {len(fixtures)} fixtures")
        else:
            logger.error("❌ Both APIs failed to fetch fixtures")
            return []
    
    # Update database with both IDs
    for fixture in fixtures:
        match_id = fixture.get("match_id") or fixture.get("af_id") or fixture.get("fd_id")
        if not match_id:
            continue
        
        update_data = {
            "home_team": fixture.get("home_team"),
            "home_team_id": fixture.get("home_team_id"),
            "away_team": fixture.get("away_team"),
            "away_team_id": fixture.get("away_team_id"),
            "status": fixture.get("status"),
            "date_iso": fixture.get("date_iso", ""),
            "time": fixture.get("time", "00:00"),
            "kickoff_utc": fixture["kickoff_utc"].isoformat() if fixture.get("kickoff_utc") else None,
            "venue": fixture.get("venue", ""),
            "league": CONFIG.world_cup_label,
        }
        
        # Add source-specific IDs
        if "af_id" in fixture:
            update_data["af_id"] = fixture["af_id"]
        if "fd_id" in fixture:
            update_data["fd_id"] = fixture["fd_id"]
        if fixture.get("source"):
            update_data["source"] = fixture["source"]
        
        col.update_one(
            {"$or": [{"match_id": match_id}, {"af_id": match_id}, {"fd_id": match_id}]},
            {"$set": update_data},
            upsert=True
        )
    
    return fixtures

# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash World Cup 2026 Hybrid Poller")
    logger.info("📡 Primary: API-Football.com | Fallback: Football-Data.org")
    logger.info("=" * 65)
    
    start_health_server()
    
    mongo_client, col = connect_db()
    fixtures = update_fixtures_from_api(col)
    
    if not fixtures:
        logger.warning("No fixtures found")
        return
    
    logger.info(f"📋 Loaded {len(fixtures)} fixtures")
    lineup_polling_started = set()
    
    while True:
        try:
            now = datetime.now(timezone.utc)
            fixtures = load_fixtures(col)
            
            live_matches = [f for f in fixtures if f.get("status") == "live"]
            if live_matches:
                logger.info(f"🔴 {len(live_matches)} live matches")
                for match in live_matches:
                    start_polling(match)
                time.sleep(CONFIG.live_check_interval_sec)
                continue
            
            upcoming = [f for f in fixtures if f.get("kickoff_utc") and f["kickoff_utc"] > now]
            upcoming.sort(key=lambda x: x["kickoff_utc"])
            
            if not upcoming:
                time.sleep(3600)
                continue
            
            closest = upcoming[0]
            minutes_until = (closest["kickoff_utc"] - now).total_seconds() / 60
            
            logger.info(f"📅 Next match: {closest['home_team']} vs {closest['away_team']}")
            logger.info(f"   {minutes_until:.0f} minutes from now")
            
            # API status check
            with CONFIG.request_lock:
                if CONFIG.api_requests["apifootball"] >= CONFIG.api_limit:
                    logger.warning(f"⚠️ API-Football limit reached. Using Football-Data.org only.")
                    CONFIG.api_switched = True
            
            # Lineup polling
            if minutes_until <= CONFIG.lineup_poll_start_min and minutes_until > 0:
                match_id = closest['match_id']
                if match_id not in lineup_polling_started and not closest.get("lineups_fetched"):
                    logger.info(f"🎯 STARTING LINEUP POLLING")
                    threading.Thread(
                        target=poll_lineups_continuous,
                        args=(closest, closest["kickoff_utc"]),
                        daemon=True
                    ).start()
                    lineup_polling_started.add(match_id)
            
            # Match start
            if minutes_until <= 5:
                if minutes_until > 0 and closest.get("status") != "soon":
                    update_game_status(closest["match_id"], "soon", is_live=False)
                
                if minutes_until <= 0:
                    if closest.get("status") != "live":
                        update_game_status(closest["match_id"], "live", is_live=True)
                    start_polling(closest)
                    time.sleep(5)
                    continue
            
            sleep_seconds = 30 if minutes_until <= CONFIG.lineup_poll_start_min else calculate_sleep_duration(closest)
            logger.info(f"💤 Sleeping for {sleep_seconds:.0f} seconds")
            time.sleep(sleep_seconds)
            
            # Refresh fixtures
            if datetime.now(timezone.utc).minute % 15 == 0:
                update_fixtures_from_api(col)
            
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down...")
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(60)
    
    if mongo_client:
        mongo_client.close()
        logger.info("Database closed")

if __name__ == "__main__":
    main()