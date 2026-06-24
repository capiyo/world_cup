"""
World Cup 2026 — Football-Data.org Poller
============================================
Using Football-Data.org API endpoints:
- Matches: GET /v4/competitions/WC/matches
- Live: Filter by status = LIVE
- Lineups: GET /v4/matches/{id}/lineups
- Statistics: Not available in free tier (use alternative)
- Events: Not available in free tier (use alternative)

Maintains full structure with:
- Smart sleep until 3 hours before match
- Continuous lineup polling from 90 minutes before kickoff
- Live commentary via WebSocket broadcasting
- FCM notifications integration
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
from dataclasses import dataclass
from enum import Enum

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
    
    # Football-Data.org
    fd_api_key: str = os.getenv("FD_API_KEY", "d6016bacb4a44b41a554cf1aa7973d72")
    fd_base: str = "https://api.football-data.org/v4"
    fd_wc_code: str = "WC"  # World Cup competition code
    
    # Database
    database_url: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name: str = "clashdb"
    collection_name: str = "fixtures"
    
    # Backend
    fanclash_api: str = os.environ.get("FANCLASH_API", "http://localhost:5000/api")
    
    # Polling intervals
    poll_interval_sec: int = 30
    lineup_poll_interval_sec: int = 30
    hour_check_interval_sec: int = 3600
    scrape_interval_sec: int = 3600 * 6
    live_check_interval_sec: int = 30
    cleanup_interval_sec: int = 300
    
    # Lineup polling window (start 90 minutes before kickoff)
    lineup_poll_start_min: int = 90
    lineup_poll_end_min: int = 0
    
    # Match duration for completion detection
    match_duration_mins: int = 120
    
    # Nairobi offset (UTC+3)
    nairobi_offset: timedelta = timedelta(hours=3)
    
    # Default odds
    default_odds: Dict[str, float] = None
    
    def __post_init__(self):
        if self.default_odds is None:
            self.default_odds = {"home_win": 2.50, "away_win": 2.80, "draw": 3.20}

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

# ─────────────────────────────────────────────────────────────────────────────
# HEALTH SERVER
# ─────────────────────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body = b'{"status":"ok","service":"worldcup-poller"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = b"FanClash WorldCup Poller Running"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body)
    
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()
    
    def log_message(self, *args, **kwargs):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", 8081))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"🌐 Health server on port {port}")

# ─────────────────────────────────────────────────────────────────────────────
# FOOTBALL-DATA.ORG HTTP CLIENT
# ─────────────────────────────────────────────────────────────────────────────

_session: Optional[std_requests.Session] = None
_session_lock = threading.Lock()

def _make_session() -> std_requests.Session:
    s = std_requests.Session()
    s.headers.update({
        "X-Auth-Token": CONFIG.fd_api_key,
        "Accept": "application/json",
    })
    return s

def _get_session() -> std_requests.Session:
    global _session
    with _session_lock:
        if _session is None:
            _session = _make_session()
        return _session

def _reset_session():
    global _session
    with _session_lock:
        _session = _make_session()
    logger.info("🔄 Session reset")

def fd_get(endpoint: str, params: Dict = None, retries: int = 3, base_delay: float = 1.0) -> Optional[Dict]:
    """
    Fetch from Football-Data.org with retry logic.
    """
    url = f"{CONFIG.fd_base}{endpoint}"
    
    for attempt in range(retries):
        try:
            with api_semaphore:
                # Rate limiting - 10 requests per minute free tier
                time.sleep(random.uniform(base_delay, base_delay + 2.0))
                resp = _get_session().get(url, params=params, timeout=20)
            
            if resp.status_code == 200:
                return resp.json()
            
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            
            if resp.status_code == 403:
                logger.warning(f"API 403 attempt {attempt+1} — check API key")
                time.sleep(5)
                continue
            
            logger.warning(f"API HTTP {resp.status_code} attempt {attempt+1}: {resp.text[:200]}")
            time.sleep(5)
            
        except Exception as e:
            logger.warning(f"API error attempt {attempt+1}: {e}")
            time.sleep(8)
    
    logger.error(f"API all retries exhausted: {endpoint}")
    return None

# ─────────────────────────────────────────────────────────────────────────────
# FOOTBALL-DATA.ORG DATA PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def _map_fd_status(status: str) -> Tuple[str, int]:
    """
    Map Football-Data.org status to internal status and code.
    Statuses: SCHEDULED, LIVE, IN_PLAY, PAUSED, FINISHED, POSTPONED, CANCELLED
    """
    status_map = {
        "SCHEDULED": ("upcoming", 1),
        "TIMED": ("upcoming", 1),
        "LIVE": ("live", 2),
        "IN_PLAY": ("live", 2),
        "PAUSED": ("live", 3),
        "FINISHED": ("completed", 7),
        "POSTPONED": ("upcoming", 11),
        "CANCELLED": ("upcoming", 12),
        "SUSPENDED": ("completed", 10),
        "AWARDED": ("completed", 14),
    }
    return status_map.get(status, ("upcoming", 1))

def parse_fixtures_response(data: Dict) -> List[Dict]:
    """Parse Football-Data.org matches response"""
    fixtures = []
    
    if not data or "matches" not in data:
        return fixtures
    
    for match in data["matches"]:
        match_id = str(match.get("id", ""))
        if not match_id:
            continue
        
        # Parse date
        date_str = match.get("utcDate", "")
        kickoff_utc = None
        try:
            if date_str:
                kickoff_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            pass
        
        # Map status
        status_text = match.get("status", "SCHEDULED")
        internal_status, status_code = _map_fd_status(status_text)
        
        # Get teams
        home_team = match.get("homeTeam", {})
        away_team = match.get("awayTeam", {})
        
        # Get scores
        score = match.get("score", {})
        full_time = score.get("fullTime", {})
        half_time = score.get("halfTime", {})
        
        home_goals = full_time.get("home")
        away_goals = full_time.get("away")
        
        fixtures.append({
            "match_id": match_id,
            "api_id": match_id,
            "home_team": home_team.get("name", "Unknown"),
            "home_team_id": home_team.get("id", ""),
            "away_team": away_team.get("name", "Unknown"),
            "away_team_id": away_team.get("id", ""),
            "status": internal_status,
            "status_code": status_code,
            "status_text": status_text,
            "kickoff_utc": kickoff_utc,
            "date_iso": date_str[:10] if date_str else "",
            "time": date_str[11:16] if date_str else "00:00",
            "home_score": home_goals if home_goals is not None else 0,
            "away_score": away_goals if away_goals is not None else 0,
            "half_time_home": half_time.get("home"),
            "half_time_away": half_time.get("away"),
            "venue": match.get("venue", ""),
            "stage": match.get("stage", ""),
            "group": match.get("group", ""),
            "season": match.get("season", {}).get("id", CONFIG.fd_wc_code),
            "competition": match.get("competition", {}).get("name", "World Cup 2026"),
        })
    
    return fixtures

def parse_lineups_response(data: Dict, match_id: str) -> Optional[Dict]:
    """Parse Football-Data.org /matches/{id}/lineups response"""
    if not data or "lineups" not in data:
        return None
    
    lineups = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }
    
    for lineup in data.get("lineups", []):
        team = lineup.get("team", {})
        team_id = str(team.get("id", ""))
        
        # Determine side
        # We need to determine if this is home or away
        # We'll use the data to figure it out later
        
        side = "home" if not lineups["home"]["players"] else "away"
        
        # Get formation
        formation_str = lineup.get("formation", "4-4-2")
        lineups[side]["formation"] = formation_str
        
        # Get coach
        coach = lineup.get("coach", {})
        lineups[side]["coach"] = {
            "name": coach.get("name", "Unknown"),
            "id": str(coach.get("id", ""))
        }
        
        # Get starting XI
        for player in lineup.get("startingXI", []):
            player_data = player.get("player", {})
            lineups[side]["players"].append({
                "name": player_data.get("name", "Unknown"),
                "position": player_data.get("position", "Unknown"),
                "jerseyNumber": player_data.get("shirtNumber", 0),
                "captain": False,
                "lineup": True,
                "playerId": str(player_data.get("id", "")),
            })
        
        # Get substitutes
        for player in lineup.get("substitutes", []):
            player_data = player.get("player", {})
            lineups[side]["bench"].append({
                "name": player_data.get("name", "Unknown"),
                "position": player_data.get("position", "Unknown"),
                "jerseyNumber": player_data.get("shirtNumber", 0),
                "captain": False,
                "lineup": False,
                "playerId": str(player_data.get("id", "")),
            })
    
    logger.info(f"📊 Parsed lineups - Home: {len(lineups['home']['players'])} players, Away: {len(lineups['away']['players'])} players")
    
    if lineups["home"]["players"] or lineups["away"]["players"]:
        return lineups
    
    return None

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
    payload = {
        "match_id": match_id,
        "entry": entry
    }
    
    try:
        r = std_requests.post(f"{CONFIG.fanclash_api}/games/commentary", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Commentary sent: {entry.get('event_type')}")
            return True
        else:
            logger.warning(f"❌ Commentary failed: {r.status_code}")
            return False
    except Exception as e:
        logger.error(f"send_commentary error: {e}")
        return False

def update_game_status(match_id: str, status: str, is_live: bool = False) -> bool:
    """Update game status in backend"""
    payload = {
        "match_id": match_id,
        "status": status,
        "is_live": is_live
    }
    
    try:
        r = std_requests.put(f"{CONFIG.fanclash_api}/games/{match_id}/status", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Status updated: {status}")
            return True
        else:
            logger.warning(f"❌ Status update failed: {r.status_code}")
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
        else:
            logger.warning(f"❌ Lineups failed: {r.status_code}")
            return False
    except Exception as e:
        logger.error(f"send_lineups error: {e}")
        return False

def send_statistics(match_id: str, stats: Dict) -> bool:
    """Send statistics to backend (limited for Football-Data.org free tier)"""
    stats["match_id"] = match_id
    
    try:
        r = std_requests.post(f"{CONFIG.fanclash_api}/games/statistics", json=stats, timeout=5)
        if r.status_code == 200:
            logger.info(f"✅ Statistics sent for {match_id}")
            return True
        else:
            logger.warning(f"❌ Statistics failed: {r.status_code}")
            return False
    except Exception as e:
        logger.error(f"send_statistics error: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# LINEUP POLLING (CONTINUOUS WINDOW)
# ─────────────────────────────────────────────────────────────────────────────

def poll_lineups_continuous(fixture: Dict, kickoff_utc: datetime):
    """Poll for lineups continuously until found"""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    
    poll_count = 0
    
    logger.info(f"🔍 Starting continuous lineup polling for {label}")
    logger.info(f"   Will poll every {CONFIG.lineup_poll_interval_sec} seconds")
    
    with lineup_polling_lock:
        lineup_polling_active[match_id] = True
    
    try:
        while True:
            poll_count += 1
            
            # Fetch lineups from Football-Data.org
            data = fd_get(f"/matches/{match_id}/lineups", base_delay=1.0)
            
            if data:
                lineups = parse_lineups_response(data, match_id)
                if lineups and (lineups["home"]["players"] or lineups["away"]["players"]):
                    logger.info(f"✅✅✅ LINEUPS FOUND for {label}! ✅✅✅")
                    logger.info(f"   Home: {len(lineups['home']['players'])} players")
                    logger.info(f"   Away: {len(lineups['away']['players'])} players")
                    
                    if send_lineups(match_id, lineups):
                        if fixture.get("_col"):
                            fixture["_col"].update_one(
                                {"match_id": match_id},
                                {"$set": {"lineups_fetched": True}}
                            )
                        return
            else:
                logger.warning(f"⚠️ No response for lineup poll #{poll_count}")
            
            time.sleep(CONFIG.lineup_poll_interval_sec)
            
    finally:
        with lineup_polling_lock:
            lineup_polling_active.pop(match_id, None)

# ─────────────────────────────────────────────────────────────────────────────
# LIVE MATCH POLLING
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_match(fixture: Dict):
    """Main live match polling loop using Football-Data.org"""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    
    logger.info(f"🔴 Starting live poll for {label}")
    
    # Get initial state
    data = fd_get(f"/matches/{match_id}", base_delay=2.0)
    fixtures = parse_fixtures_response(data)
    initial = fixtures[0] if fixtures else None
    
    if initial and initial["status"] == "completed":
        logger.info(f"Match already completed: {label}")
        update_game_status(match_id, "completed")
        return
    
    # FINAL LINEUP CHECK - Poll multiple times if needed
    if not fixture.get("lineups_fetched"):
        logger.info(f"📋 Final lineup check for {label} - polling for 3 minutes")
        
        for attempt in range(6):
            logger.info(f"   Lineup check attempt {attempt + 1}/6")
            data = fd_get(f"/matches/{match_id}/lineups", base_delay=2.0)
            if data:
                lineups = parse_lineups_response(data, match_id)
                if lineups and (lineups["home"]["players"] or lineups["away"]["players"]):
                    logger.info(f"✅ LINEUPS FOUND at kickoff for {label}!")
                    if send_lineups(match_id, lineups):
                        if fixture.get("_col"):
                            fixture["_col"].update_one(
                                {"match_id": match_id},
                                {"$set": {"lineups_fetched": True}}
                            )
                        break
            
            if attempt < 5:
                time.sleep(30)
        else:
            logger.warning(f"⚠️ No lineups found for {label} after 3 minutes of polling")
    
    # Set live status
    update_game_status(match_id, "live", is_live=True)
    
    # State tracking
    last_home = initial["home_score"] if initial else 0
    last_away = initial["away_score"] if initial else 0
    half_time_sent = False
    full_time_sent = False
    second_half_sent = False
    last_commentary_time = time.time()
    
    while True:
        try:
            # Fetch live data
            data = fd_get(f"/matches/{match_id}", base_delay=1.0)
            if not data or "matches" not in data:
                time.sleep(CONFIG.poll_interval_sec)
                continue
            
            fixtures_data = parse_fixtures_response(data)
            if not fixtures_data:
                time.sleep(CONFIG.poll_interval_sec)
                continue
            
            live = fixtures_data[0]
            home_score = live["home_score"]
            away_score = live["away_score"]
            status = live["status"]
            status_code = live["status_code"]
            status_text = live.get("status_text", "SCHEDULED")
            
            # Football-Data.org doesn't provide elapsed time in free tier
            # We'll track time ourselves
            minute_disp = "??"
            
            logger.info(f"📊 {label}: {home_score}-{away_score} ({status_text})")
            
            # Check for goals (simplified - FD doesn't provide events in free tier)
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
                
                send_commentary(match_id, {
                    "minute": 0,
                    "minute_display": "?",
                    "text": f"⚽ GOAL! {fixture['home_team']} scores! ({home_score}-{away_score})",
                    "event_type": "goal",
                    "home_score": home_score,
                    "away_score": away_score,
                    "team": fixture["home_team"],
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
                
                send_commentary(match_id, {
                    "minute": 0,
                    "minute_display": "?",
                    "text": f"⚽ GOAL! {fixture['away_team']} scores! ({home_score}-{away_score})",
                    "event_type": "goal",
                    "home_score": home_score,
                    "away_score": away_score,
                    "team": fixture["away_team"],
                })
                last_away = away_score
            
            # Half Time (detected by status PAUSED)
            if status_text == "PAUSED" and not half_time_sent:
                logger.info(f"⏸ HALF TIME: {home_score}-{away_score}")
                send_live_update(match_id, "half_time", {
                    "minute": 0,
                    "minute_display": "HT",
                    "home_score": home_score,
                    "away_score": away_score
                })
                send_commentary(match_id, {
                    "minute": 0,
                    "minute_display": "HT",
                    "text": f"⏸ HALF TIME: {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}",
                    "event_type": "half_time",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                half_time_sent = True
            
            # Second Half Start
            if status_text == "IN_PLAY" and half_time_sent and not second_half_sent:
                logger.info("▶️ SECOND HALF STARTED")
                send_live_update(match_id, "second_half", {
                    "minute": 0,
                    "minute_display": "2H"
                })
                send_commentary(match_id, {
                    "minute": 0,
                    "minute_display": "2H",
                    "text": "▶️ SECOND HALF UNDERWAY!",
                    "event_type": "second_half",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                second_half_sent = True
            
            # Full Time
            if status == "completed" and not full_time_sent:
                logger.info(f"🏁 FULL TIME: {home_score}-{away_score}")
                send_live_update(match_id, "match_end", {
                    "minute": 0,
                    "minute_display": "FT",
                    "home_score": home_score,
                    "away_score": away_score
                })
                send_commentary(match_id, {
                    "minute": 0,
                    "minute_display": "FT",
                    "text": f"🏁 FULL TIME: {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}",
                    "event_type": "full_time",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                update_game_status(match_id, "completed")
                full_time_sent = True
                break
            
            time.sleep(CONFIG.poll_interval_sec)
            
        except Exception as e:
            logger.error(f"Error in poll loop for {label}: {e}")
            time.sleep(CONFIG.poll_interval_sec)
    
    logger.info(f"✅ Done polling {label}")

# ─────────────────────────────────────────────────────────────────────────────
# POLL QUEUE
# ─────────────────────────────────────────────────────────────────────────────

poll_queue = _queue.Queue()
queue_worker_started = False
queue_lock = threading.Lock()

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
    """Connect to MongoDB"""
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
    """Load fixtures from database"""
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
            "api_id": f.get("api_id", f.get("match_id")),
            "home_team": f.get("home_team"),
            "away_team": f.get("away_team"),
            "home_team_id": f.get("home_team_id", ""),
            "away_team_id": f.get("away_team_id", ""),
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
    """Fetch fixtures from Football-Data.org and update database"""
    logger.info("📡 Fetching fixtures from Football-Data.org...")
    
    data = fd_get(f"/competitions/{CONFIG.fd_wc_code}/matches")
    if not data:
        logger.error("Failed to fetch fixtures from Football-Data.org")
        return []
    
    fixtures = parse_fixtures_response(data)
    logger.info(f"📋 Found {len(fixtures)} fixtures from API")
    
    # Update database
    for fixture in fixtures:
        match_id = fixture["match_id"]
        col.update_one(
            {"match_id": match_id},
            {"$set": {
                "api_id": fixture["api_id"],
                "home_team": fixture["home_team"],
                "home_team_id": fixture["home_team_id"],
                "away_team": fixture["away_team"],
                "away_team_id": fixture["away_team_id"],
                "status": fixture["status"],
                "status_code": fixture["status_code"],
                "status_text": fixture["status_text"],
                "date_iso": fixture["date_iso"],
                "time": fixture["time"],
                "kickoff_utc": fixture["kickoff_utc"].isoformat() if fixture["kickoff_utc"] else None,
                "venue": fixture.get("venue", ""),
                "stage": fixture.get("stage", ""),
                "group": fixture.get("group", ""),
                "league": CONFIG.world_cup_label,
                "competition": fixture.get("competition", "World Cup 2026"),
                "season": fixture.get("season", CONFIG.fd_wc_code),
            }},
            upsert=True
        )
    
    return fixtures

# ─────────────────────────────────────────────────────────────────────────────
# SLEEP CALCULATION (SMART SLEEP)
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
    
    if minutes_until > 180:
        sleep_until = kickoff - timedelta(hours=3)
        sleep_seconds = (sleep_until - now).total_seconds()
        return max(60, sleep_seconds)
    elif minutes_until > 60:
        return 3600
    else:
        return 30

# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash World Cup 2026 Poller")
    logger.info("📡 Using Football-Data.org")
    logger.info("=" * 65)
    
    # Start health server
    start_health_server()
    
    # Connect to database
    mongo_client, col = connect_db()
    
    # Fetch fixtures from API
    fixtures = update_fixtures_from_api(col)
    if not fixtures:
        logger.warning("No fixtures found from Football-Data.org")
        return
    
    logger.info(f"📋 Loaded {len(fixtures)} fixtures")
    
    # Track which matches we've started lineup polling for
    lineup_polling_started = set()
    
    # Main loop
    while True:
        try:
            now = datetime.now(timezone.utc)
            fixtures = load_fixtures(col)
            
            # Find live matches
            live_matches = [f for f in fixtures if f.get("status") == "live"]
            
            if live_matches:
                logger.info(f"🔴 {len(live_matches)} live matches")
                for match in live_matches:
                    start_polling(match)
                time.sleep(CONFIG.live_check_interval_sec)
                continue
            
            # Find upcoming matches
            upcoming = [f for f in fixtures if f.get("kickoff_utc") and f["kickoff_utc"] > now]
            upcoming.sort(key=lambda x: x["kickoff_utc"])
            
            if not upcoming:
                logger.info("No upcoming matches, sleeping 1 hour")
                time.sleep(3600)
                continue
            
            closest = upcoming[0]
            minutes_until = (closest["kickoff_utc"] - now).total_seconds() / 60
            
            logger.info(f"📅 Next match: {closest['home_team']} vs {closest['away_team']}")
            logger.info(f"   Kickoff: {closest['kickoff_utc'].strftime('%Y-%m-%d %H:%M UTC')}")
            logger.info(f"   {minutes_until:.0f} minutes from now")
            
            # Start lineup polling at 90 minutes before kickoff
            if minutes_until <= CONFIG.lineup_poll_start_min and minutes_until > 0:
                match_id = closest['match_id']
                
                if match_id not in lineup_polling_started and not closest.get("lineups_fetched"):
                    logger.info(f"🎯 STARTING LINEUP POLLING for {closest['home_team']} vs {closest['away_team']}")
                    
                    threading.Thread(
                        target=poll_lineups_continuous,
                        args=(closest, closest["kickoff_utc"]),
                        daemon=True
                    ).start()
                    
                    lineup_polling_started.add(match_id)
            
            # Match start window
            if minutes_until <= 5:
                if minutes_until > 0 and closest.get("status") != "soon":
                    logger.info(f"⚽ Match starting soon!")
                    update_game_status(closest["match_id"], "soon", is_live=False)
                
                if minutes_until <= 0:
                    if closest.get("status") != "live":
                        logger.info(f"⚽ MATCH LIVE! Starting poller")
                        update_game_status(closest["match_id"], "live", is_live=True)
                    
                    start_polling(closest)
                    time.sleep(5)
                    continue
            
            # Smart sleep
            if minutes_until <= CONFIG.lineup_poll_start_min:
                sleep_seconds = 30
            else:
                sleep_seconds = calculate_sleep_duration(closest)
            
            logger.info(f"💤 Sleeping for {sleep_seconds:.0f} seconds")
            time.sleep(sleep_seconds)
            
            # Refresh fixtures periodically (every 15 minutes)
            if datetime.now(timezone.utc).minute % 15 == 0 and datetime.now(timezone.utc).second < 30:
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