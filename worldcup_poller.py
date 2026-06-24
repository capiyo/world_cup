"""
World Cup 2026 Poller - Football-Data.org Only
================================================
- Uses Football-Data.org API (working for 2026 World Cup)
- IMMEDIATELY removes completed games
- Clean, simple, no unnecessary API switching
- Full health monitoring
"""

import os
import time
import logging
import threading
import queue as _queue
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json

import requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Config:
    world_cup_label: str = "World Cup 2026"
    fd_api_key: str = os.getenv("FD_API_KEY", "d6016bacb4a44b41a554cf1aa7973d72")
    fd_base: str = "https://api.football-data.org/v4"
    fd_wc_code: str = "WC"
    
    database_url: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name: str = "clashdb"
    collection_name: str = "fixtures"
    
    fanclash_api: str = os.environ.get("FANCLASH_API", "http://localhost:5000/api")
    
    # Polling intervals
    poll_interval_sec: int = 60
    lineup_poll_interval_sec: int = 120
    live_check_interval_sec: int = 60
    
    # Lineup polling starts 90 minutes before kickoff
    lineup_poll_start_min: int = 90

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

active_polls = set()
polls_lock = threading.Lock()
lineup_polling_active = {}
lineup_polling_lock = threading.Lock()
poll_queue = _queue.Queue()
queue_worker_started = False
queue_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# HEALTH SERVER
# ─────────────────────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            status = {
                "status": "ok",
                "service": "worldcup-poller",
                "api": "football-data.org",
                "active_polls": len(active_polls),
                "queue_size": poll_queue.qsize()
            }
            body = json.dumps(status).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = b"FanClash WorldCup Poller Running"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body)

def start_health_server():
    port = int(os.environ.get("PORT", 8081))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"🌐 Health server on port {port}")

# ─────────────────────────────────────────────────────────────────────────────
# FOOTBALL-DATA.ORG CLIENT
# ─────────────────────────────────────────────────────────────────────────────

session = None
session_lock = threading.Lock()
api_semaphore = threading.Semaphore(1)

def get_session():
    global session
    with session_lock:
        if session is None:
            session = requests.Session()
            session.headers.update({
                "X-Auth-Token": CONFIG.fd_api_key,
                "Accept": "application/json",
            })
        return session

def fd_get(endpoint: str, params: Dict = None, retries: int = 3) -> Optional[Dict]:
    """Fetch from Football-Data.org with retry logic"""
    url = f"{CONFIG.fd_base}{endpoint}"
    
    for attempt in range(retries):
        try:
            with api_semaphore:
                time.sleep(1.0)
                resp = get_session().get(url, params=params, timeout=15)
            
            if resp.status_code == 200:
                return resp.json()
            
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            
            logger.warning(f"FD HTTP {resp.status_code} attempt {attempt+1}")
            time.sleep(5)
            
        except Exception as e:
            logger.warning(f"FD error attempt {attempt+1}: {e}")
            time.sleep(8)
    
    logger.error(f"FD all retries exhausted: {endpoint}")
    return None

# ─────────────────────────────────────────────────────────────────────────────
# DATA PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def parse_fixtures(data: Dict) -> List[Dict]:
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
            "SCHEDULED": "upcoming",
            "TIMED": "upcoming",
            "LIVE": "live",
            "IN_PLAY": "live",
            "PAUSED": "live",
            "FINISHED": "completed",
            "POSTPONED": "upcoming",
            "CANCELLED": "upcoming",
            "SUSPENDED": "completed"
        }
        internal_status = status_map.get(status_text, "upcoming")
        
        home_team = match.get("homeTeam", {})
        away_team = match.get("awayTeam", {})
        score = match.get("score", {})
        full_time = score.get("fullTime", {})
        half_time = score.get("halfTime", {})
        
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
            "half_time_home": half_time.get("home") if half_time else None,
            "half_time_away": half_time.get("away") if half_time else None,
            "venue": match.get("venue", ""),
            "stage": match.get("stage", ""),
            "group": match.get("group", ""),
        })
    
    return fixtures

def parse_lineups(data: Dict) -> Optional[Dict]:
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
    
    return lineups if (lineups["home"]["players"] or lineups["away"]["players"]) else None

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
        r = requests.post(f"{CONFIG.fanclash_api}/games/live-update", json=payload, timeout=5)
        return r.status_code == 200
    except Exception as e:
        logger.error(f"send_live_update error: {e}")
        return False

def send_commentary(match_id: str, entry: Dict) -> bool:
    """Send commentary to backend"""
    payload = {"match_id": match_id, "entry": entry}
    try:
        r = requests.post(f"{CONFIG.fanclash_api}/games/commentary", json=payload, timeout=5)
        return r.status_code == 200
    except Exception as e:
        logger.error(f"send_commentary error: {e}")
        return False

def update_game_status(match_id: str, status: str, is_live: bool = False) -> bool:
    """Update game status in backend"""
    payload = {"match_id": match_id, "status": status, "is_live": is_live}
    try:
        r = requests.put(f"{CONFIG.fanclash_api}/games/{match_id}/status", json=payload, timeout=5)
        return r.status_code == 200
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
        r = requests.post(f"{CONFIG.fanclash_api}/games/lineups", json=payload, timeout=5)
        return r.status_code == 200
    except Exception as e:
        logger.error(f"send_lineups error: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE OPERATIONS - IMMEDIATE CLEANUP
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
        logger.error(f"❌ MongoDB connection failed: {e}")
        return None, None

def remove_completed_game(col, match_id: str) -> bool:
    """
    IMMEDIATELY remove a completed game from database.
    Called as soon as a match finishes.
    """
    if col is None:
        return False
        
    result = col.delete_one({
        "match_id": match_id,
        "status": "completed"
    })
    
    if result.deleted_count > 0:
        logger.info(f"🗑️ IMMEDIATELY removed completed game: {match_id}")
        return True
    return False

def cleanup_completed_fixtures(col) -> int:
    """
    Clean up any completed fixtures that might still be in DB.
    Called regularly to ensure no completed games linger.
    """
    if col is None:
        return 0
        
    result = col.delete_many({
        "league": CONFIG.world_cup_label,
        "status": "completed"
    })
    
    if result.deleted_count > 0:
        logger.info(f"🗑️ Removed {result.deleted_count} completed fixtures from DB")
    
    return result.deleted_count

def load_fixtures(col) -> List[Dict]:
    """Load fixtures from database (completed games are auto-removed)"""
    if col is None:
        return []
    
    # Remove any completed fixtures that might still exist
    cleanup_completed_fixtures(col)
    
    fixtures = []
    for f in col.find({
        "league": CONFIG.world_cup_label
    }):
        # Skip if status is completed (shouldn't exist, but just in case)
        if f.get("status") == "completed":
            continue
            
        kickoff_utc = f.get("kickoff_utc")
        if isinstance(kickoff_utc, str):
            try:
                kickoff_utc = datetime.fromisoformat(kickoff_utc.replace("Z", "+00:00"))
            except Exception:
                kickoff_utc = None
        
        fixtures.append({
            "match_id": f.get("match_id"),
            "fd_id": f.get("fd_id"),
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

def update_fixtures_from_api(col) -> List[Dict]:
    """Fetch fixtures from Football-Data.org and update database"""
    if col is None:
        return []
        
    logger.info("📡 Fetching fixtures from Football-Data.org...")
    
    data = fd_get(f"/competitions/{CONFIG.fd_wc_code}/matches")
    if not data:
        logger.error("Failed to fetch fixtures")
        return []
    
    fixtures = parse_fixtures(data)
    logger.info(f"📋 Found {len(fixtures)} fixtures")
    
    # First, remove ALL completed fixtures from DB
    cleanup_completed_fixtures(col)
    
    # Update database - only store upcoming and live matches
    for fixture in fixtures:
        # Skip completed matches - we don't store them at all
        if fixture["status"] == "completed":
            # If it exists in DB, remove it
            remove_completed_game(col, fixture["match_id"])
            continue
        
        match_id = fixture["match_id"]
        col.update_one(
            {"fd_id": match_id},
            {"$set": {
                "match_id": match_id,
                "fd_id": match_id,
                "home_team": fixture["home_team"],
                "home_team_id": fixture["home_team_id"],
                "away_team": fixture["away_team"],
                "away_team_id": fixture["away_team_id"],
                "status": fixture["status"],
                "status_text": fixture["status_text"],
                "date_iso": fixture["date_iso"],
                "time": fixture["time"],
                "kickoff_utc": fixture["kickoff_utc"].isoformat() if fixture["kickoff_utc"] else None,
                "venue": fixture["venue"],
                "stage": fixture["stage"],
                "group": fixture["group"],
                "league": CONFIG.world_cup_label,
                "home_score": fixture["home_score"],
                "away_score": fixture["away_score"],
                "half_time_home": fixture["half_time_home"],
                "half_time_away": fixture["half_time_away"],
                "lineups_fetched": False,
            }},
            upsert=True
        )
    
    # Final cleanup - ensure no completed games remain
    cleanup_completed_fixtures(col)
    
    # Count stored fixtures
    stored = col.count_documents({"league": CONFIG.world_cup_label})
    logger.info(f"✅ Stored {stored} upcoming/live fixtures (completed removed)")
    
    return fixtures

# ─────────────────────────────────────────────────────────────────────────────
# LINEUP POLLING
# ─────────────────────────────────────────────────────────────────────────────

def poll_lineups_continuous(fixture: Dict):
    """Poll for lineups continuously until found"""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    poll_count = 0
    col = fixture.get("_col")
    
    logger.info(f"🔍 Starting lineup polling for {label}")
    
    with lineup_polling_lock:
        lineup_polling_active[match_id] = True
    
    try:
        while True:
            poll_count += 1
            
            data = fd_get(f"/matches/{match_id}/lineups")
            if data:
                lineups = parse_lineups(data)
                if lineups:
                    logger.info(f"✅✅✅ LINEUPS FOUND for {label}! ✅✅✅")
                    if send_lineups(match_id, lineups):
                        if col is not None:
                            col.update_one(
                                {"match_id": match_id},
                                {"$set": {"lineups_fetched": True}}
                            )
                        return
            
            time.sleep(CONFIG.lineup_poll_interval_sec)
            
    finally:
        with lineup_polling_lock:
            lineup_polling_active.pop(match_id, None)

# ─────────────────────────────────────────────────────────────────────────────
# LIVE MATCH POLLING
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_match(fixture: Dict):
    """Main live match polling loop with immediate cleanup on completion"""
    match_id = fixture["match_id"]
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    col = fixture.get("_col")
    
    logger.info(f"🔴 Starting live poll for {label}")
    
    # Get initial state
    data = fd_get(f"/matches/{match_id}")
    fixtures = parse_fixtures(data)
    initial = fixtures[0] if fixtures else None
    
    if initial and initial["status"] == "completed":
        logger.info(f"Match already completed: {label}")
        update_game_status(match_id, "completed")
        if col is not None:
            remove_completed_game(col, match_id)
        return
    
    # Final lineup check
    if not fixture.get("lineups_fetched"):
        logger.info(f"📋 Final lineup check for {label}")
        for attempt in range(3):
            data = fd_get(f"/matches/{match_id}/lineups")
            if data:
                lineups = parse_lineups(data)
                if lineups:
                    logger.info(f"✅ LINEUPS FOUND at kickoff for {label}!")
                    if send_lineups(match_id, lineups):
                        if col is not None:
                            col.update_one(
                                {"match_id": match_id},
                                {"$set": {"lineups_fetched": True}}
                            )
                        break
            time.sleep(30)
    
    # Set live status
    update_game_status(match_id, "live", is_live=True)
    
    # State tracking
    last_home = initial["home_score"] if initial else 0
    last_away = initial["away_score"] if initial else 0
    half_time_sent = False
    full_time_sent = False
    second_half_sent = False
    
    while True:
        try:
            data = fd_get(f"/matches/{match_id}")
            if not data:
                time.sleep(CONFIG.poll_interval_sec)
                continue
            
            fixtures_data = parse_fixtures(data)
            if not fixtures_data:
                time.sleep(CONFIG.poll_interval_sec)
                continue
            
            live = fixtures_data[0]
            home_score = live["home_score"]
            away_score = live["away_score"]
            status_text = live.get("status_text", "SCHEDULED")
            status = live["status"]
            
            logger.info(f"📊 {label}: {home_score}-{away_score} ({status_text})")
            
            # Check goals
            if home_score > last_home:
                logger.info(f"⚽ GOAL! {fixture['home_team']} scores!")
                send_live_update(match_id, "goal", {
                    "home_score": home_score,
                    "away_score": away_score,
                    "team": fixture["home_team"],
                    "player": "Unknown",
                })
                last_home = home_score
            
            if away_score > last_away:
                logger.info(f"⚽ GOAL! {fixture['away_team']} scores!")
                send_live_update(match_id, "goal", {
                    "home_score": home_score,
                    "away_score": away_score,
                    "team": fixture["away_team"],
                    "player": "Unknown",
                })
                last_away = away_score
            
            # Half Time
            if status_text == "PAUSED" and not half_time_sent:
                logger.info(f"⏸ HALF TIME: {home_score}-{away_score}")
                send_commentary(match_id, {
                    "text": f"⏸ HALF TIME: {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}",
                    "event_type": "half_time",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                half_time_sent = True
            
            # Second Half
            if status_text == "IN_PLAY" and half_time_sent and not second_half_sent:
                logger.info("▶️ SECOND HALF STARTED")
                send_commentary(match_id, {
                    "text": "▶️ SECOND HALF UNDERWAY!",
                    "event_type": "second_half",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                second_half_sent = True
            
            # Full Time - IMMEDIATELY REMOVE FROM DB
            if status == "completed" and not full_time_sent:
                logger.info(f"🏁 FULL TIME: {home_score}-{away_score}")
                update_game_status(match_id, "completed")
                send_commentary(match_id, {
                    "text": f"🏁 FULL TIME: {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}",
                    "event_type": "full_time",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                full_time_sent = True
                
                # IMMEDIATELY REMOVE from database
                if col is not None:
                    remove_completed_game(col, match_id)
                
                break
            
            time.sleep(CONFIG.poll_interval_sec)
            
        except Exception as e:
            logger.error(f"Error in poll loop: {e}")
            time.sleep(CONFIG.poll_interval_sec)
    
    logger.info(f"✅ Done polling {label} - removed from DB")

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
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash World Cup 2026 Poller")
    logger.info("📡 Using Football-Data.org (2026 World Cup Working!)")
    logger.info("🗑️ Completed games: IMMEDIATELY REMOVED from database")
    logger.info("=" * 65)
    
    start_health_server()
    
    mongo_client, col = connect_db()
    if col is None:
        logger.error("❌ Database connection failed")
        return
    
    # Initial fetch (completed games removed)
    fixtures = update_fixtures_from_api(col)
    if not fixtures:
        logger.warning("No fixtures found")
        return
    
    # Show only upcoming matches
    stored = col.count_documents({"league": CONFIG.world_cup_label})
    logger.info(f"📋 Stored {stored} upcoming fixtures (completed removed)")
    
    lineup_polling_started = set()
    
    while True:
        try:
            now = datetime.now(timezone.utc)
            
            # Clean up any completed fixtures that might have slipped through
            cleanup_completed_fixtures(col)
            
            # Load fixtures (completed games already removed)
            fixtures = load_fixtures(col)
            
            # Live matches
            live_matches = [f for f in fixtures if f.get("status") == "live"]
            if live_matches:
                logger.info(f"🔴 {len(live_matches)} live matches")
                for match in live_matches:
                    start_polling(match)
                time.sleep(CONFIG.live_check_interval_sec)
                continue
            
            # Upcoming matches
            upcoming = [f for f in fixtures if f.get("kickoff_utc") and f["kickoff_utc"] > now]
            upcoming.sort(key=lambda x: x["kickoff_utc"])
            
            if not upcoming:
                logger.info("No upcoming matches, sleeping 1 hour")
                time.sleep(3600)
                continue
            
            closest = upcoming[0]
            minutes_until = (closest["kickoff_utc"] - now).total_seconds() / 60
            
            logger.info(f"📅 Next match: {closest['home_team']} vs {closest['away_team']}")
            logger.info(f"   {minutes_until:.0f} minutes from now")
            
            # Lineup polling
            if minutes_until <= CONFIG.lineup_poll_start_min and minutes_until > 0:
                match_id = closest['match_id']
                if match_id not in lineup_polling_started and not closest.get("lineups_fetched"):
                    logger.info(f"🎯 STARTING LINEUP POLLING")
                    threading.Thread(
                        target=poll_lineups_continuous,
                        args=(closest,),
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
            
            # Smart sleep
            if minutes_until <= CONFIG.lineup_poll_start_min:
                sleep_seconds = 30
            else:
                sleep_seconds = min(3600, minutes_until * 60 * 0.5)
            
            logger.info(f"💤 Sleeping for {sleep_seconds:.0f} seconds")
            time.sleep(sleep_seconds)
            
            # Refresh fixtures every 15 minutes
            if datetime.now(timezone.utc).minute % 15 == 0:
                update_fixtures_from_api(col)
            
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down...")
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(60)
    
    if mongo_client is not None:
        mongo_client.close()
        logger.info("Database closed")

if __name__ == "__main__":
    main()