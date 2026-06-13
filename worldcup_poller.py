"""
World Cup 2026 — Flashscore.ninja Scraper + Live Poller
=========================================================
Complete rewrite with:
- Smart sleep: deep sleep until 3 hours before match, then hourly, then continuous
- Continuous lineup polling from 58-53 minutes before kickoff
- Live commentary via WebSocket broadcasting
- Statistics fetching every 5 minutes
- FCM notifications integration
- Full compatibility with Rust backend
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
    fs_ninja_host: str = "global.flashscore.ninja"
    fs_feed_base: str = f"https://global.flashscore.ninja/2/x/feed/"
    x_fsign_token: str = "SW9D1eZo"
    
    # football-data.org
    fd_api_key: str = os.getenv("FD_API_KEY", "")
    fd_base: str = "https://api.football-data.org/v4"
    fd_wc_code: str = "WC"
    
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
    
    # Lineup polling window (58 to 53 minutes before kickoff)
    lineup_poll_start_min: int = 58
    lineup_poll_end_min: int = 53
    
    # Match duration for completion detection
    match_duration_mins: int = 120
    
    # Nairobi offset (UTC+3)
    nairobi_offset: timedelta = timedelta(hours=3)
    
    # Default odds
    default_odds: Dict[str, float] = None
    
    def __post_init__(self):
        if self.default_odds is None:
            self.default_odds = {"home_win": 2.50, "away_win": 2.80, "draw": 3.20}
    
    @property
    def fs_feed_url(self) -> str:
        return self.fs_feed_base

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
fs_semaphore = threading.Semaphore(1)
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
# HTTP CLIENT WITH ROTATING USER AGENTS
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
]

_session: Optional[std_requests.Session] = None
_session_lock = threading.Lock()

def _make_session() -> std_requests.Session:
    s = std_requests.Session()
    s.headers.update({
        "Accept": "text/plain, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.flashscore.com/",
        "Origin": "https://www.flashscore.com",
        "User-Agent": random.choice(USER_AGENTS),
        "X-Fsign": CONFIG.x_fsign_token,
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

def fs_get(query: str, retries: int = 5, base_delay: float = 2.0) -> Optional[str]:
    """Fetch from Flashscore with retry logic"""
    url = f"{CONFIG.fs_feed_url}{query}"
    session_reset_done = False
    
    for attempt in range(retries):
        try:
            with fs_semaphore:
                time.sleep(random.uniform(base_delay, base_delay + 2.0))
                resp = _get_session().get(url, timeout=20)
            
            if resp.status_code == 200:
                return resp.text
            
            if resp.status_code == 404:
                logger.debug(f"FS 404: {query}")
                return None
            
            if resp.status_code == 403:
                wait = (2 ** attempt) * random.uniform(4, 8)
                logger.warning(f"FS 403 attempt {attempt+1} — backoff {wait:.0f}s")
                time.sleep(wait)
                if not session_reset_done:
                    _reset_session()
                    session_reset_done = True
                continue
            
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"FS 429 — waiting {wait}s")
                time.sleep(wait)
                continue
            
            logger.warning(f"FS HTTP {resp.status_code} attempt {attempt+1}")
            time.sleep(5)
            
        except Exception as e:
            logger.warning(f"FS error attempt {attempt+1}: {e}")
            time.sleep(8)
    
    logger.error(f"FS all retries exhausted: {query}")
    return None

# ─────────────────────────────────────────────────────────────────────────────
# PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_rows(raw: str) -> List[Dict[str, str]]:
    """Parse Flashscore pipe-delimited format"""
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

_STRIP_TAGS = re.compile(r"<[^>]+>")

def _clean(s: str) -> str:
    return " ".join(_STRIP_TAGS.sub("", s).split())

def _map_status_code(code: int) -> str:
    if code in (100, 110, 111, 120, 121):
        return "completed"
    if code in (2, 3, 4, 5, 6, 7, 8, 9, 10, 41, 42):
        return "live"
    return "upcoming"

def _ts_to_eat(ts: int) -> Tuple[str, str, str]:
    """Convert timestamp to EAT (UTC+3)"""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + CONFIG.nairobi_offset
    return dt.strftime("%Y-%m-%d"), dt.strftime("%d %b"), dt.strftime("%H:%M")

def _safe_int(v: str) -> Optional[int]:
    v = v.strip()
    if v and v not in ("-", ""):
        try:
            return int(v)
        except ValueError:
            pass
    return None

def parse_live_feed(raw: str) -> Optional[Dict]:
    """Parse dc_{match_id} - live scores, status, time elapsed"""
    if not raw:
        return None
    
    for f in _parse_rows(raw):
        match_id = f.get("AA", "").strip()
        if not match_id:
            continue
        
        try:
            status_code = int(f.get("AB", "1"))
        except (ValueError, TypeError):
            status_code = 1
        
        status = _map_status_code(status_code)
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
        
        kickoff_ts = 0
        try:
            kickoff_ts = int(f.get("AD", 0))
        except (ValueError, TypeError):
            pass
        
        return {
            "status_code": status_code,
            "status": status,
            "home_score": home_score,
            "away_score": away_score,
            "time_elapsed": time_elapsed,
            "time_extra": time_extra,
            "kickoff_ts": kickoff_ts,
        }
    
    return None

def parse_incidents_feed(raw: str) -> List[Dict]:
    """Parse incidents (goals, cards, subs)"""
    incidents = []
    if not raw:
        return incidents
    
    for row in raw.split("~"):
        row = row.strip()
        if not row or "INC÷" not in row:
            continue
        for segment in row.split("¬"):
            if not segment.startswith("INC÷"):
                continue
            parts = segment[4:].split("÷")
            if len(parts) < 6:
                continue
            try:
                inc = {
                    "id": parts[0],
                    "type": parts[1].upper(),
                    "minute": int(parts[2]) if parts[2].isdigit() else 0,
                    "extra": int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0,
                    "is_home": parts[4] == "1",
                    "player": _clean(parts[5]) if len(parts) > 5 else "Unknown",
                    "assist": _clean(parts[6]) if len(parts) > 6 and parts[6].strip() else None,
                    "sub_out": _clean(parts[7]) if len(parts) > 7 and parts[7].strip() else None,
                }
                incidents.append(inc)
            except (ValueError, IndexError):
                continue
    
    return incidents

def parse_lineups_feed(raw: str) -> Optional[Dict]:
    """Parse lineups data"""
    if not raw:
        return None
    
    lineups = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }
    found_any = False
    
    for row in raw.split("~"):
        row = row.strip()
        if not row:
            continue
        for segment in row.split("¬"):
            segment = segment.strip()
            if not segment:
                continue
            
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
                        "position": parts[3] or "Unknown",
                        "jerseyNumber": jersey,
                        "captain": len(parts) > 7 and parts[7] == "1",
                        "lineup": is_starter,
                        "playerId": None,
                    }
                    lineups[side]["players" if is_starter else "bench"].append(player)
                    found_any = True
                except (ValueError, IndexError):
                    continue
            
            elif segment.startswith("CO÷"):
                parts = segment[3:].split("÷")
                if len(parts) >= 3:
                    side = "home" if parts[2] == "1" else "away"
                    lineups[side]["coach"]["name"] = _clean(parts[1]) or "Unknown"
    
    return lineups if found_any else None

def parse_statistics_feed(raw: str, time_elapsed: int, time_extra: int, home_score: int, away_score: int) -> Optional[Dict]:
    """Parse statistics data"""
    if not raw:
        return None
    
    KEY_MAP = {
        "possession_home": ("ball_possession_home", "ball_possession_away"),
        "ball possession": ("ball_possession_home", "ball_possession_away"),
        "shots_total": ("total_shots_home", "total_shots_away"),
        "total shots": ("total_shots_home", "total_shots_away"),
        "shots_on_target": ("shots_on_target_home", "shots_on_target_away"),
        "shots on target": ("shots_on_target_home", "shots_on_target_away"),
        "corner_kicks": ("corners_home", "corners_away"),
        "corner kicks": ("corners_home", "corners_away"),
        "fouls": ("fouls_home", "fouls_away"),
        "offsides": ("offsides_home", "offsides_away"),
        "yellow_cards": ("yellow_cards_home", "yellow_cards_away"),
        "yellow cards": ("yellow_cards_home", "yellow_cards_away"),
        "red_cards": ("red_cards_home", "red_cards_away"),
        "red cards": ("red_cards_home", "red_cards_away"),
    }
    
    stats = {}
    for row in raw.split("~"):
        for segment in row.split("¬"):
            if not segment.startswith("ST÷"):
                continue
            parts = segment[3:].split("÷")
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
    
    if not stats:
        return None
    
    minute_disp = f"{time_elapsed}" + (f"+{time_extra}" if time_extra else "")
    stats.update({
        "minute": time_elapsed,
        "minute_display": minute_disp,
        "home_score": home_score,
        "away_score": away_score,
    })
    return stats

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
    """Send statistics to backend"""
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

def check_lineups_exist(match_id: str) -> bool:
    """Check if lineups already exist in backend"""
    try:
        r = std_requests.get(f"{CONFIG.fanclash_api}/games/{match_id}/lineups", timeout=5)
        if r.status_code == 200:
            data = r.json()
            home_players = data.get("lineups", {}).get("home", {}).get("players", [])
            away_players = data.get("lineups", {}).get("away", {}).get("players", [])
            return bool(home_players or away_players)
        return False
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────────────
# LINEUP POLLING (CONTINUOUS WINDOW)
# ─────────────────────────────────────────────────────────────────────────────

def is_in_lineup_window(minutes_until: float) -> bool:
    """Check if within 58-53 minute window"""
    return CONFIG.lineup_poll_start_min <= minutes_until <= CONFIG.lineup_poll_end_min

def get_lineup_poll_deadline(kickoff_utc: datetime) -> float:
    """Get timestamp for end of lineup polling window"""
    deadline = kickoff_utc - timedelta(minutes=CONFIG.lineup_poll_end_min)
    return deadline.timestamp()

def poll_lineups_continuous(fixture: Dict, kickoff_utc: datetime):
    """Continuously poll for lineups during the 58-53 minute window"""
    match_id = fixture["match_id"]
    fs_id = fixture.get("flashscore_id") or match_id
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    
    deadline = get_lineup_poll_deadline(kickoff_utc)
    poll_count = 0
    
    logger.info(f"🔍 Starting lineup polling for {label} (window: {CONFIG.lineup_poll_start_min}-{CONFIG.lineup_poll_end_min} mins before)")
    
    with lineup_polling_lock:
        lineup_polling_active[match_id] = True
    
    try:
        while time.time() < deadline:
            poll_count += 1
            remaining = (deadline - time.time()) / 60
            logger.info(f"📋 Lineup poll #{poll_count} for {label} ({remaining:.1f} min left)")
            
            raw = fs_get(f"li_{fs_id}_1_en", base_delay=1.0)
            if raw:
                lineups = parse_lineups_feed(raw)
                if lineups and (lineups["home"]["players"] or lineups["away"]["players"]):
                    logger.info(f"✅ Lineups found for {label}!")
                    if send_lineups(match_id, lineups):
                        # Update MongoDB
                        if fixture.get("_col"):
                            fixture["_col"].update_one(
                                {"match_id": match_id},
                                {"$set": {"lineups_fetched": True}}
                            )
                        return
            
            time.sleep(CONFIG.lineup_poll_interval_sec)
        
        logger.info(f"⏰ Lineup polling window expired for {label}")
    
    finally:
        with lineup_polling_lock:
            lineup_polling_active.pop(match_id, None)

# ─────────────────────────────────────────────────────────────────────────────
# LIVE MATCH POLLING
# ─────────────────────────────────────────────────────────────────────────────

def _find_goal_scorer(incidents: List[Dict], is_home: bool) -> Tuple[str, Optional[str]]:
    """Find the most recent goal scorer"""
    for inc in reversed(incidents):
        if inc["type"] == "G" and inc["is_home"] == is_home:
            return inc.get("player", "Unknown"), inc.get("assist")
    return "Unknown", None

def poll_live_match(fixture: Dict):
    """Main live match polling loop"""
    match_id = fixture["match_id"]
    fs_id = fixture.get("flashscore_id") or match_id
    label = f"{fixture['home_team']} vs {fixture['away_team']}"
    
    logger.info(f"🔴 Starting live poll for {label}")
    
    # Get initial state
    initial = parse_live_feed(fs_get(f"dc_{fs_id}", base_delay=2.0))
    if initial and initial["status"] == "completed":
        logger.info(f"Match already completed: {label}")
        update_game_status(match_id, "completed")
        return
    
    # FINAL LINEUP CHECK - Poll multiple times if needed
    if not fixture.get("lineups_fetched"):
        logger.info(f"📋 Final lineup check for {label} - polling for 3 minutes")
        
        # Poll for lineups for up to 3 minutes (6 attempts)
        for attempt in range(6):
            logger.info(f"   Lineup check attempt {attempt + 1}/6")
            raw = fs_get(f"li_{fs_id}_1_en", base_delay=2.0)
            if raw:
                lineups = parse_lineups_feed(raw)
                if lineups and (lineups["home"]["players"] or lineups["away"]["players"]):
                    logger.info(f"✅ LINEUPS FOUND at kickoff for {label}!")
                    if send_lineups(match_id, lineups):
                        # Update MongoDB
                        if fixture.get("_col"):
                            fixture["_col"].update_one(
                                {"match_id": match_id},
                                {"$set": {"lineups_fetched": True}}
                            )
                        break
            
            if attempt < 5:  # Don't sleep after last attempt
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
    seen_incidents = set()
    last_stats_time = time.time()
    last_commentary_time = time.time()
    
    while True:
        try:
            # Fetch live data
            live_raw = fs_get(f"dc_{fs_id}", base_delay=1.0)
            if not live_raw:
                time.sleep(CONFIG.poll_interval_sec)
                continue
            
            live = parse_live_feed(live_raw)
            if not live:
                time.sleep(CONFIG.poll_interval_sec)
                continue
            
            # Fetch incidents
            incidents_raw = fs_get(f"d_hb_{fs_id}", base_delay=1.0)
            incidents = parse_incidents_feed(incidents_raw or "")
            
            home_score = live["home_score"]
            away_score = live["away_score"]
            status = live["status"]
            status_code = live["status_code"]
            time_elapsed = live["time_elapsed"]
            time_extra = live.get("time_extra", 0)
            minute_disp = f"{time_elapsed}" + (f"+{time_extra}" if time_extra else "")
            
            logger.info(f"📊 {label} @ {minute_disp}: {home_score}-{away_score}")
            
            # Check for goals
            if home_score > last_home:
                scorer, assist = _find_goal_scorer(incidents, is_home=True)
                logger.info(f"⚽ GOAL! {fixture['home_team']} - {scorer} ({minute_disp})")
                
                send_live_update(match_id, "goal", {
                    "minute": time_elapsed,
                    "minute_display": minute_disp,
                    "home_score": home_score,
                    "away_score": away_score,
                    "team": fixture["home_team"],
                    "player": scorer,
                    "assist": assist
                })
                
                send_commentary(match_id, {
                    "minute": time_elapsed,
                    "minute_display": minute_disp,
                    "text": f"⚽ GOAL! {scorer} scores! ({home_score}-{away_score})",
                    "event_type": "goal",
                    "home_score": home_score,
                    "away_score": away_score,
                    "team": fixture["home_team"],
                    "player": scorer,
                })
                last_home = home_score
            
            if away_score > last_away:
                scorer, assist = _find_goal_scorer(incidents, is_home=False)
                logger.info(f"⚽ GOAL! {fixture['away_team']} - {scorer} ({minute_disp})")
                
                send_live_update(match_id, "goal", {
                    "minute": time_elapsed,
                    "minute_display": minute_disp,
                    "home_score": home_score,
                    "away_score": away_score,
                    "team": fixture["away_team"],
                    "player": scorer,
                    "assist": assist
                })
                
                send_commentary(match_id, {
                    "minute": time_elapsed,
                    "minute_display": minute_disp,
                    "text": f"⚽ GOAL! {scorer} scores! ({home_score}-{away_score})",
                    "event_type": "goal",
                    "home_score": home_score,
                    "away_score": away_score,
                    "team": fixture["away_team"],
                    "player": scorer,
                })
                last_away = away_score
            
            # Process other incidents
            for inc in incidents:
                inc_id = inc.get("id", "")
                if inc_id in seen_incidents:
                    continue
                seen_incidents.add(inc_id)
                
                # Prevent memory bloat
                if len(seen_incidents) > 1000:
                    seen_incidents.clear()
                
                inc_type = inc["type"]
                if inc_type == "G":  # Already handled goals
                    continue
                
                is_home = inc["is_home"]
                team = fixture["home_team"] if is_home else fixture["away_team"]
                minute = inc["minute"]
                extra = inc.get("extra", 0)
                m_disp = f"{minute}" + (f"+{extra}" if extra else "")
                player = inc.get("player", "Unknown")
                
                # Yellow Card
                if inc_type == "YC":
                    send_live_update(match_id, "yellow_card", {
                        "minute": minute,
                        "minute_display": m_disp,
                        "player": player,
                        "team": team
                    })
                    send_commentary(match_id, {
                        "minute": minute,
                        "minute_display": m_disp,
                        "text": f"🟨 YELLOW CARD - {player} ({team})",
                        "event_type": "yellow_card",
                        "home_score": home_score,
                        "away_score": away_score,
                        "team": team,
                        "player": player,
                    })
                
                # Red Card
                elif inc_type == "RC":
                    send_live_update(match_id, "red_card", {
                        "minute": minute,
                        "minute_display": m_disp,
                        "player": player,
                        "team": team
                    })
                    send_commentary(match_id, {
                        "minute": minute,
                        "minute_display": m_disp,
                        "text": f"🟥 RED CARD - {player} ({team})",
                        "event_type": "red_card",
                        "home_score": home_score,
                        "away_score": away_score,
                        "team": team,
                        "player": player,
                    })
                
                # Substitution
                elif inc_type == "SB":
                    p_out = inc.get("sub_out") or "Unknown"
                    send_live_update(match_id, "substitution", {
                        "minute": minute,
                        "minute_display": m_disp,
                        "player_out": p_out,
                        "player_in": player,
                        "team": team
                    })
                    send_commentary(match_id, {
                        "minute": minute,
                        "minute_display": m_disp,
                        "text": f"🔄 SUB: {p_out} → {player} ({team})",
                        "event_type": "substitution",
                        "home_score": home_score,
                        "away_score": away_score,
                        "team": team,
                    })
            
            # Half Time
            if status_code == 3 and not half_time_sent:
                logger.info(f"⏸ HALF TIME: {home_score}-{away_score}")
                send_live_update(match_id, "half_time", {
                    "minute": time_elapsed,
                    "minute_display": f"{time_elapsed}'",
                    "home_score": home_score,
                    "away_score": away_score
                })
                send_commentary(match_id, {
                    "minute": time_elapsed,
                    "minute_display": f"{time_elapsed}'",
                    "text": f"⏸ HALF TIME: {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}",
                    "event_type": "half_time",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                half_time_sent = True
                
                # Fetch statistics at half time
                stats_raw = fs_get(f"od_{fs_id}", base_delay=2.0)
                if stats_raw:
                    stats = parse_statistics_feed(stats_raw, time_elapsed, time_extra, home_score, away_score)
                    if stats:
                        send_statistics(match_id, stats)
            
            # Second Half Start
            if status_code == 4 and half_time_sent and not second_half_sent:
                logger.info("▶️ SECOND HALF STARTED")
                send_live_update(match_id, "second_half", {
                    "minute": time_elapsed,
                    "minute_display": f"{time_elapsed}'"
                })
                send_commentary(match_id, {
                    "minute": time_elapsed,
                    "minute_display": f"{time_elapsed}'",
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
                    "minute": time_elapsed,
                    "minute_display": f"{time_elapsed}'",
                    "home_score": home_score,
                    "away_score": away_score
                })
                send_commentary(match_id, {
                    "minute": time_elapsed,
                    "minute_display": f"{time_elapsed}'",
                    "text": f"🏁 FULL TIME: {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}",
                    "event_type": "full_time",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                update_game_status(match_id, "completed")
                full_time_sent = True
                break
            
            # Periodic statistics (every 5 minutes)
            if time.time() - last_stats_time >= 300:
                stats_raw = fs_get(f"od_{fs_id}", base_delay=2.0)
                if stats_raw:
                    stats = parse_statistics_feed(stats_raw, time_elapsed, time_extra, home_score, away_score)
                    if stats:
                        send_statistics(match_id, stats)
                last_stats_time = time.time()
            
            # Periodic commentary heartbeat (every 30 seconds during live play)
            if time.time() - last_commentary_time >= 30 and status_code in (2, 4):
                send_commentary(match_id, {
                    "minute": time_elapsed,
                    "minute_display": minute_disp,
                    "text": f"⏱️ Match is underway at {minute_disp}",
                    "event_type": "heartbeat",
                    "home_score": home_score,
                    "away_score": away_score,
                })
                last_commentary_time = time.time()
            
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
    
    # Ensure worker is running
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
        date_iso = f.get("date_iso", "")
        time_str = f.get("time", "00:00")
        kickoff_utc = None
        
        if time_str and time_str != "TBD":
            try:
                naive_eat = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
                kickoff_utc = (naive_eat - CONFIG.nairobi_offset).replace(tzinfo=timezone.utc)
            except Exception:
                pass
        
        fixtures.append({
            "match_id": f.get("match_id"),
            "flashscore_id": f.get("flashscore_id"),
            "home_team": f.get("home_team"),
            "away_team": f.get("away_team"),
            "status": f.get("status", "upcoming"),
            "date_iso": date_iso,
            "time": time_str,
            "_kickoff_utc": kickoff_utc,
            "lineups_fetched": f.get("lineups_fetched", False),
            "_col": col,  # Store reference for updates
        })
    
    fixtures.sort(key=lambda x: x["_kickoff_utc"] or datetime.max.replace(tzinfo=timezone.utc))
    return fixtures

# ─────────────────────────────────────────────────────────────────────────────
# SLEEP CALCULATION (SMART SLEEP)
# ─────────────────────────────────────────────────────────────────────────────

def calculate_sleep_duration(closest_match: Dict) -> float:
    """Calculate optimal sleep duration based on closest match"""
    if not closest_match:
        return 3600  # Default 1 hour
    
    kickoff = closest_match.get("_kickoff_utc")
    if not kickoff:
        return 3600
    
    now = datetime.now(timezone.utc)
    minutes_until = (kickoff - now).total_seconds() / 60
    
    # More than 3 hours away -> sleep until exactly 3 hours before
    if minutes_until > 180:
        sleep_until = kickoff - timedelta(hours=3)
        sleep_seconds = (sleep_until - now).total_seconds()
        return max(60, sleep_seconds)  # At least 1 minute
    
    # 1-3 hours away -> sleep 1 hour
    elif minutes_until > 60:
        return 3600
    
    # Less than 1 hour -> sleep 30 seconds
    else:
        return 30

# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash World Cup 2026 Poller")
    logger.info("=" * 65)
    
    # Start health server
    start_health_server()
    
    # Connect to database
    mongo_client, col = connect_db()
    
    # Load fixtures
    fixtures = load_fixtures(col)
    if not fixtures:
        logger.warning("No fixtures found. Run scraper first.")
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
            upcoming = [f for f in fixtures if f.get("_kickoff_utc") and f.get("_kickoff_utc") > now]
            upcoming.sort(key=lambda x: x["_kickoff_utc"])
            
            if not upcoming:
                logger.info("No upcoming matches, sleeping 1 hour")
                time.sleep(3600)
                continue
            
            closest = upcoming[0]
            minutes_until = (closest["_kickoff_utc"] - now).total_seconds() / 60
            
            logger.info(f"📅 Next match: {closest['home_team']} vs {closest['away_team']}")
            logger.info(f"   Kickoff: {closest['_kickoff_utc'].strftime('%Y-%m-%d %H:%M UTC')}")
            logger.info(f"   {minutes_until:.0f} minutes from now")
            
            # ============================================================
            # LINEUP POLLING WINDOW (60 to 50 minutes before kickoff)
            # ============================================================
            if 10 <= minutes_until <= 65:  # Start checking at 65 min, poll at 60 min
                match_id = closest['match_id']
                
                # Start lineup polling at exactly 60 minutes before
                if minutes_until <= 60 and match_id not in lineup_polling_started:
                    if not closest.get("lineups_fetched"):
                        logger.info(f"🎯 STARTING LINEUP POLLING for {closest['home_team']} vs {closest['away_team']}")
                        logger.info(f"   {minutes_until:.0f} minutes until kickoff - polling every 30s")
                        
                        threading.Thread(
                            target=poll_lineups_continuous,
                            args=(closest, closest["_kickoff_utc"]),
                            daemon=True
                        ).start()
                        
                        lineup_polling_started.add(match_id)
            
            # ============================================================
            # MATCH START WINDOW (5 minutes before and after kickoff)
            # ============================================================
            if minutes_until <= 5:
                # Update status to "soon" at 5 minutes before
                if minutes_until > 0 and closest.get("status") != "soon":
                    logger.info(f"⚽ Match starting soon! Setting status to 'soon'")
                    update_game_status(closest["match_id"], "soon", is_live=False)
                
                # Start live polling at kickoff (0 minutes)
                if minutes_until <= 0:
                    # Update status to live
                    if closest.get("status") != "live":
                        logger.info(f"⚽ MATCH LIVE! Starting poller")
                        update_game_status(closest["match_id"], "live", is_live=True)
                    
                    # Start the live polling thread
                    start_polling(closest)
                    
                    # Small sleep before checking again
                    time.sleep(5)
                    continue
            
            # ============================================================
            # SMART SLEEP CALCULATION
            # ============================================================
            sleep_seconds = calculate_sleep_duration(closest)
            logger.info(f"💤 Sleeping for {sleep_seconds:.0f} seconds")
            time.sleep(sleep_seconds)
            
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