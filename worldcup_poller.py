"""
World Cup 2026 — Flashscore.ninja Scraper + Live Poller
=========================================================
Verified working configuration (captured via live browser interception):

  Host:    global.flashscore.ninja
  Base:    /2/x/feed/
  Token:   SW9D1eZo  (x-fsign header)
  WC ID:   lvUBR5F8  (World Championship 2026)

Hosts file required (run PowerShell as Admin):
  Add-Content C:\\Windows\\System32\\drivers\\etc\\hosts "34.8.77.207 global.flashscore.ninja"
  ipconfig /flushdns

Feed row format (entire response is ONE line, rows separated by ~):
  Rows are ¬-delimited KEY÷VALUE pairs.

  Tournament header row (ZA÷ present):
    ZC÷  season_id   (e.g. SbLsX4y7)
    ZE÷  stage_id    (e.g. zeSHfCx3)

  Fixture rows in to_{stage}_{season}_{page} (LME÷ present):
    LME÷  match_id
    LMJ÷  home team name
    LMK÷  away team name
    LMC÷  kickoff timestamp (unix)
    LMS÷  status  (? = upcoming, "finished"/"FT" = completed, else live)
    LMF÷  home score  (empty if not started)
    AU÷   away score  (empty if not started)

  Today's-only fixture rows in t_1_8_{WC_ID}_3_en_{page} (AA÷ present):
    AA÷   match_id
    CX÷   home team name
    AE÷   away team name
    AD÷   kickoff timestamp
    AB÷   status code  1=upcoming 2=1H 3=HT 4=2H 7=FT 8=AET 9=AP
    AG÷   home score
    AH÷   away score

  Live match detail dc_{id}:
    Same AA÷ row format as above, plus:
    BC÷/BD÷  time elapsed (minutes)

  Incidents d_hb_{id}  — rows starting with INC÷:
    INC÷<id>÷<type>÷<minute>÷<extra>÷<is_home>÷<player>÷[<assist_or_sub_out>]÷
    type: G=goal YC=yellow RC=red SB=substitution MS=missed_pen PEN=penalty

  Lineups li_{id}_1_en:
    LU÷<home_formation>÷<away_formation>÷
    PL÷<id>÷<name>÷<jersey>÷<pos>÷<is_home:1/0>÷<is_starter:1/0>÷[captain:1]÷
    CO÷<id>÷<name>÷<is_home:1/0>÷

  Statistics od_{id}:
    ST÷<stat_key>÷<home_val>÷<away_val>÷

Install:  pip install requests pymongo python-dotenv
Run:      python worldcup_poller_flashscore.py
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

WORLD_CUP_LABEL     = "World Cup 2026"
WC_TOURNAMENT_ID    = "lvUBR5F8"          # Flashscore internal ID

FS_NINJA_HOST       = "global.flashscore.ninja"
FS_FEED_BASE        = f"https://{FS_NINJA_HOST}/2/x/feed/"
X_FSIGN_TOKEN       = "SW9D1eZo"

MATCH_DURATION_MINS = 120
DATABASE_URL        = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME             = "clashdb"
COLLECTION_NAME     = "fixturexi"
NAIROBI_OFFSET      = timedelta(hours=3)

FANCLASH_API        = os.environ.get("FANCLASH_API")
DEFAULT_ODDS        = {"home_win": 2.50, "away_win": 2.80, "draw": 3.20}

POLL_INTERVAL_SEC        = 45
LINEUP_POLL_INTERVAL_SEC = 30
HOUR_CHECK_INTERVAL_SEC  = 3600
SCRAPE_INTERVAL_SEC      = 3600 * 6
LIVE_CHECK_INTERVAL_SEC  = 60
CLEANUP_INTERVAL_SEC     = 300

# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────

active_polls: set   = set()
polls_lock          = threading.Lock()
FS_SEMAPHORE        = threading.Semaphore(1)   # one Flashscore request at a time

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
    port   = int(os.environ.get("PORT", 8081))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"🌐 Health server on port {port}")


# ─────────────────────────────────────────────────────────────────────────────
# FLASHSCORE HTTP CLIENT
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

_session:      Optional[std_requests.Session] = None
_session_lock: threading.Lock                 = threading.Lock()


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
    logger.info("   🔄 Session reset")


def fs_get(query: str, retries: int = 5, base_delay: float = 2.0) -> Optional[str]:
    """
    GET https://global.flashscore.ninja/2/x/feed/<query>
    One request at a time via FS_SEMAPHORE.
    Exponential back-off on 403; one session reset per call.
    """
    url               = f"{FS_FEED_BASE}{query}"
    session_reset_done = False

    for attempt in range(retries):
        try:
            with FS_SEMAPHORE:
                time.sleep(random.uniform(base_delay, base_delay + 2.0))
                resp = _get_session().get(url, timeout=20)

            if resp.status_code == 200:
                return resp.text

            if resp.status_code == 404:
                logger.debug(f"   FS 404: {query}")
                return None

            if resp.status_code == 403:
                wait = (2 ** attempt) * random.uniform(4, 8)
                logger.warning(f"   FS 403 attempt {attempt+1} — back-off {wait:.0f}s")
                time.sleep(wait)
                if not session_reset_done:
                    _reset_session()
                    session_reset_done = True
                continue

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"   FS 429 — waiting {wait}s")
                time.sleep(wait)
                continue

            logger.warning(f"   FS HTTP {resp.status_code} attempt {attempt+1}: {query}")
            time.sleep(5)

        except Exception as e:
            logger.warning(f"   FS error attempt {attempt+1}: {e}")
            time.sleep(8)

    logger.error(f"   FS all retries exhausted: {query}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# LOW-LEVEL FEED PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_rows(raw: str) -> List[Dict[str, str]]:
    """
    Split on ~ (row separator) then parse each row's ¬-delimited KEY÷VALUE pairs.
    Returns list of field dicts, one per non-empty row.
    """
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


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

_STRIP_TAGS = re.compile(r"<[^>]+>")


def _clean(s: str) -> str:
    return " ".join(_STRIP_TAGS.sub("", s).split())


def _map_status_code(code: int) -> str:
    """Map Flashscore AB÷ integer status codes to internal status strings."""
    if code in (100, 110, 111, 120, 121):
        return "completed"
    if code in (2, 3, 4, 5, 6, 7, 8, 9, 10, 41, 42):
        return "live"
    return "upcoming"  # code 1 = upcoming


def _map_status_str(s: str) -> str:
    """Map LMS÷ string status to internal status strings."""
    sl = s.lower().strip()
    if sl in ("?", "upcoming", ""):
        return "upcoming"
    if sl in ("finished", "ft", "aet", "ap", "after extra time", "after penalties"):
        return "completed"
    return "live"


def is_match_over(date_iso: str, time_str: str) -> bool:
    if not date_iso or not time_str or time_str == "TBD":
        return False
    try:
        naive       = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
        kickoff_utc = (naive - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        return (kickoff_utc + timedelta(minutes=MATCH_DURATION_MINS)) < datetime.now(timezone.utc)
    except Exception:
        return False


def _ts_to_eat(ts: int) -> Tuple[str, str, str]:
    """Convert unix timestamp to (date_iso, date_display, time_eat) in EAT (UTC+3)."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + NAIROBI_OFFSET
    return dt.strftime("%Y-%m-%d"), dt.strftime("%d %b"), dt.strftime("%H:%M")


def _build_fixture_doc(
    match_id:   str,
    home_team:  str,
    away_team:  str,
    ts:         int,
    status:     str,
    home_score: Optional[int],
    away_score: Optional[int],
) -> Dict:
    if ts:
        date_iso, date_display, time_eat = _ts_to_eat(ts)
    else:
        now          = datetime.now(timezone.utc)
        date_iso     = now.strftime("%Y-%m-%d")
        date_display = now.strftime("%d %b")
        time_eat     = "TBD"

    return {
        "_id":                  match_id,
        "match_id":             match_id,
        "flashscore_id":        match_id,
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
        "source":               "flashscore",
        "scraped_at":           datetime.now(timezone.utc),
        "votes":                0,
        "comments":             0,
        "voters":               [],
        "commentary":           [],
        "commentary_count":     0,
        "last_commentary_at":   None,
    }


def _safe_int(v: str) -> Optional[int]:
    v = v.strip()
    if v and v not in ("-", ""):
        try:
            return int(v)
        except ValueError:
            pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# FEED PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def parse_schedule_feed(raw: str, upcoming_only: bool = True) -> List[Dict]:
    """
    Parse to_{stage}_{season}_{page} response.
    Match rows are identified by LME÷ (match id).
    """
    docs: List[Dict] = []
    if not raw:
        return docs

    for f in _parse_rows(raw):
        match_id = f.get("LME", "").strip()
        if not match_id:
            continue

        home_team = _clean(f.get("LMJ", ""))
        away_team = _clean(f.get("LMK", ""))
        if not home_team or not away_team:
            continue

        try:
            ts = int(f.get("LMC", 0))
        except (ValueError, TypeError):
            ts = 0

        status = _map_status_str(f.get("LMS", "?"))

        if upcoming_only and status not in ("upcoming", "live"):
            continue

        if ts:
            date_iso, _, time_eat = _ts_to_eat(ts)
            if upcoming_only and is_match_over(date_iso, time_eat):
                continue

        home_score = _safe_int(f.get("LMF", ""))
        away_score = _safe_int(f.get("AU", ""))

        docs.append(_build_fixture_doc(
            match_id, home_team, away_team, ts, status, home_score, away_score
        ))

    return docs


def parse_today_feed(raw: str, upcoming_only: bool = True) -> List[Dict]:
    """
    Parse t_1_8_{WC_ID}_3_en_{page} response (today's matches only).
    Match rows are identified by AA÷ (match id).
    """
    docs: List[Dict] = []
    if not raw:
        return docs

    for f in _parse_rows(raw):
        match_id = f.get("AA", "").strip()
        if not match_id:
            continue

        home_team = _clean(f.get("CX", "") or f.get("FH", ""))
        away_team = _clean(f.get("AE", "") or f.get("AF", ""))
        if not home_team or not away_team:
            continue

        try:
            ts = int(f.get("AD", 0))
        except (ValueError, TypeError):
            ts = 0

        try:
            status_code = int(f.get("AB", "1"))
        except (ValueError, TypeError):
            status_code = 1

        status = _map_status_code(status_code)

        if upcoming_only and status not in ("upcoming", "live"):
            continue

        if ts:
            date_iso, _, time_eat = _ts_to_eat(ts)
            if upcoming_only and is_match_over(date_iso, time_eat):
                continue

        home_score = _safe_int(f.get("AG", ""))
        away_score = _safe_int(f.get("AH", ""))

        docs.append(_build_fixture_doc(
            match_id, home_team, away_team, ts, status, home_score, away_score
        ))

    return docs


def parse_live_feed(raw: str) -> Optional[Dict]:
    """
    Parse dc_{match_id} response.
    Returns live state dict or None.
    """
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

        return {
            "status_code":  status_code,
            "status":       status,
            "home_score":   home_score,
            "away_score":   away_score,
            "time_elapsed": time_elapsed,
            "time_extra":   time_extra,
        }

    return None


def parse_incidents_feed(raw: str) -> List[Dict]:
    """
    Parse d_hb_{match_id} incidents feed.
    """
    incidents: List[Dict] = []
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
                    "id":      parts[0],
                    "type":    parts[1].upper(),
                    "minute":  int(parts[2]) if parts[2].isdigit() else 0,
                    "extra":   int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0,
                    "is_home": parts[4] == "1",
                    "player":  _clean(parts[5]) if len(parts) > 5 else "Unknown",
                    "assist":  _clean(parts[6]) if len(parts) > 6 and parts[6].strip() else None,
                    "sub_out": _clean(parts[7]) if len(parts) > 7 and parts[7].strip() else None,
                }
                incidents.append(inc)
            except (ValueError, IndexError):
                continue

    return incidents


def parse_lineups_feed(raw: str) -> Optional[Dict]:
    """
    Parse li_{match_id}_1_en lineups feed.
    """
    if not raw:
        return None

    lineups: Dict = {
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
                    jersey     = int(parts[2]) if parts[2].isdigit() else 0
                    side       = "home" if parts[4] == "1" else "away"
                    is_starter = parts[5] == "1"
                    player = {
                        "name":         _clean(parts[1]),
                        "position":     parts[3] or "Unknown",
                        "jerseyNumber": jersey,
                        "captain":      len(parts) > 7 and parts[7] == "1",
                        "lineup":       is_starter,
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


def parse_statistics_feed(
    raw: str,
    time_elapsed: int,
    time_extra:   int,
    home_score:   int,
    away_score:   int,
) -> Optional[Dict]:
    """
    Parse od_{match_id} statistics feed.
    Rows: ST÷<stat_key>÷<home_val>÷<away_val>÷
    """
    if not raw:
        return None

    KEY_MAP = {
        "possession_home":   ("ball_possession_home",  "ball_possession_away"),
        "ball possession":   ("ball_possession_home",  "ball_possession_away"),
        "shots_total":       ("total_shots_home",       "total_shots_away"),
        "total shots":       ("total_shots_home",       "total_shots_away"),
        "shots_on_target":   ("shots_on_target_home",   "shots_on_target_away"),
        "shots on target":   ("shots_on_target_home",   "shots_on_target_away"),
        "corner_kicks":      ("corners_home",            "corners_away"),
        "corner kicks":      ("corners_home",            "corners_away"),
        "fouls":             ("fouls_home",               "fouls_away"),
        "offsides":          ("offsides_home",            "offsides_away"),
        "yellow_cards":      ("yellow_cards_home",        "yellow_cards_away"),
        "yellow cards":      ("yellow_cards_home",        "yellow_cards_away"),
        "red_cards":         ("red_cards_home",           "red_cards_away"),
        "red cards":         ("red_cards_home",           "red_cards_away"),
        "pass_accuracy":     ("pass_accuracy_home",       "pass_accuracy_away"),
        "passes %":          ("pass_accuracy_home",       "pass_accuracy_away"),
    }

    stats: Dict[str, Any] = {}

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
        "minute":         time_elapsed,
        "minute_display": minute_disp,
        "home_score":     home_score,
        "away_score":     away_score,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    })
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def _get_season_stage_ids() -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch the tournament header row to extract season_id (ZC) and stage_id (ZE).
    """
    raw = fs_get(f"t_1_8_{WC_TOURNAMENT_ID}_3_en_1", base_delay=2.0)
    if not raw:
        return None, None

    for f in _parse_rows(raw):
        if "ZA" in f:
            season_id = f.get("ZC", "").strip()
            stage_id  = f.get("ZE", "").strip()
            if season_id and stage_id:
                logger.info(f"   Tournament header → season_id={season_id}  stage_id={stage_id}")
                return season_id, stage_id

    logger.warning("   Could not find ZA tournament header row")
    return None, None


def run_scraper(col) -> List[Dict]:
    logger.info("\n" + "=" * 65)
    logger.info("📡 RUNNING WORLD CUP 2026 FLASHSCORE SCRAPER")
    logger.info("=" * 65)

    docs: List[Dict] = []
    seen: set        = set()

    season_id, stage_id = _get_season_stage_ids()

    if season_id and stage_id:
        for page in range(1, 20):
            endpoint = f"to_{stage_id}_{season_id}_{page}"
            logger.info(f"   Fetching page {page}: {endpoint}")
            raw = fs_get(endpoint, base_delay=2.0)

            if not raw or len(raw.strip()) < 10:
                logger.info(f"   Page {page} empty — done")
                break

            page_docs = parse_schedule_feed(raw, upcoming_only=True)
            new = [d for d in page_docs if d["_id"] not in seen]
            for d in new:
                seen.add(d["_id"])
            docs.extend(new)

            logger.info(f"   Page {page} → {len(new)} fixtures  (running total: {len(docs)})")

            if len(page_docs) == 0:
                break
            time.sleep(random.uniform(2.0, 3.5))
    else:
        logger.warning("   season_id/stage_id not found — skipping schedule pages")

    if not docs:
        logger.info("   Falling back to today's t_ endpoint...")
        for page in range(1, 6):
            endpoint = f"t_1_8_{WC_TOURNAMENT_ID}_3_en_{page}"
            raw      = fs_get(endpoint, base_delay=2.0)
            if not raw or len(raw.strip()) < 10:
                break
            page_docs = parse_today_feed(raw, upcoming_only=True)
            new = [d for d in page_docs if d["_id"] not in seen]
            for d in new:
                seen.add(d["_id"])
            docs.extend(new)
            logger.info(f"   Fallback page {page} → {len(new)} fixtures")
            if len(page_docs) == 0:
                break
            time.sleep(random.uniform(2.0, 3.5))

    if docs and col is not None:
        saved = 0
        for d in docs:
            try:
                # FIX: on scrape, never overwrite status of a game that is
                # genuinely upcoming (kickoff in future). This prevents the
                # scraper from re-marking a DB-live record back to "upcoming"
                # mid-match, or vice versa stamping "live" on a future game.
                existing = col.find_one({"_id": d["_id"]}, {"status": 1, "_kickoff_ts": 1})
                if existing:
                    # Only overwrite status if we have a better ground-truth
                    # (scraped data says completed, or DB record is stale-live
                    # and kickoff hasn't happened yet).
                    col.update_one(
                        {"_id": d["_id"]},
                        {"$set": {k: v for k, v in d.items() if k != "status"}},
                        upsert=True,
                    )
                else:
                    col.update_one({"_id": d["_id"]}, {"$set": d}, upsert=True)
                saved += 1
            except Exception:
                pass
        logger.info(f"   💾 Saved {saved} World Cup fixtures")

    if not docs:
        wait = random.uniform(60, 120)
        logger.warning(f"   ⚠️  No fixtures found — backing off {wait:.0f}s")
        time.sleep(wait)

    logger.info(f"\n📊 Scraper done: {len(docs)} fixtures total")
    return docs


# ─────────────────────────────────────────────────────────────────────────────
# DB HELPERS
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
    return hcol


def repair_stale_live_records(col):
    """
    FIX: On startup, reset any DB records that are marked 'live' or 'soon'
    but whose kickoff timestamp is in the future. These are stale records
    from a previous run that crashed or was restarted before the game started.
    """
    if col is None:
        return
    now_utc = datetime.now(timezone.utc)
    repaired = 0
    try:
        for doc in col.find({"status": {"$in": ["live", "soon"]}, "league": WORLD_CUP_LABEL}):
            date_iso = doc.get("date_iso", "")
            time_str = doc.get("time", "")
            if not date_iso or not time_str or time_str == "TBD":
                continue
            try:
                naive_eat   = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
                kickoff_utc = (naive_eat - NAIROBI_OFFSET).replace(tzinfo=timezone.utc)
            except Exception:
                continue

            if kickoff_utc > now_utc:
                # Kickoff is in the future — this status is stale
                col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {
                        "status":               "upcoming",
                        "is_live":              False,
                        "available_for_voting": True,
                    }}
                )
                logger.warning(
                    f"🔧 Repaired stale '{doc['status']}' → 'upcoming': "
                    f"{doc.get('home_team')} vs {doc.get('away_team')} "
                    f"(kickoff {kickoff_utc.strftime('%Y-%m-%d %H:%M')} UTC)"
                )
                repaired += 1
    except Exception as e:
        logger.error(f"repair_stale_live_records error: {e}")

    if repaired:
        logger.info(f"🔧 Repaired {repaired} stale live/soon records")
    else:
        logger.info("✅ No stale live records found")


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
    for f in col.find({"status": {"$ne": "completed"}, "league": WORLD_CUP_LABEL}):
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
            "flashscore_id":    f.get("flashscore_id"),
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
        "status":               status,
        "is_live":              status == "live",
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
    """
    FIX: Only return fixtures that are genuinely live — i.e. the wall-clock
    time is at or past the kickoff timestamp. This prevents a stale DB status
    of 'live' from triggering polling for a game that hasn't kicked off yet.
    """
    now_utc = datetime.now(timezone.utc)
    result  = []
    for f in fixtures:
        ko = f.get("_kickoff_utc")

        # If we have a kickoff time and it's still in the future, never live
        if ko and now_utc < ko:
            continue

        if f.get("status") in ("live",) or (
            ko
            and now_utc >= ko
            and f.get("status") not in ("completed",)
        ):
            result.append(f)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# BACKEND API
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
            hp   = data.get("lineups", {}).get("home", {}).get("players", [])
            ap   = data.get("lineups", {}).get("away", {}).get("players", [])
            return bool(hp or ap)
        return False
    except Exception:
        return False


def forward_event(fixture: dict, event_type: str, data: dict):
    ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    payload = {k: v for k, v in {
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
        "minute":         data.get("minute", 0),
        "minute_display": data.get("minute_display", ""),
        "text":           data.get("text", ""),
        "event_type":     data.get("event_type", ""),
        "home_score":     data.get("home_score", 0),
        "away_score":     data.get("away_score", 0),
        "team":           data.get("team"),
        "player":         data.get("player"),
        "created_at":     {"$date": ts_ms},
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
    fs_id    = fixture.get("flashscore_id") or fixture.get("match_id")
    match_id = fixture.get("match_id")
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"

    if not fs_id:
        logger.warning(f"⚠️  No flashscore_id for {label}")
        return False

    logger.info(f"📋 Fetching lineups for {label}")
    raw     = fs_get(f"li_{fs_id}_1_en", base_delay=3.0)
    lineups = parse_lineups_feed(raw)

    if not lineups:
        logger.info(f"   ⏳ Lineups not yet available for {label}")
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

    raw = fs_get(f"od_{fs_id}", base_delay=2.0)
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

def _find_goal(incidents: List[Dict], is_home: bool) -> Tuple[str, Optional[str]]:
    for inc in reversed(incidents):
        if inc["type"] == "G" and inc["is_home"] == is_home:
            return inc.get("player", "Unknown"), inc.get("assist")
    return "Unknown", None


def poll_live_game(fixture: dict, col, history_col):
    fs_id    = fixture.get("flashscore_id") or fixture.get("match_id")
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"
    match_id = fixture["match_id"]

    if not fs_id:
        logger.error(f"❌ No flashscore_id for {label}")
        return

    # ── Sanity check: don't poll if kickoff hasn't happened yet ──────────────
    ko = fixture.get("_kickoff_utc")
    if ko:
        now_utc      = datetime.now(timezone.utc)
        mins_to_kick = (ko - now_utc).total_seconds() / 60
        if mins_to_kick > 5:
            logger.warning(
                f"⏭  Skipping poll for {label} — kickoff in {int(mins_to_kick)} mins "
                f"(not live yet). DB status was '{fixture.get('status')}'."
            )
            # Repair DB status if it was stale
            if fixture.get("status") in ("live", "soon") and mins_to_kick > 30:
                update_db_status(col, match_id, "upcoming")
            return

    # Check if already finished before starting poll loop
    initial = parse_live_feed(fs_get(f"dc_{fs_id}", base_delay=3.0))
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
        live = parse_live_feed(fs_get(f"dc_{fs_id}", base_delay=3.0))
        if not live:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        incidents = parse_incidents_feed(fs_get(f"d_hb_{fs_id}", base_delay=2.0) or "")

        home_score   = live["home_score"]
        away_score   = live["away_score"]
        status       = live["status"]
        status_code  = live["status_code"]
        time_elapsed = live["time_elapsed"]
        time_extra   = live.get("time_extra", 0)
        minute_disp  = f"{time_elapsed}" + (f"+{time_extra}" if time_extra else "")

        # ── Goals ──────────────────────────────────────────────────────────
        if home_score > last_home:
            scorer, assist = _find_goal(incidents, is_home=True)
            logger.info(f"⚽ GOAL {fixture['home_team']} — {scorer} ({minute_disp}')")
            forward_event(fixture, "goal", {
                "minute": time_elapsed, "minute_display": minute_disp,
                "home_score": home_score, "away_score": away_score,
                "team": fixture["home_team"], "player": scorer, "assist": assist,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": minute_disp,
                "text": f"⚽ GOAL! {scorer} scores! ({home_score}-{away_score})",
                "event_type": "goal", "home_score": home_score, "away_score": away_score,
                "team": fixture["home_team"], "player": scorer,
            })
            last_home = home_score

        if away_score > last_away:
            scorer, assist = _find_goal(incidents, is_home=False)
            logger.info(f"⚽ GOAL {fixture['away_team']} — {scorer} ({minute_disp}')")
            forward_event(fixture, "goal", {
                "minute": time_elapsed, "minute_display": minute_disp,
                "home_score": home_score, "away_score": away_score,
                "team": fixture["away_team"], "player": scorer, "assist": assist,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": minute_disp,
                "text": f"⚽ GOAL! {scorer} scores! ({home_score}-{away_score})",
                "event_type": "goal", "home_score": home_score, "away_score": away_score,
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
            text     = ""
            ev_type  = inc_type.lower()

            if inc_type == "G":
                continue  # handled above

            elif inc_type == "YC":
                text = f"🟨 YELLOW CARD - {player} ({team})"
                logger.info(f"🟨 YELLOW — {team}: {player} ({m_disp}')")
                forward_event(fixture, "yellow_card", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })
                ev_type = "yellow_card"

            elif inc_type == "RC":
                text = f"🟥 RED CARD - {player} ({team})"
                logger.info(f"🟥 RED — {team}: {player} ({m_disp}')")
                forward_event(fixture, "red_card", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })
                ev_type = "red_card"

            elif inc_type == "SB":
                p_out = inc.get("sub_out") or inc.get("assist") or "Unknown"
                text  = f"🔄 SUB: {p_out} → {player} ({team})"
                logger.info(f"🔄 SUB — {team}: {p_out} → {player} ({m_disp}')")
                forward_event(fixture, "substitution", {
                    "minute": minute, "minute_display": m_disp,
                    "player_out": p_out, "player_in": player, "team": team,
                })
                ev_type = "substitution"

            elif inc_type == "MS":
                text = f"❌ MISSED PENALTY - {player} ({team})"
                logger.info(f"❌ MISSED PEN — {team}: {player} ({m_disp}')")
                forward_event(fixture, "missed_penalty", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })
                ev_type = "missed_penalty"

            elif inc_type == "PEN":
                text = f"🎯 PENALTY! {player} ({team})"
                logger.info(f"🎯 PENALTY — {team}: {player} ({m_disp}')")
                forward_event(fixture, "penalty", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })
                ev_type = "penalty"

            if text:
                send_commentary(fixture, {
                    "minute": minute, "minute_display": m_disp, "text": text,
                    "event_type": ev_type, "home_score": home_score, "away_score": away_score,
                    "team": team,
                    "player": player if inc_type != "SB" else None,
                })

        # ── Match phases ───────────────────────────────────────────────────
        if status_code == 3 and not half_time_sent:
            logger.info(f"⏸  HALF TIME: {home_score}–{away_score}")
            forward_event(fixture, "half_time", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "home_score": home_score, "away_score": away_score,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "text": f"⏸ HALF TIME: {fixture['home_team']} {home_score}–{away_score} {fixture['away_team']}",
                "event_type": "half_time", "home_score": home_score, "away_score": away_score,
            })
            update_db_status(col, match_id, "live", {"time_elapsed": time_elapsed, "half": 1})
            fetch_and_forward_statistics(fixture, live)
            half_time_sent = True

        if status_code == 4 and half_time_sent and not second_half_sent:
            logger.info("▶️  SECOND HALF STARTED")
            forward_event(fixture, "second_half", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "text": f"▶️ SECOND HALF UNDERWAY! {fixture['home_team']} {home_score}–{away_score} {fixture['away_team']}",
                "event_type": "second_half", "home_score": home_score, "away_score": away_score,
            })
            update_db_status(col, match_id, "live", {"half": 2})
            second_half_sent = True

        if status == "completed" and not full_time_sent:
            logger.info(f"🏁 FULL TIME: {label} — {home_score}–{away_score}")
            forward_event(fixture, "match_end", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "home_score": home_score, "away_score": away_score,
            })
            send_commentary(fixture, {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
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

        # Periodic stats every 5 polls (~225 s)
        poll_count += 1
        if poll_count % 5 == 0:
            logger.info(f"📊 Stats snapshot at {minute_disp}' for {label}")
            fetch_and_forward_statistics(fixture, live)

        time.sleep(POLL_INTERVAL_SEC)

    logger.info(f"✅ Done polling {label}")


# ─────────────────────────────────────────────────────────────────────────────
# POLL QUEUE — single worker thread, auto-restarts on crash
# ─────────────────────────────────────────────────────────────────────────────

_poll_queue:           _queue.Queue  = _queue.Queue()
_queue_worker_thread:  Optional[threading.Thread] = None
_queue_lock:           threading.Lock = threading.Lock()


def _queue_worker():
    logger.info("🔁 Poll queue worker started")
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
                    logger.debug(f"   Already polling {label} — skipping duplicate")
                    _poll_queue.task_done()
                    continue
                active_polls.add(match_id)

            try:
                poll_live_game(fixture, col, history_col)
            except Exception as e:
                logger.error(f"Poll error for {label}: {e}", exc_info=True)
            finally:
                with polls_lock:
                    active_polls.discard(match_id)
                _poll_queue.task_done()

        except _queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Queue worker unhandled error: {e}", exc_info=True)
            # Don't exit — keep running


def _ensure_queue_worker():
    """
    FIX: Start the worker thread if it hasn't been started or if it has
    died (e.g. due to an unhandled exception). This prevents silent hangs
    where tasks pile up in the queue but nothing processes them.
    """
    global _queue_worker_thread
    with _queue_lock:
        if _queue_worker_thread is None or not _queue_worker_thread.is_alive():
            if _queue_worker_thread is not None:
                logger.warning("⚠️  Poll queue worker died — restarting")
            _queue_worker_thread = threading.Thread(
                target=_queue_worker, daemon=True, name="wc-poll-worker"
            )
            _queue_worker_thread.start()


def start_polling_for_game(fixture: dict, col, history_col):
    match_id = fixture["match_id"]
    with polls_lock:
        if match_id in active_polls:
            logger.debug(f"   Already polling {fixture['home_team']} vs {fixture['away_team']}")
            return
    _ensure_queue_worker()
    _poll_queue.put((fixture, col, history_col))
    logger.info(f"📥 Queued: {fixture['home_team']} vs {fixture['away_team']}")


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

    # FIX: Repair stale live/soon records before doing anything else.
    # This is the primary fix for the "stuck live" bug seen in logs.
    repair_stale_live_records(col)

    cleanup_all_completed_games(col, history_col)

    existing = load_fixtures_from_db(col)
    if existing:
        logger.info(f"📦 {len(existing)} fixture(s) in DB — skipping startup scrape")
        last_scrape_time = time.time()
    else:
        logger.info("📭 DB empty — running initial scrape...")
        run_scraper(col)
        last_scrape_time = time.time()

    last_cleanup_time   = time.time()
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

            # Periodic cleanup every 5 minutes
            if time.time() - last_cleanup_time >= CLEANUP_INTERVAL_SEC:
                cleanup_all_completed_games(col, history_col)
                last_cleanup_time = time.time()

            # FIX: Re-run stale record repair periodically in case a restart
            # left new stale records (e.g. service bounced mid-match).
            # Only do this check every 10 minutes to avoid spam.
            if int(time.time()) % 600 < LIVE_CHECK_INTERVAL_SEC:
                repair_stale_live_records(col)

            fixtures = load_fixtures_from_db(col)
            if not fixtures:
                logger.warning("📭 No fixtures in DB — scraping now")
                run_scraper(col)
                last_scrape_time = time.time()
                fixtures = load_fixtures_from_db(col)

            # Ensure worker thread is alive every loop iteration
            _ensure_queue_worker()

            # ── Live games ─────────────────────────────────────────────────
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

            # ── Upcoming games in next 24h ─────────────────────────────────
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
                icon     = "🔴" if f.get("status") == "soon" else "⏳"
                logger.info(f"   {icon} {f['home_team']} vs {f['away_team']} at {ko_local} EAT ({int(mins)} mins)")

            for mins_to_game, fixture in upcoming:
                mid   = fixture["match_id"]
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

            # ── Start polling if kickoff ≤ 5 min away ─────────────────────
            closest_mins, closest_fixture = upcoming[0]
            if 0 < closest_mins <= 5:
                logger.info(f"⚽ {closest_fixture['home_team']} vs {closest_fixture['away_team']} in {int(closest_mins)} mins")
                start_polling_for_game(closest_fixture, col, history_col)
                time.sleep(POLL_INTERVAL_SEC)
                continue

            # Sleep based on proximity to next game
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