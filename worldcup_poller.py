"""
World Cup 2026 — Combined Sofascore + Flashscore Poller
=========================================================
Strategy:
  - Maintains a "preferred_source" that persists across scrape cycles.
  - On each scrape attempt, tries preferred source first.
  - If preferred source fails (returns 0 fixtures after exhausting retries),
    switches to the other and updates preferred_source.
  - Live polling uses the same source that produced the fixture's data
    (tracked via fixture["source"]).
  - Source preference survives across 6-hour rescrape cycles.

Sources:
  sofascore  → curl_cffi chrome-impersonation session, api.sofascore.com
  flashscore → requests session, global.flashscore.ninja (needs hosts entry)

Install:  pip install curl_cffi pymongo requests python-dotenv
Run:      python worldcup_poller_combined.py
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
from typing import Any, Dict, List, Optional, Tuple

from curl_cffi import requests as cffi_requests
import requests as std_requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

WORLD_CUP_LABEL          = "World Cup 2026"

# Sofascore
SS_TOURNAMENT_ID         = 16
SS_API                   = "https://api.sofascore.com/api/v1"
SS_HOME                  = "https://www.sofascore.com"

# Flashscore
FS_TOURNAMENT_ID         = "lvUBR5F8"
FS_NINJA_HOST            = "global.flashscore.ninja"
FS_FEED_BASE             = f"https://{FS_NINJA_HOST}/2/x/feed/"
X_FSIGN_TOKEN            = "SW9D1eZo"

MATCH_DURATION_MINS      = 120
DAILY_MAX_DAYS           = 60
DAILY_MAX_MISSES         = 7
DATABASE_URL             = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME                  = "clashdb"
COLLECTION_NAME          = "fixtures"
NAIROBI_OFFSET           = timedelta(hours=3)
DEFAULT_ODDS             = {"home_win": 2.50, "away_win": 2.80, "draw": 3.20}
FANCLASH_API             = os.environ.get("FANCLASH_API")

POLL_INTERVAL_SEC        = 45
LINEUP_POLL_INTERVAL_SEC = 30
HOUR_CHECK_INTERVAL_SEC  = 3600
SCRAPE_INTERVAL_SEC      = 3600 * 6
LIVE_CHECK_INTERVAL_SEC  = 60
CLEANUP_INTERVAL_SEC     = 300

# Source failover — "sofascore" | "flashscore"
# Modified at runtime when a source fails.
preferred_source         = "sofascore"
source_lock              = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────

active_polls: set    = set()
polls_lock           = threading.Lock()
SS_SEMAPHORE         = threading.Semaphore(1)
FS_SEMAPHORE         = threading.Semaphore(1)

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
        body = (
            f'{{"status":"ok","preferred_source":"{src}"}}'.encode()
            if self.path == "/wakeup"
            else f"FanClash WorldCup Poller OK | source={src}".encode()
        )
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
# SOURCE PREFERENCE HELPERS
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


def other_source(src: str) -> str:
    return "flashscore" if src == "sofascore" else "sofascore"


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────────────────────────────────────────

TEAM_NAME_CORRECTIONS = {
    "Türkiye": "Turkey", "Korea Republic": "South Korea",
    "Côte d'Ivoire": "Ivory Coast", "Congo DR": "DR Congo",
    "Bosnia & Herzegovina": "Bosnia and Herzegovina",
    "USA": "United States", "Curaçao": "Curacao", "Cabo Verde": "Cape Verde",
}


def correct_team_name(name: str) -> str:
    cleaned = " ".join(name.split())
    return TEAM_NAME_CORRECTIONS.get(cleaned, cleaned)


def eat_from_timestamp(ts: int) -> Tuple[str, str, str]:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) + NAIROBI_OFFSET
    return dt.strftime("%Y-%m-%d"), dt.strftime("%d %b"), dt.strftime("%H:%M")


def is_match_over(date_iso: str, time_str: str) -> bool:
    if not date_iso or not time_str or time_str == "TBD":
        return False
    try:
        naive       = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
        kickoff_utc = (naive - timedelta(hours=3)).replace(tzinfo=timezone.utc)
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


def build_fixture_doc(
    match_id:   str,
    home_team:  str,
    away_team:  str,
    ts:         int,
    status:     str,
    home_score: Optional[int],
    away_score: Optional[int],
    source:     str,
    extra_ids:  Optional[Dict] = None,
) -> Dict:
    if ts:
        date_iso, date_display, time_eat = eat_from_timestamp(ts)
    else:
        now          = datetime.now(timezone.utc)
        date_iso     = now.strftime("%Y-%m-%d")
        date_display = now.strftime("%d %b")
        time_eat     = "TBD"

    doc = {
        "_id":                  match_id,
        "match_id":             match_id,
        "home_team":            correct_team_name(home_team),
        "away_team":            correct_team_name(away_team),
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
        "source":               source,
        "scraped_at":           datetime.now(timezone.utc),
        "votes":                0,
        "comments":             0,
        "voters":               [],
        "commentary":           [],
        "commentary_count":     0,
        "last_commentary_at":   None,
    }
    if extra_ids:
        doc.update(extra_ids)
    return doc


# ─────────────────────────────────────────────────────────────────────────────
# ══ SOFASCORE CLIENT ══════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

SS_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

SS_WARMUP_URLS = [
    SS_HOME,
    f"{SS_HOME}/football",
    f"{SS_HOME}/team/football/brazil/14",
    f"{SS_HOME}/team/football/argentina/12",
    f"{SS_HOME}/team/football/france/4481",
]


def ss_make_session(warm_up: bool = True) -> cffi_requests.Session:
    session = cffi_requests.Session(impersonate="chrome124")
    session.headers.update({
        "Accept-Language":    "en-US,en;q=0.9",
        "Accept-Encoding":    "gzip, deflate, br",
        "Accept":             "application/json, text/plain, */*",
        "Connection":         "keep-alive",
        "Sec-Fetch-Dest":     "empty",
        "Sec-Fetch-Mode":     "cors",
        "Sec-Fetch-Site":     "same-site",
        "User-Agent":         random.choice(SS_USER_AGENTS),
        "Referer":            f"{SS_HOME}/",
        "Origin":             SS_HOME,
        "Cache-Control":      "max-age=0",
        "Sec-Ch-Ua":          '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile":   "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    })
    if warm_up:
        try:
            time.sleep(random.uniform(5.0, 10.0))
            url = random.choice(SS_WARMUP_URLS)
            r   = session.get(url, timeout=15)
            logger.info(f"   SS warm-up: HTTP {r.status_code} ({url})")
            time.sleep(random.uniform(1.5, 3.5))
        except Exception as e:
            logger.warning(f"   SS warm-up failed: {e}")
    return session


def ss_api_get(
    session: cffi_requests.Session,
    path: str,
    retries: int = 5,
) -> Tuple[Optional[Dict], cffi_requests.Session]:
    url              = f"{SS_API}{path}"
    refreshed_once   = False

    for attempt in range(retries):
        try:
            with SS_SEMAPHORE:
                time.sleep(random.uniform(1.5, 3.0))
                resp = session.get(url, timeout=25)

            if resp.status_code == 200:
                return resp.json(), session
            if resp.status_code == 404:
                return None, session
            if resp.status_code == 403:
                wait = (2 ** attempt) * random.uniform(4, 8)
                logger.warning(f"   SS 403 ({path}) attempt {attempt+1} — back-off {wait:.0f}s")
                time.sleep(wait)
                if not refreshed_once:
                    session       = ss_make_session(warm_up=True)
                    refreshed_once = True
                continue
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"   SS 429 — waiting {wait}s")
                time.sleep(wait)
            else:
                time.sleep(5)
        except Exception as e:
            logger.warning(f"   SS error attempt {attempt+1}: {e}")
            time.sleep(8)

    return None, session


# ─────────────────────────────────────────────────────────────────────────────
# SOFASCORE SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def ss_event_status(event: Dict) -> str:
    type_ = (event.get("status") or {}).get("type", "")
    code  = (event.get("status") or {}).get("code", 0)
    if type_ == "inprogress":
        return "live"
    if code in (100, 110, 120):
        return "completed"
    return "upcoming"


def ss_parse_event(event: Dict) -> Optional[Dict]:
    home_name = (event.get("homeTeam") or {}).get("name", "")
    away_name = (event.get("awayTeam") or {}).get("name", "")
    if not home_name or not away_name:
        return None

    ts = event.get("startTimestamp", 0)
    if ts:
        date_iso, _, time_eat = eat_from_timestamp(ts)
    else:
        now      = datetime.now(timezone.utc)
        date_iso = now.strftime("%Y-%m-%d")
        time_eat = "TBD"

    status = ss_event_status(event)
    home_score = away_score = None
    if status in ("completed", "live"):
        home_score = (event.get("homeScore") or {}).get("current")
        away_score = (event.get("awayScore") or {}).get("current")

    sofascore_id = event.get("id")
    match_id     = str(sofascore_id) if sofascore_id else hashlib.md5(
        f"{home_name}_{away_name}_{date_iso}".encode()
    ).hexdigest()[:12]

    return build_fixture_doc(
        match_id, home_name, away_name, ts, status, home_score, away_score,
        source="sofascore",
        extra_ids={"sofascore_id": sofascore_id},
    )


def ss_get_current_season(
    session: cffi_requests.Session,
) -> Tuple[Optional[int], cffi_requests.Session]:
    data, session = ss_api_get(session, f"/unique-tournament/{SS_TOURNAMENT_ID}/seasons")
    if not data:
        return None, session
    seasons = data.get("seasons", [])
    return (seasons[0].get("id") if seasons else None), session


def ss_scrape_via_rounds(
    session: cffi_requests.Session,
    season_id: int,
) -> Tuple[List[Dict], cffi_requests.Session]:
    data, session = ss_api_get(
        session,
        f"/unique-tournament/{SS_TOURNAMENT_ID}/season/{season_id}/rounds",
        retries=2,
    )
    if not data:
        return [], session

    all_rounds = sorted(set(
        r.get("round") for r in data.get("rounds", []) if r.get("round") is not None
    ))
    if not all_rounds:
        return [], session

    logger.info(f"   SS: {len(all_rounds)} rounds in season {season_id}")
    docs: List[Dict] = []
    seen: set        = set()

    for rnd in all_rounds:
        rdata, session = ss_api_get(
            session,
            f"/unique-tournament/{SS_TOURNAMENT_ID}/season/{season_id}/events/round/{rnd}",
        )
        if not rdata:
            continue

        for ev in rdata.get("events", []):
            doc = ss_parse_event(ev)
            if not doc or doc["_id"] in seen:
                continue
            if doc["status"] != "upcoming":
                continue
            if is_match_over(doc["date_iso"], doc["time"]):
                continue
            seen.add(doc["_id"])
            docs.append(doc)

        time.sleep(random.uniform(5.0, 10.0))

    return docs, session


def ss_scrape_via_daily(
    session: cffi_requests.Session,
) -> Tuple[List[Dict], cffi_requests.Session]:
    logger.info("   SS: Day-by-day scan...")
    docs: List[Dict]   = []
    seen: set          = set()
    matchdays_found    = 0
    consecutive_misses = 0
    blocked_streak     = 0
    MAX_BLOCKED        = 3

    today   = datetime.now(timezone.utc).date()
    cutoff  = today + timedelta(days=DAILY_MAX_DAYS)
    current = today

    while current <= cutoff:
        if consecutive_misses >= DAILY_MAX_MISSES and matchdays_found > 0:
            break
        if blocked_streak >= MAX_BLOCKED:
            wait = random.uniform(120, 240)
            logger.warning(f"   SS: {blocked_streak} blocked days — pause {wait:.0f}s")
            time.sleep(wait)
            session       = ss_make_session(warm_up=True)
            blocked_streak = 0

        day_str       = current.strftime("%Y-%m-%d")
        data, session = ss_api_get(session, f"/sport/football/events/{day_str}")

        if data is None:
            blocked_streak     += 1
            consecutive_misses += 1
        else:
            blocked_streak = 0
            day_docs       = []
            for ev in data.get("events", []):
                tid = (ev.get("tournament") or {}).get("uniqueTournament", {}).get("id")
                if tid != SS_TOURNAMENT_ID:
                    continue
                doc = ss_parse_event(ev)
                if not doc or doc["_id"] in seen:
                    continue
                if doc["status"] != "upcoming" or is_match_over(doc["date_iso"], doc["time"]):
                    continue
                seen.add(doc["_id"])
                day_docs.append(doc)

            if day_docs:
                matchdays_found   += 1
                consecutive_misses = 0
                logger.info(f"   SS: {day_str} → {len(day_docs)} matches")
                docs.extend(day_docs)
            else:
                consecutive_misses += 1

        current += timedelta(days=1)
        time.sleep(random.uniform(4.0, 8.0))

    return docs, session


def ss_run_scraper() -> List[Dict]:
    """Run Sofascore scraper. Returns fixture list (empty on failure/block)."""
    logger.info("   📡 Sofascore scraper starting...")
    session           = ss_make_session(warm_up=True)
    season_id, session = ss_get_current_season(session)

    if season_id:
        logger.info(f"   SS: season_id={season_id}")
        docs, _ = ss_scrape_via_rounds(session, season_id)
    else:
        docs = []

    if not docs:
        logger.info("   SS: rounds empty — falling back to daily scan")
        docs, _ = ss_scrape_via_daily(session)

    logger.info(f"   SS: got {len(docs)} fixtures")
    return docs


# ─────────────────────────────────────────────────────────────────────────────
# ══ FLASHSCORE CLIENT ═════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

FS_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

_fs_session:      Optional[std_requests.Session] = None
_fs_session_lock: threading.Lock                 = threading.Lock()


def _fs_make_session() -> std_requests.Session:
    s = std_requests.Session()
    s.headers.update({
        "Accept":          "text/plain, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Referer":         "https://www.flashscore.com/",
        "Origin":          "https://www.flashscore.com",
        "User-Agent":      random.choice(FS_USER_AGENTS),
        "X-Fsign":         X_FSIGN_TOKEN,
    })
    return s


def _fs_get_session() -> std_requests.Session:
    global _fs_session
    with _fs_session_lock:
        if _fs_session is None:
            _fs_session = _fs_make_session()
        return _fs_session


def _fs_reset_session():
    global _fs_session
    with _fs_session_lock:
        _fs_session = _fs_make_session()
    logger.info("   FS: session reset")


def fs_get(query: str, retries: int = 5, base_delay: float = 2.0) -> Optional[str]:
    url              = f"{FS_FEED_BASE}{query}"
    reset_done       = False

    for attempt in range(retries):
        try:
            with FS_SEMAPHORE:
                time.sleep(random.uniform(base_delay, base_delay + 2.0))
                resp = _fs_get_session().get(url, timeout=20)

            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 404:
                return None
            if resp.status_code == 403:
                wait = (2 ** attempt) * random.uniform(4, 8)
                logger.warning(f"   FS 403 attempt {attempt+1} — back-off {wait:.0f}s")
                time.sleep(wait)
                if not reset_done:
                    _fs_reset_session()
                    reset_done = True
                continue
            if resp.status_code == 429:
                time.sleep(30 * (attempt + 1))
                continue
            time.sleep(5)
        except Exception as e:
            logger.warning(f"   FS error attempt {attempt+1}: {e}")
            time.sleep(8)

    logger.error(f"   FS all retries exhausted: {query}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# FLASHSCORE FEED PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_fs_rows(raw: str) -> List[Dict[str, str]]:
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
    if code in (100, 110, 111, 120, 121):
        return "completed"
    if code in (2, 3, 4, 5, 6, 7, 8, 9, 10, 41, 42):
        return "live"
    return "upcoming"


def _fs_map_status_str(s: str) -> str:
    sl = s.lower().strip()
    if sl in ("?", "upcoming", ""):
        return "upcoming"
    if sl in ("finished", "ft", "aet", "ap", "after extra time", "after penalties"):
        return "completed"
    return "live"


def fs_parse_schedule_feed(raw: str, upcoming_only: bool = True) -> List[Dict]:
    docs: List[Dict] = []
    if not raw:
        return docs
    for f in _parse_fs_rows(raw):
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
        status = _fs_map_status_str(f.get("LMS", "?"))
        if upcoming_only and status not in ("upcoming", "live"):
            continue
        if ts:
            date_iso, _, time_eat = eat_from_timestamp(ts)
            if upcoming_only and is_match_over(date_iso, time_eat):
                continue
        home_score = _safe_int(f.get("LMF", ""))
        away_score = _safe_int(f.get("AU", ""))
        docs.append(build_fixture_doc(
            match_id, home_team, away_team, ts, status, home_score, away_score,
            source="flashscore",
            extra_ids={"flashscore_id": match_id},
        ))
    return docs


def fs_parse_today_feed(raw: str, upcoming_only: bool = True) -> List[Dict]:
    docs: List[Dict] = []
    if not raw:
        return docs
    for f in _parse_fs_rows(raw):
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
        status = _fs_map_status_code(status_code)
        if upcoming_only and status not in ("upcoming", "live"):
            continue
        if ts:
            date_iso, _, time_eat = eat_from_timestamp(ts)
            if upcoming_only and is_match_over(date_iso, time_eat):
                continue
        home_score = _safe_int(f.get("AG", ""))
        away_score = _safe_int(f.get("AH", ""))
        docs.append(build_fixture_doc(
            match_id, home_team, away_team, ts, status, home_score, away_score,
            source="flashscore",
            extra_ids={"flashscore_id": match_id},
        ))
    return docs


def fs_get_season_stage_ids() -> Tuple[Optional[str], Optional[str]]:
    raw = fs_get(f"t_1_8_{FS_TOURNAMENT_ID}_3_en_1", base_delay=2.0)
    if not raw:
        return None, None
    for f in _parse_fs_rows(raw):
        if "ZA" in f:
            season_id = f.get("ZC", "").strip()
            stage_id  = f.get("ZE", "").strip()
            if season_id and stage_id:
                logger.info(f"   FS: season_id={season_id}  stage_id={stage_id}")
                return season_id, stage_id
    return None, None


def fs_run_scraper() -> List[Dict]:
    """Run Flashscore scraper. Returns fixture list (empty on failure/block)."""
    logger.info("   📡 Flashscore scraper starting...")
    docs: List[Dict] = []
    seen: set        = set()

    season_id, stage_id = fs_get_season_stage_ids()

    if season_id and stage_id:
        for page in range(1, 20):
            endpoint  = f"to_{stage_id}_{season_id}_{page}"
            raw       = fs_get(endpoint, base_delay=2.0)
            if not raw or len(raw.strip()) < 10:
                break
            page_docs = fs_parse_schedule_feed(raw, upcoming_only=True)
            new       = [d for d in page_docs if d["_id"] not in seen]
            for d in new:
                seen.add(d["_id"])
            docs.extend(new)
            logger.info(f"   FS: page {page} → {len(new)} fixtures")
            if not page_docs:
                break
            time.sleep(random.uniform(2.0, 3.5))
    else:
        logger.warning("   FS: could not get season/stage IDs — trying today feed")

    if not docs:
        for page in range(1, 6):
            endpoint  = f"t_1_8_{FS_TOURNAMENT_ID}_3_en_{page}"
            raw       = fs_get(endpoint, base_delay=2.0)
            if not raw or len(raw.strip()) < 10:
                break
            page_docs = fs_parse_today_feed(raw, upcoming_only=True)
            new       = [d for d in page_docs if d["_id"] not in seen]
            for d in new:
                seen.add(d["_id"])
            docs.extend(new)
            if not page_docs:
                break
            time.sleep(random.uniform(2.0, 3.5))

    logger.info(f"   FS: got {len(docs)} fixtures")
    return docs


# ─────────────────────────────────────────────────────────────────────────────
# ══ COMBINED SCRAPER WITH FAILOVER ════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def run_scraper(col) -> List[Dict]:
    logger.info("\n" + "=" * 65)
    logger.info("📡 RUNNING WORLD CUP 2026 COMBINED SCRAPER")
    logger.info("=" * 65)

    primary   = get_preferred()
    secondary = other_source(primary)

    logger.info(f"   Primary source: {primary}  |  Fallback: {secondary}")

    scraper_map = {
        "sofascore":  ss_run_scraper,
        "flashscore": fs_run_scraper,
    }

    docs = scraper_map[primary]()

    if not docs:
        logger.warning(f"   ⚠️  {primary} returned 0 fixtures — switching to {secondary}")
        set_preferred(secondary)
        docs = scraper_map[secondary]()

        if not docs:
            # Both failed — stay on secondary, back off
            wait = random.uniform(60, 120)
            logger.warning(f"   ⚠️  Both sources failed — backing off {wait:.0f}s")
            time.sleep(wait)
        else:
            logger.info(f"   ✅ {secondary} succeeded — now preferred source")
    else:
        logger.info(f"   ✅ {primary} succeeded")

    if docs and col is not None:
        saved = 0
        for d in docs:
            try:
                col.update_one({"_id": d["_id"]}, {"$set": d}, upsert=True)
                saved += 1
            except Exception:
                pass
        logger.info(f"   💾 Saved {saved} World Cup fixtures (source={get_preferred()})")

    logger.info(f"\n📊 Scraper done: {len(docs)} fixtures  |  preferred_source={get_preferred()}")
    return docs


# ─────────────────────────────────────────────────────────────────────────────
# ══ DB HELPERS ════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    try:
        client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=15000)
        client.admin.command("ping")
        col = client[DB_NAME][COLLECTION_NAME]
        col.create_index("match_id",      unique=True)
        col.create_index("sofascore_id")
        col.create_index("flashscore_id")
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
        if moved:
            logger.info(f"🧹 Moved {moved} completed games to history")
    except Exception as e:
        logger.error(f"cleanup error: {e}")


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
            "sofascore_id":     f.get("sofascore_id"),
            "flashscore_id":    f.get("flashscore_id"),
            "home_team":        f.get("home_team"),
            "away_team":        f.get("away_team"),
            "home_score":       f.get("home_score", 0),
            "away_score":       f.get("away_score", 0),
            "status":           f.get("status", "upcoming"),
            "is_live":          f.get("is_live", False),
            "date_iso":         date_iso,
            "time":             time_str,
            "source":           f.get("source", "sofascore"),
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
        logger.warning(f"mark_lineups_fetched error: {e}")


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
    now_utc = datetime.now(timezone.utc)
    return [
        f for f in fixtures
        if f.get("status") == "live"
        or (f.get("_kickoff_utc") and now_utc >= f["_kickoff_utc"] and f.get("status") != "completed")
    ]


# ─────────────────────────────────────────────────────────────────────────────
# ══ BACKEND API CALLS ═════════════════════════════════════════════════════════
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
    ts_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
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
# ══ LINEUP FETCHERS (source-dispatched) ═══════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

# ── Sofascore lineups ─────────────────────────────────────────────────────────

def _ss_parse_lineups(lineups_data: Dict) -> Optional[Dict]:
    home_raw = lineups_data.get("home", {}).get("players", [])
    away_raw = lineups_data.get("away", {}).get("players", [])
    if not home_raw and not away_raw:
        return None

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

    def split(raw, bench_raw):
        starters, bench = [], []
        for p in raw:
            (starters if p.get("lineup", True) else bench).append(safe_player(p))
        for p in bench_raw:
            bench.append(safe_player(p))
        return starters, bench

    hs, hb = split(home_raw, lineups_data.get("home", {}).get("bench", []))
    as_, ab = split(away_raw, lineups_data.get("away", {}).get("bench", []))

    return {
        "home": {
            "formation": str(lineups_data.get("home", {}).get("formation") or "4-4-2"),
            "players":   hs,
            "bench":     hb,
            "coach":     {"name": str(lineups_data.get("home", {}).get("coach", {}).get("name") or "Unknown")},
        },
        "away": {
            "formation": str(lineups_data.get("away", {}).get("formation") or "4-4-2"),
            "players":   as_,
            "bench":     ab,
            "coach":     {"name": str(lineups_data.get("away", {}).get("coach", {}).get("name") or "Unknown")},
        },
    }


def ss_fetch_lineups(fixture: Dict) -> Optional[Dict]:
    sofascore_id = fixture.get("sofascore_id")
    if not sofascore_id:
        return None
    session = ss_make_session(warm_up=False)
    session.headers.update({
        "Referer":           f"{SS_HOME}/event/{sofascore_id}",
        "X-Requested-With":  "XMLHttpRequest",
    })
    try:
        resp = session.get(f"{SS_API}/event/{sofascore_id}/lineups", timeout=15)
        if resp.status_code != 200:
            logger.warning(f"   SS lineups HTTP {resp.status_code}")
            return None
        return _ss_parse_lineups(resp.json())
    except Exception as e:
        logger.error(f"ss_fetch_lineups error: {e}")
        return None


# ── Flashscore lineups ────────────────────────────────────────────────────────

def _fs_parse_lineups(raw: str) -> Optional[Dict]:
    if not raw:
        return None
    lineups: Dict = {
        "home": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
        "away": {"formation": "4-4-2", "players": [], "bench": [], "coach": {"name": "Unknown"}},
    }
    found_any = False
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


def fs_fetch_lineups(fixture: Dict) -> Optional[Dict]:
    fs_id = fixture.get("flashscore_id") or fixture.get("match_id")
    if not fs_id:
        return None
    raw = fs_get(f"li_{fs_id}_1_en", base_delay=3.0)
    return _fs_parse_lineups(raw)


# ── Dispatcher ────────────────────────────────────────────────────────────────

def fetch_and_forward_lineups(fixture: Dict, col) -> bool:
    match_id = fixture.get("match_id")
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"
    source   = fixture.get("source", get_preferred())

    logger.info(f"📋 Fetching lineups for {label} via {source}")

    if source == "sofascore":
        lineups = ss_fetch_lineups(fixture)
        if not lineups and fixture.get("flashscore_id"):
            logger.info(f"   SS lineups failed — trying FS for {label}")
            lineups = fs_fetch_lineups(fixture)
    else:
        lineups = fs_fetch_lineups(fixture)
        if not lineups and fixture.get("sofascore_id"):
            logger.info(f"   FS lineups failed — trying SS for {label}")
            lineups = ss_fetch_lineups(fixture)

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
        logger.warning(f"❌ Backend rejected lineups: {r.status_code}")
        return False
    except Exception as e:
        logger.error(f"fetch_and_forward_lineups error: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# ══ STATISTICS FETCHERS (source-dispatched) ════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def ss_fetch_statistics(fixture: dict, live_data: dict) -> Optional[Dict]:
    sofascore_id = fixture.get("sofascore_id")
    if not sofascore_id:
        return None
    session = ss_make_session(warm_up=False)
    data, _ = ss_api_get(session, f"/event/{sofascore_id}/statistics")
    if not data:
        return None

    time_elapsed = live_data.get("time_elapsed", 0)
    time_extra   = live_data.get("time_extra", 0)
    minute_disp  = f"{time_elapsed}" + (f"+{time_extra}" if time_extra else "")

    def extract(groups, name, side="home"):
        for group in groups:
            for item in group.get("statisticsItems", []):
                if item.get("name", "").lower() == name.lower():
                    try:
                        return int(str(item.get(side, 0)).replace("%", "").strip())
                    except (ValueError, TypeError):
                        return 0
        return 0

    groups = data.get("statistics", [{}])[0].get("groups", [])
    return {
        "minute":                time_elapsed,
        "minute_display":        minute_disp,
        "home_score":            live_data.get("home_score", 0),
        "away_score":            live_data.get("away_score", 0),
        "ball_possession_home":  extract(groups, "Ball possession", "home"),
        "ball_possession_away":  extract(groups, "Ball possession", "away"),
        "total_shots_home":      extract(groups, "Total shots", "home"),
        "total_shots_away":      extract(groups, "Total shots", "away"),
        "shots_on_target_home":  extract(groups, "Shots on target", "home"),
        "shots_on_target_away":  extract(groups, "Shots on target", "away"),
        "corners_home":          extract(groups, "Corner kicks", "home"),
        "corners_away":          extract(groups, "Corner kicks", "away"),
        "fouls_home":            extract(groups, "Fouls", "home"),
        "fouls_away":            extract(groups, "Fouls", "away"),
        "offsides_home":         extract(groups, "Offsides", "home"),
        "offsides_away":         extract(groups, "Offsides", "away"),
        "yellow_cards_home":     extract(groups, "Yellow cards", "home"),
        "yellow_cards_away":     extract(groups, "Yellow cards", "away"),
        "red_cards_home":        extract(groups, "Red cards", "home"),
        "red_cards_away":        extract(groups, "Red cards", "away"),
        "pass_accuracy_home":    extract(groups, "Passes %", "home"),
        "pass_accuracy_away":    extract(groups, "Passes %", "away"),
        "timestamp":             datetime.now(timezone.utc).isoformat(),
    }


def fs_fetch_statistics(fixture: dict, live_data: dict) -> Optional[Dict]:
    fs_id = fixture.get("flashscore_id") or fixture.get("match_id")
    if not fs_id:
        return None

    raw = fs_get(f"od_{fs_id}", base_delay=2.0)
    if not raw:
        return None

    KEY_MAP = {
        "ball possession": ("ball_possession_home", "ball_possession_away"),
        "total shots":     ("total_shots_home",     "total_shots_away"),
        "shots on target": ("shots_on_target_home", "shots_on_target_away"),
        "corner kicks":    ("corners_home",           "corners_away"),
        "fouls":           ("fouls_home",              "fouls_away"),
        "offsides":        ("offsides_home",           "offsides_away"),
        "yellow cards":    ("yellow_cards_home",       "yellow_cards_away"),
        "red cards":       ("red_cards_home",          "red_cards_away"),
        "passes %":        ("pass_accuracy_home",      "pass_accuracy_away"),
    }

    stats: Dict[str, Any] = {}
    for row in raw.split("~"):
        for seg in row.split("¬"):
            if not seg.startswith("ST÷"):
                continue
            parts = seg[3:].split("÷")
            if len(parts) < 3:
                continue
            mapped = KEY_MAP.get(parts[0].lower())
            if mapped:
                try:
                    stats[mapped[0]] = int(str(parts[1]).replace("%", "").strip())
                    stats[mapped[1]] = int(str(parts[2]).replace("%", "").strip())
                except (ValueError, TypeError):
                    pass

    if not stats:
        return None

    te   = live_data.get("time_elapsed", 0)
    tx   = live_data.get("time_extra", 0)
    stats.update({
        "minute":         te,
        "minute_display": f"{te}" + (f"+{tx}" if tx else ""),
        "home_score":     live_data.get("home_score", 0),
        "away_score":     live_data.get("away_score", 0),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    })
    return stats


def fetch_and_forward_statistics(fixture: dict, live_data: dict):
    match_id = fixture["match_id"]
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"
    source   = fixture.get("source", get_preferred())

    if source == "sofascore":
        payload = ss_fetch_statistics(fixture, live_data)
        if not payload and fixture.get("flashscore_id"):
            payload = fs_fetch_statistics(fixture, live_data)
    else:
        payload = fs_fetch_statistics(fixture, live_data)
        if not payload and fixture.get("sofascore_id"):
            payload = ss_fetch_statistics(fixture, live_data)

    if not payload:
        logger.warning(f"   No stats for {label}")
        return

    payload["match_id"] = match_id
    try:
        r = std_requests.post(f"{FANCLASH_API}/games/statistics", json=payload, timeout=5)
        if r.status_code == 200:
            logger.info(f"📊 Stats forwarded for {label} ({payload.get('minute_display', '?')}')")
        else:
            logger.warning(f"❌ Stats failed: {r.status_code}")
    except Exception as e:
        logger.error(f"fetch_and_forward_statistics error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# ══ LIVE DATA FETCHERS (source-dispatched) ════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def ss_fetch_live_data(
    session: cffi_requests.Session,
    sofascore_id: int,
    retries: int = 4,
) -> Tuple[Optional[dict], cffi_requests.Session]:
    url = f"{SS_API}/event/{sofascore_id}"
    for attempt in range(retries):
        try:
            with SS_SEMAPHORE:
                time.sleep(random.uniform(5.0, 10.0))
                session.headers.update({
                    "Referer":          f"{SS_HOME}/event/{sofascore_id}",
                    "X-Requested-With": "XMLHttpRequest",
                })
                resp = session.get(url, timeout=15)

            if resp.status_code == 200:
                event     = resp.json().get("event", {})
                incidents = []
                try:
                    with SS_SEMAPHORE:
                        time.sleep(random.uniform(5.0, 10.0))
                        inc_resp = session.get(f"{SS_API}/event/{sofascore_id}/incidents", timeout=15)
                    if inc_resp.status_code == 200:
                        incidents = inc_resp.json().get("incidents", [])
                except Exception as ie:
                    logger.warning(f"   SS incidents error: {ie}")

                return {
                    "home_score":   (event.get("homeScore") or {}).get("current", 0),
                    "away_score":   (event.get("awayScore") or {}).get("current", 0),
                    "status_type":  (event.get("status") or {}).get("type", ""),
                    "status_code":  (event.get("status") or {}).get("code", 0),
                    "time_elapsed": event.get("time", {}).get("elapsed", 0),
                    "time_extra":   event.get("time", {}).get("extra", 0),
                    "incidents":    incidents,
                    "_source":      "sofascore",
                }, session

            elif resp.status_code == 403:
                wait = (2 ** attempt) * random.uniform(3, 6)
                logger.warning(f"   SS live 403 attempt {attempt+1} — back-off {wait:.0f}s")
                time.sleep(wait)
                continue
            else:
                time.sleep(5)

        except Exception as e:
            logger.warning(f"   SS live error attempt {attempt+1}: {e}")
            time.sleep(5)

    logger.warning(f"   SS all live retries failed for {sofascore_id} — rebuilding session")
    session = ss_make_session(warm_up=True)
    return None, session


def fs_fetch_live_data(fs_id: str) -> Optional[dict]:
    raw = fs_get(f"dc_{fs_id}", base_delay=3.0)
    if not raw:
        return None
    for f in _parse_fs_rows(raw):
        match_id = f.get("AA", "").strip()
        if not match_id:
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

        # Translate FS status_code into Sofascore-compatible status_type for shared poll loop
        status_type = "inprogress"
        if status_code == 3:
            status_type = "pause"   # HT
        elif status in ("completed",):
            status_type = "finished"

        incidents = _fs_parse_incidents(fs_get(f"d_hb_{fs_id}", base_delay=2.0) or "")

        return {
            "home_score":   home_score,
            "away_score":   away_score,
            "status_type":  status_type,
            "status_code":  status_code,
            "time_elapsed": time_elapsed,
            "time_extra":   time_extra,
            "incidents":    incidents,
            "_source":      "flashscore",
        }
    return None


def _fs_parse_incidents(raw: str) -> List[Dict]:
    """Parse d_hb_{id} and return a normalised incident list compatible with poll_live_game."""
    incidents = []
    if not raw:
        return incidents
    for row in raw.split("~"):
        for segment in row.split("¬"):
            if not segment.startswith("INC÷"):
                continue
            parts = segment[4:].split("÷")
            if len(parts) < 6:
                continue
            try:
                fs_type = parts[1].upper()
                # Translate FS incident types to the normalised set used in poll_live_game
                type_map = {
                    "G":   "goal",
                    "YC":  "card",
                    "RC":  "card",
                    "SB":  "substitution",
                    "MS":  "missed_penalty",
                    "PEN": "penalty",
                    "CO":  "corner",
                }
                inc_type = type_map.get(fs_type, fs_type.lower())
                inc = {
                    "id":           parts[0],
                    "incidentType": inc_type,
                    "incidentClass": "yellow" if fs_type == "YC" else ("red" if fs_type == "RC" else ""),
                    "isHome":       parts[4] == "1",
                    "time":         {"elapsed": int(parts[2]) if parts[2].isdigit() else 0,
                                     "extra":   int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0},
                    "player":       {"name": _clean(parts[5])} if len(parts) > 5 else {"name": "Unknown"},
                }
                if fs_type == "SB" and len(parts) > 6 and parts[6].strip():
                    inc["playerOut"] = {"name": _clean(parts[6])}
                    inc["playerIn"]  = inc["player"]
                elif fs_type in ("G",) and len(parts) > 6 and parts[6].strip():
                    inc["assist"] = {"name": _clean(parts[6])}
                incidents.append(inc)
            except (ValueError, IndexError):
                continue
    return incidents


# ─────────────────────────────────────────────────────────────────────────────
# ══ LIVE POLLER — unified, source-aware ═══════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

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
    for inc in reversed(incidents):
        if inc.get("incidentType", "").lower() == "goal" and inc.get("isHome") == is_home:
            scorer = _get_player_name(inc)
            assist = None
            if "assist" in inc:
                assist = _get_player_name(inc["assist"])
            elif "assistPlayer" in inc:
                assist = _get_player_name(inc["assistPlayer"])
            return scorer, assist
    return "Unknown", None


def _fetch_live(
    source: str,
    fixture: dict,
    ss_session: cffi_requests.Session,
) -> Tuple[Optional[dict], cffi_requests.Session]:
    """
    Fetch live data using `source`. On failure, automatically tries the other
    source. Returns (live_data, updated_ss_session).
    live_data["_source"] indicates which source actually answered.
    """
    if source == "sofascore":
        ss_id = fixture.get("sofascore_id")
        if ss_id:
            live, ss_session = ss_fetch_live_data(ss_session, ss_id)
            if live:
                return live, ss_session
        # SS failed — try FS
        fs_id = fixture.get("flashscore_id")
        if fs_id:
            logger.warning(f"   SS live failed — trying FS for {fixture['home_team']} vs {fixture['away_team']}")
            live = fs_fetch_live_data(fs_id)
            return live, ss_session
    else:
        fs_id = fixture.get("flashscore_id") or fixture.get("match_id")
        if fs_id:
            live = fs_fetch_live_data(fs_id)
            if live:
                return live, ss_session
        # FS failed — try SS
        ss_id = fixture.get("sofascore_id")
        if ss_id:
            logger.warning(f"   FS live failed — trying SS for {fixture['home_team']} vs {fixture['away_team']}")
            live, ss_session = ss_fetch_live_data(ss_session, ss_id)
            return live, ss_session

    return None, ss_session


def poll_live_game(fixture: dict, col, history_col):
    label    = f"{fixture['home_team']} vs {fixture['away_team']}"
    match_id = fixture["match_id"]
    source   = fixture.get("source", get_preferred())

    ss_session = ss_make_session(warm_up=(source == "sofascore"))

    # Check if already finished
    initial, ss_session = _fetch_live(source, fixture, ss_session)
    if initial:
        sc = initial["status_code"]
        st = initial["status_type"]
        if sc in (100, 110, 120) or st == "finished":
            logger.info(f"⏭  {label} already completed")
            update_fixture_status(match_id, "completed")
            update_db_status(col, match_id, "completed")
            move_completed_game_to_history(col, history_col, match_id)
            return

    update_fixture_status(match_id, "live")
    update_db_status(col, match_id, "live")
    logger.info(f"🔴 LIVE POLLING: {label} (source={source})")

    last_home        = 0
    last_away        = 0
    half_time_sent   = False
    full_time_sent   = False
    second_half_sent = False
    seen_incidents: set = set()
    poll_count       = 0

    while True:
        live, ss_session = _fetch_live(source, fixture, ss_session)
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

        # ── Goals ──────────────────────────────────────────────────────────
        if home_score > last_home:
            scorer, assist = _find_goal_scorer_and_assist(incidents, is_home=True)
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
            scorer, assist = _find_goal_scorer_and_assist(incidents, is_home=False)
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
            inc_id = str(inc.get("id", ""))
            if inc_id in seen_incidents:
                continue
            seen_incidents.add(inc_id)

            inc_type = inc.get("incidentType", "").lower()
            inc_cls  = inc.get("incidentClass", "").lower()
            is_home  = inc.get("isHome", True)
            team     = fixture["home_team"] if is_home else fixture["away_team"]
            minute   = (inc.get("time") or {}).get("elapsed", time_elapsed)
            extra    = (inc.get("time") or {}).get("extra", 0)
            m_disp   = f"{minute}" + (f"+{extra}" if extra else "")
            player   = _get_player_name(inc)
            text     = ""
            ev_type  = inc_type

            if inc_type == "goal":
                continue

            elif inc_type == "card":
                card = "yellow_card" if inc_cls == "yellow" else "red_card"
                icon = "🟨" if inc_cls == "yellow" else "🟥"
                text    = f"{icon} {inc_cls.upper()} CARD - {player} ({team})"
                ev_type = card
                logger.info(f"{icon} {inc_cls.upper()} CARD — {team}: {player} ({m_disp}')")
                forward_event(fixture, card, {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })

            elif inc_type == "substitution":
                p_out = _get_player_name(inc.get("playerOut") or {})
                p_in  = _get_player_name(inc.get("playerIn") or inc)
                text  = f"🔄 SUB: {p_out} → {p_in} ({team})"
                logger.info(f"🔄 SUB — {team}: {p_out} → {p_in} ({m_disp}')")
                forward_event(fixture, "substitution", {
                    "minute": minute, "minute_display": m_disp,
                    "player_out": p_out, "player_in": p_in, "team": team,
                })

            elif inc_type == "penalty":
                text = f"🎯 PENALTY! {player} ({team})"
                logger.info(f"🎯 PENALTY — {team}: {player} ({m_disp}')")
                forward_event(fixture, "penalty", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })

            elif inc_type == "missed_penalty":
                text = f"❌ MISSED PENALTY - {player} ({team})"
                forward_event(fixture, "missed_penalty", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })

            elif inc_type == "corner":
                text = f"🚩 CORNER - {team}"
                forward_event(fixture, "corner", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })

            elif inc_type == "offside":
                text = f"🚩 OFFSIDE - {player} ({team})"
                forward_event(fixture, "offside", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })

            elif inc_type == "shot":
                on_target = inc.get("on_target") or inc.get("onTarget", False)
                blocked   = inc.get("blocked", False)
                text      = (
                    f"🎯 SHOT ON TARGET - {player} ({team})" if on_target
                    else f"🛡️ SHOT BLOCKED - {player} ({team})" if blocked
                    else f"💨 SHOT OFF TARGET - {player} ({team})"
                )
                forward_event(fixture, "shot", {
                    "minute": minute, "minute_display": m_disp,
                    "player": player, "team": team,
                    "on_target": on_target, "blocked": blocked,
                })

            elif inc_type == "foul":
                text = f"⚠️ FOUL - {player} ({team})"
                forward_event(fixture, "foul", {
                    "minute": minute, "minute_display": m_disp, "player": player, "team": team,
                })

            if text:
                send_commentary(fixture, {
                    "minute": minute, "minute_display": m_disp, "text": text,
                    "event_type": ev_type, "home_score": home_score, "away_score": away_score,
                    "team": team,
                    "player": player if inc_type != "substitution" else None,
                })

        # ── Match phases ───────────────────────────────────────────────────
        is_ht = (status_type == "pause") or (status_code == 3)
        is_2h = (status_type == "inprogress" and half_time_sent) or (status_code == 4 and half_time_sent)
        is_ft = (status_code in (100, 110, 120)) or (status_type == "finished")

        if is_ht and not half_time_sent:
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

        if is_2h and not second_half_sent:
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

        if is_ft and not full_time_sent:
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

        poll_count += 1
        if poll_count % 5 == 0:
            logger.info(f"📊 Stats snapshot at {minute_disp}' for {label}")
            fetch_and_forward_statistics(fixture, live)

        time.sleep(POLL_INTERVAL_SEC)

    logger.info(f"✅ Done polling {label}")


# ─────────────────────────────────────────────────────────────────────────────
# ══ POLL QUEUE ════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

_poll_queue:          _queue.Queue  = _queue.Queue()
_queue_worker_started: bool         = False
_queue_lock:          threading.Lock = threading.Lock()


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
# ══ MAIN LOOP ═════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("🏆 FanClash — World Cup 2026 Combined Poller")
    logger.info(f"   Initial preferred source: {get_preferred()}")
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

    last_cleanup_time   = time.time()
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
            logger.info(f"📅 {len(upcoming)} fixture(s) in next 24h  (source={get_preferred()}):")
            for mins, f in upcoming:
                ko_local = (f["_kickoff_utc"] + NAIROBI_OFFSET).strftime("%H:%M")
                icon     = "🔴" if f.get("status") == "soon" else "⏳"
                logger.info(
                    f"   {icon} {f['home_team']} vs {f['away_team']} "
                    f"at {ko_local} EAT ({int(mins)} mins) [{f.get('source','')}]"
                )

            for mins_to_game, fixture in upcoming:
                mid   = fixture["match_id"]
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