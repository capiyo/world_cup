"""
Flashscore Connectivity Debugger
==================================
Run this FIRST before running the full poller.
It will tell you exactly which hosts resolve, which endpoints respond,
and what the correct feed URL structure is for your environment.

Run:  python flashscore_debug.py
"""

import socket
import time
import json
import sys

try:
    import requests
except ImportError:
    print("❌ requests not installed — run: pip install requests")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# 1. DNS RESOLUTION CHECK
# ─────────────────────────────────────────────────────────────────────────────

HOSTS_TO_CHECK = [
    "global.flashscore.com",
    "www.flashscore.com",
    "flashscore.com",
    "d.flashscore.com",
    "s.flashscore.com",
    "8.8.8.8",            # Google DNS — sanity check that internet works at all
]

print("\n" + "=" * 60)
print("1. DNS RESOLUTION")
print("=" * 60)

dns_results = {}
for host in HOSTS_TO_CHECK:
    try:
        ip = socket.gethostbyname(host)
        print(f"  ✅ {host:<35} → {ip}")
        dns_results[host] = ip
    except socket.gaierror as e:
        print(f"  ❌ {host:<35} → FAILED ({e})")
        dns_results[host] = None

# ─────────────────────────────────────────────────────────────────────────────
# 2. HTTP REACHABILITY CHECK
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("2. HTTP REACHABILITY")
print("=" * 60)

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}

URLS_TO_CHECK = [
    "https://www.flashscore.com",
    "https://www.flashscore.com/football",
    "https://global.flashscore.com",
    "https://d.flashscore.com",
]

http_results = {}
for url in URLS_TO_CHECK:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        print(f"  ✅ {url:<45} → HTTP {r.status_code} ({len(r.content)} bytes)")
        http_results[url] = r.status_code
    except requests.exceptions.ConnectionError as e:
        msg = str(e)[:80]
        print(f"  ❌ {url:<45} → CONNECTION ERROR: {msg}")
        http_results[url] = None
    except requests.exceptions.Timeout:
        print(f"  ⏱️  {url:<45} → TIMEOUT")
        http_results[url] = "timeout"
    except Exception as e:
        print(f"  ❌ {url:<45} → {type(e).__name__}: {e}")
        http_results[url] = None

# ─────────────────────────────────────────────────────────────────────────────
# 3. FLASHSCORE FEED ENDPOINT PROBE
#    Try multiple known X-Fsign token values and URL patterns
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("3. FLASHSCORE FEED ENDPOINT PROBE")
print("=" * 60)

# Known X-Fsign tokens seen in the wild (Flashscore rotates these occasionally)
FSIGN_CANDIDATES = [
    "SW9D1eZo",   # widely documented 2024-2025
    "t8Gd3mHq",   # seen in some 2025 scrapers
    "B8hRmp1Y",   # alternate
]

FEED_HOSTS = [
    "global.flashscore.com",
    "www.flashscore.com",
]

FEED_PATHS = [
    "/x/feed/?_={ts}&q=football/world/world-cup/fixtures/",
    "/x/feed/?_={ts}&q=football/2026-06-11/",
]

working_combo = None

for host in FEED_HOSTS:
    if dns_results.get(host) is None:
        print(f"\n  ⏭️  Skipping {host} — DNS failed")
        continue

    for token in FSIGN_CANDIDATES:
        for path_template in FEED_PATHS:
            ts   = int(time.time() * 1000)
            path = path_template.format(ts=ts)
            url  = f"https://{host}{path}"

            feed_headers = {
                "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                                   "Chrome/131.0.0.0 Safari/537.36",
                "Accept":          "text/plain, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer":         "https://www.flashscore.com/",
                "Origin":          "https://www.flashscore.com",
                "X-Fsign":         token,
            }

            try:
                r = requests.get(url, headers=feed_headers, timeout=10)
                status = r.status_code
                preview = r.text[:120].replace("\n", " ") if r.text else ""
                icon = "✅" if status == 200 else "❌"
                print(f"\n  {icon} host={host} token={token}")
                print(f"     path={path_template[:50]}")
                print(f"     HTTP {status}")
                if status == 200:
                    print(f"     Preview: {preview}")
                    if working_combo is None:
                        working_combo = {"host": host, "token": token, "url": url}
            except requests.exceptions.ConnectionError as e:
                print(f"\n  ❌ host={host} token={token} → DNS/Connection failed")
                break  # no point trying other tokens if host is unreachable
            except Exception as e:
                print(f"\n  ❌ host={host} token={token} → {type(e).__name__}: {e}")

            time.sleep(1.5)

# ─────────────────────────────────────────────────────────────────────────────
# 4. WORLD CUP SPECIFIC ENDPOINT TEST (if feed works)
# ─────────────────────────────────────────────────────────────────────────────

if working_combo:
    print("\n" + "=" * 60)
    print("4. WORLD CUP FIXTURE FETCH TEST")
    print("=" * 60)

    host  = working_combo["host"]
    token = working_combo["token"]

    WC_QUERIES = [
        "football/world/world-cup/fixtures/",
        "football/world/world-cup/results/",
        "football/world/world-cup-2026/fixtures/",    # alternate slug
        "football/world/fifa-world-cup/fixtures/",    # alternate slug
        "football/world/world-championship/fixtures/", # alternate slug
    ]

    feed_headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/131.0.0.0 Safari/537.36",
        "Accept":          "text/plain, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://www.flashscore.com/",
        "Origin":          "https://www.flashscore.com",
        "X-Fsign":         token,
    }

    for q in WC_QUERIES:
        ts  = int(time.time() * 1000)
        url = f"https://{host}/x/feed/?_={ts}&q={q}"
        try:
            r = requests.get(url, headers=feed_headers, timeout=10)
            preview = r.text[:200].replace("\n", " ") if r.text else "(empty)"
            icon = "✅" if r.status_code == 200 and r.text.strip() else (
                   "⚠️ " if r.status_code == 200 else "❌"
            )
            print(f"\n  {icon} query: {q}")
            print(f"     HTTP {r.status_code} | {len(r.text)} bytes")
            print(f"     Preview: {preview[:150]}")
        except Exception as e:
            print(f"\n  ❌ query: {q} → {e}")
        time.sleep(2)

# ─────────────────────────────────────────────────────────────────────────────
# 5. SUMMARY & RECOMMENDATION
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("5. SUMMARY")
print("=" * 60)

if dns_results.get("global.flashscore.com") is None:
    if dns_results.get("www.flashscore.com") is None:
        print("""
  ❌ BOTH flashscore hosts fail DNS.
  Possible causes:
    a) No internet connection
    b) Your ISP/network blocks flashscore.com
    c) Corporate/VPN firewall blocking the domain
    d) Windows hosts file blocking it

  Try:
    1. Open browser → go to https://www.flashscore.com
       If it loads → DNS issue specific to Python/this process
       If it doesn't → your network blocks flashscore entirely

    2. Run in terminal:
       nslookup global.flashscore.com
       nslookup www.flashscore.com

    3. Try adding to your script:
       import socket
       socket.setdefaulttimeout(30)
       # and use a custom DNS resolver:
       # pip install dnspython
""")
    else:
        print(f"""
  ⚠️  www.flashscore.com resolves ({dns_results['www.flashscore.com']})
     but global.flashscore.com does NOT.

  This means the feed subdomain is blocked or not in your DNS.
  Fix: use www.flashscore.com as the feed host instead of global.flashscore.com
  Update FS_FEED_BASE in the poller to:
    https://www.flashscore.com/x/feed/
""")
elif working_combo:
    print(f"""
  ✅ Working configuration found:
     Host:  {working_combo['host']}
     Token: {working_combo['token']}
     URL:   {working_combo['url']}

  Update your poller:
    FS_FEED_BASE   = "https://{working_combo['host']}/x/feed/"
    X_FSIGN_TOKEN  = "{working_combo['token']}"
""")
else:
    print("""
  ⚠️  DNS resolves but feed endpoint returns non-200.
  The X-Fsign token may have rotated.
  
  To get the current token:
    1. Open https://www.flashscore.com in Chrome
    2. DevTools (F12) → Network tab
    3. Filter by "x/feed"
    4. Click any request → Headers → find "x-fsign"
    5. Copy that value and update X_FSIGN_TOKEN in the poller
""")

print("=" * 60)
print("Debug complete.\n")