"""
Flashscore Token Finder + Feed Verifier
========================================
1. Fetches the Flashscore homepage
2. Extracts the live X-Fsign token from the embedded JS
3. Tests it against the feed endpoint
4. Prints the exact config lines to paste into the poller

Run AFTER adding 34.8.77.207 to your hosts file.

Run:  python flashscore_token_finder.py
"""

import re
import time
import socket
import requests

HEADERS_BROWSER = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

HEADERS_FEED = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept":          "text/plain, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.flashscore.com/",
    "Origin":          "https://www.flashscore.com",
}

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Check hosts file fix applied
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 1 — Checking DNS for global.flashscore.com")
print("=" * 60)

try:
    ip = socket.gethostbyname("global.flashscore.com")
    print(f"  ✅ Resolves to: {ip}")
    if ip != "34.8.77.207":
        print(f"  ⚠️  Different IP than www ({ip} vs 34.8.77.207) — may be fine")
    FEED_HOST = "global.flashscore.com"
except socket.gaierror:
    print("  ❌ Still failing — hosts file fix not applied yet")
    print("     Add this line to C:\\Windows\\System32\\drivers\\etc\\hosts:")
    print("     34.8.77.207    global.flashscore.com")
    print("\n  Trying www.flashscore.com as fallback host for feed...")
    FEED_HOST = "www.flashscore.com"

print(f"  Using feed host: {FEED_HOST}")

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Extract X-Fsign token from the homepage JS
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 2 — Extracting X-Fsign token from Flashscore JS")
print("=" * 60)

found_tokens = []

try:
    # Get homepage HTML to find JS bundle URLs
    r = requests.get("https://www.flashscore.com", headers=HEADERS_BROWSER, timeout=15)
    print(f"  Homepage: HTTP {r.status_code} ({len(r.content)} bytes)")
    html = r.text

    # Token pattern: alphanumeric, 8 chars, appears near "fsign" or "sign" in JS
    # Pattern 1: directly in HTML inline scripts
    token_matches = re.findall(r'["\']([A-Za-z0-9]{8})["\']', html)

    # Find JS bundle URLs
    js_urls = re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', html)
    js_urls = [u if u.startswith("http") else f"https://www.flashscore.com{u}" for u in js_urls]

    print(f"  Found {len(js_urls)} JS bundles")

    # Search JS bundles for the token
    for js_url in js_urls[:8]:  # check first 8 bundles max
        try:
            js_r = requests.get(js_url, headers=HEADERS_BROWSER, timeout=10)
            if js_r.status_code != 200:
                continue
            js_text = js_r.text

            # Pattern: fsign:"XXXXXXXX" or x-fsign":"XXXXXXXX" or sign:"XXXXXXXX"
            patterns = [
                r'fsign["\s:]+["\']([A-Za-z0-9]{6,12})["\']',
                r'[Ff]sign["\s:=]+["\']([A-Za-z0-9]{6,12})["\']',
                r'x-fsign["\s:=]+["\']([A-Za-z0-9]{6,12})["\']',
                r'FSIGN["\s:=]+["\']([A-Za-z0-9]{6,12})["\']',
                r'signKey["\s:=]+["\']([A-Za-z0-9]{6,12})["\']',
            ]

            for pat in patterns:
                matches = re.findall(pat, js_text, re.IGNORECASE)
                for m in matches:
                    if m not in found_tokens:
                        found_tokens.append(m)
                        bundle_name = js_url.split("/")[-1][:40]
                        print(f"  🔑 Found token candidate: '{m}' in {bundle_name}")

        except Exception as e:
            continue

except Exception as e:
    print(f"  ❌ Homepage fetch failed: {e}")

# Also try the known candidates as fallback
KNOWN_TOKENS = ["SW9D1eZo", "t8Gd3mHq", "B8hRmp1Y", "IluX2Lf0", "dds3X9mS"]
for t in KNOWN_TOKENS:
    if t not in found_tokens:
        found_tokens.append(t)

print(f"\n  Tokens to test: {found_tokens}")

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Test each token against feed endpoint
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 3 — Testing feed endpoint with each token")
print("=" * 60)

FEED_QUERIES = [
    "football/world/world-cup/fixtures/",
    "football/world/world-cup/results/",
    "football/world/world-cup-2026/fixtures/",
    "football/2026-06-11/",
    "football/2026-06-12/",
]

working_token  = None
working_query  = None
working_sample = None

for token in found_tokens:
    if working_token:
        break
    headers = {**HEADERS_FEED, "X-Fsign": token}

    for query in FEED_QUERIES:
        ts  = int(time.time() * 1000)
        url = f"https://{FEED_HOST}/x/feed/?_={ts}&q={query}"

        try:
            r = requests.get(url, headers=headers, timeout=12)
            status = r.status_code
            body   = r.text.strip() if r.text else ""

            if status == 200 and body:
                print(f"\n  ✅ WORKING! token='{token}' query='{query}'")
                print(f"     HTTP {status} | {len(body)} bytes")
                print(f"     Sample: {body[:200]}")
                working_token  = token
                working_query  = query
                working_sample = body
                break
            else:
                print(f"  ❌ token={token} query={query[:40]} → HTTP {status}"
                      + (f" body={body[:60]}" if body else " (empty)"))

        except Exception as e:
            print(f"  ❌ token={token} → {type(e).__name__}: {str(e)[:80]}")
            break  # host unreachable, skip remaining queries for this token

        time.sleep(1.0)

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: World Cup slug discovery
# ─────────────────────────────────────────────────────────────────────────────

if working_token:
    print("\n" + "=" * 60)
    print("STEP 4 — Finding correct World Cup tournament slug")
    print("=" * 60)

    headers = {**HEADERS_FEED, "X-Fsign": working_token}

    WC_SLUGS = [
        "football/world/world-cup/fixtures/",
        "football/world/world-cup-2026/fixtures/",
        "football/world/fifa-world-cup/fixtures/",
        "football/world/world-championship/fixtures/",
        "football/world/world-cup/",
    ]

    working_slug = None
    for slug in WC_SLUGS:
        ts  = int(time.time() * 1000)
        url = f"https://{FEED_HOST}/x/feed/?_={ts}&q={slug}"
        try:
            r = requests.get(url, headers=headers, timeout=12)
            body = r.text.strip() if r.text else ""
            has_data = r.status_code == 200 and len(body) > 50
            icon = "✅" if has_data else ("⚠️ " if r.status_code == 200 else "❌")
            print(f"  {icon} {slug:<50} HTTP {r.status_code} | {len(body)} bytes")
            if has_data and working_slug is None:
                working_slug = slug
                print(f"     Sample: {body[:200]}")
        except Exception as e:
            print(f"  ❌ {slug} → {e}")
        time.sleep(1.5)

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Print exact config to paste into poller
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 5 — CONFIG FOR worldcup_poller_flashscore.py")
print("=" * 60)

if working_token:
    slug = working_slug or "football/world/world-cup"
    # Extract just the tournament path without trailing /fixtures/
    wc_path = slug.replace("/fixtures/", "").replace("/", " ").strip()
    parts   = slug.replace("/fixtures/", "").split("/")

    print(f"""
  ✅ Paste these values into the top of worldcup_poller_flashscore.py:

  FS_FEED_BASE   = "https://{FEED_HOST}/x/feed/"
  X_FSIGN_TOKEN  = "{working_token}"

  WC_SPORT       = "{parts[0] if len(parts) > 0 else 'football'}"
  WC_COUNTRY     = "{parts[1] if len(parts) > 1 else 'world'}"
  WC_TOURNAMENT  = "{parts[2] if len(parts) > 2 else 'world-cup'}"
""")
else:
    print("""
  ❌ No working token/endpoint found.

  Manual steps to get the X-Fsign token:
    1. Open https://www.flashscore.com in Chrome
    2. Press F12 → Network tab → clear requests
    3. Refresh the page
    4. In the filter box type: x/feed
    5. Click any matching request
    6. Go to Headers tab
    7. Under "Request Headers" find: x-fsign
    8. Copy the value (8 chars, letters+numbers)
    9. Set X_FSIGN_TOKEN = "<that value>" in the poller

  Also check the URL of that request to confirm the feed host.
  It may be global.flashscore.com or a CDN subdomain.
""")

print("=" * 60)