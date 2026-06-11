import requests, time

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.flashscore.com/",
    "Origin": "https://www.flashscore.com",
    "x-fsign": "SW9D1eZo",
}

# Test 1: tournament fixtures  — t_1_8_{tournament_id}_{page}_{lang}_{something}
url = f"https://global.flashscore.ninja/2/x/feed/t_1_8_lvUBR5F8_3_en_1"
r = requests.get(url, headers=headers, timeout=10)
print(f"Tournament fixtures: HTTP {r.status_code}")
print(r.text[:500])