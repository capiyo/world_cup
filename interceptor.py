import requests

r = requests.get(
    "https://global.flashscore.ninja/2/x/feed/to_zeSHfCx3_SbLsX4y7_1",
    headers={
        "User-Agent": "Mozilla/5.0 Chrome/148.0.0.0",
        "Referer": "https://www.flashscore.com/",
        "x-fsign": "SW9D1eZo",
    },
    timeout=10
)

rows = r.text.split('~')
print(f"Total rows: {len(rows)}")

# Print first 10 rows field by field so we see all keys
for i, row in enumerate(rows[:10]):
    if not row.strip():
        continue
    print(f"\n--- ROW {i} ---")
    for part in row.split('¬'):
        if part.strip():
            print(f"  {part}")