import requests
from datetime import datetime, timezone

def fetch_lineups_by_date(home_team: str, away_team: str) -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    r = requests.get(
        "https://v3.football.api-sports.io/fixtures",
        headers={"x-apisports-key": "YOUR_KEY"},
        params={"date": today, "league": 1, "season": 2026},  # league 1 = World Cup
        timeout=10
    )
    
    fixtures = r.json().get("response", [])
    
    for f in fixtures:
        teams = f["teams"]
        if home_team.lower() in teams["home"]["name"].lower() or away_team.lower() in teams["away"]["name"].lower():
            fixture_id = f["fixture"]["id"]
            
            # Now get lineups
            r2 = requests.get(
                "https://v3.football.api-sports.io/fixtures/lineups",
                headers={"x-apisports-key": "YOUR_KEY"},
                params={"fixture": fixture_id},
                timeout=10
            )
            return r2.json().get("response", [])
    
    return []

data = fetch_lineups_by_date("Qatar", "Switzerland")
print(data)