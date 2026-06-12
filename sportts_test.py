# test_thesportsdb.py
import requests

# Get all events for World Cup 2026
url = "https://www.thesportsdb.com/api/v1/json/3/eventsnext.php?id=4772"  # World Cup ID
response = requests.get(url, timeout=30)
print(response.json())