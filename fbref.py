import asyncio
import random
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
#from playwright_stealth import stealth_async
#await Stealth().apply_stealth_async(page)
from bs4 import BeautifulSoup

URL = "https://fbref.com/en/comps/1/schedule/World-Cup-Scores-and-Fixtures"

async def scrape_fbref():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-infobars',
                '--disable-dev-shm-usage',
                '--disable-extensions',
                '--disable-gpu',
            ]
        )

        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            viewport={'width': 1366, 'height': 768},
            locale='en-US',
            timezone_id='America/New_York',
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'DNT': '1',
            }
        )

        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        # Block images/fonts to speed up + reduce fingerprint surface
        await page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda r: r.abort())

        print("Warming up on homepage...")
        await page.goto("https://fbref.com/", wait_until='domcontentloaded')
        await asyncio.sleep(random.uniform(2.0, 4.0))

        # Simulate scroll to look human
        await page.evaluate("window.scrollBy(0, 300)")
        await asyncio.sleep(random.uniform(1.0, 2.0))

        print("Navigating to fixtures page...")
        await page.goto(URL, wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(2.5, 4.0))

        # Wait for the fixtures table specifically
        try:
            await page.wait_for_selector('table#sched_all', timeout=15000)
            print("✅ Table found in DOM")
        except Exception:
            print("⚠️  Table selector timed out — dumping raw HTML for inspection")
            html = await page.content()
            with open("fbref_debug.html", "w", encoding="utf-8") as f:
                f.write(html)
            await browser.close()
            return

        html = await page.content()
        await browser.close()

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', id='sched_all')

        if not table:
            print("❌ Table not found after parse")
            return

        rows = table.find('tbody').find_all('tr')
        fixtures = []

        for row in rows:
            if 'spacer' in row.get('class', []):
                continue

            cells = row.find_all(['td', 'th'])
            if len(cells) < 5:
                continue

            try:
                fixture = {
                    'round':    row.find('th').get_text(strip=True) if row.find('th') else '',
                    'date':     row.find('td', {'data-stat': 'date'}).get_text(strip=True) if row.find('td', {'data-stat': 'date'}) else '',
                    'time':     row.find('td', {'data-stat': 'time'}).get_text(strip=True) if row.find('td', {'data-stat': 'time'}) else '',
                    'home':     row.find('td', {'data-stat': 'home_team'}).get_text(strip=True) if row.find('td', {'data-stat': 'home_team'}) else '',
                    'score':    row.find('td', {'data-stat': 'score'}).get_text(strip=True) if row.find('td', {'data-stat': 'score'}) else '',
                    'away':     row.find('td', {'data-stat': 'away_team'}).get_text(strip=True) if row.find('td', {'data-stat': 'away_team'}) else '',
                    'venue':    row.find('td', {'data-stat': 'venue'}).get_text(strip=True) if row.find('td', {'data-stat': 'venue'}) else '',
                    'referee':  row.find('td', {'data-stat': 'referee'}).get_text(strip=True) if row.find('td', {'data-stat': 'referee'}) else '',
                }
                if fixture['home'] or fixture['away']:
                    fixtures.append(fixture)
            except Exception as e:
                continue

        print(f"\n✅ Scraped {len(fixtures)} fixtures\n")
        for f in fixtures[:5]:  # preview first 5
            print(f"{f['date']} | {f['home']} {f['score']} {f['away']} | {f['venue']}")

        return fixtures

if __name__ == "__main__":
    asyncio.run(scrape_fbref())