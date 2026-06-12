import asyncio
import random
from playwright.async_api import async_playwright
#from playwright_stealth import stealth_async
from playwright_stealth import Stealth
from bs4 import BeautifulSoup

URL = "https://fbref.com/en/comps/1/schedule/World-Cup-Scores-and-Fixtures"

async def scrape_fbref():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # Watch what happens
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-infobars',
                '--disable-dev-shm-usage',
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

        print("Warming up on homepage...")
        await page.goto("https://fbref.com/", wait_until='domcontentloaded')
        await asyncio.sleep(random.uniform(3.0, 5.0))
        await page.evaluate("window.scrollBy(0, 300)")
        await asyncio.sleep(random.uniform(1.5, 3.0))

        print("Navigating to fixtures page...")
        await page.goto(URL, wait_until='domcontentloaded', timeout=60000)

        # Wait longer for Cloudflare to resolve + page to fully render
        print("Waiting for Cloudflare to clear...")
        await asyncio.sleep(8)

        # Check what's actually on the page
        title = await page.title()
        print(f"Page title: {title}")

        try:
            await page.wait_for_selector('table#sched_all', timeout=20000)
            print("✅ Table found")
        except Exception:
            html = await page.content()
            with open("fbref_debug.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("❌ Still timed out — check fbref_debug.html")
            await asyncio.sleep(5)  # Keep window open briefly to inspect
            await browser.close()
            return

        html = await page.content()
        await browser.close()

        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', id='sched_all')
        rows = table.find('tbody').find_all('tr')
        fixtures = []

        for row in rows:
            if 'spacer' in row.get('class', []):
                continue
            try:
                fixture = {
                    'round':   row.find('th').get_text(strip=True) if row.find('th') else '',
                    'date':    row.find('td', {'data-stat': 'date'}).get_text(strip=True),
                    'time':    row.find('td', {'data-stat': 'time'}).get_text(strip=True),
                    'home':    row.find('td', {'data-stat': 'home_team'}).get_text(strip=True),
                    'score':   row.find('td', {'data-stat': 'score'}).get_text(strip=True),
                    'away':    row.find('td', {'data-stat': 'away_team'}).get_text(strip=True),
                    'venue':   row.find('td', {'data-stat': 'venue'}).get_text(strip=True),
                    'referee': row.find('td', {'data-stat': 'referee'}).get_text(strip=True),
                }
                if fixture['home'] or fixture['away']:
                    fixtures.append(fixture)
            except Exception:
                continue

        print(f"\n✅ Scraped {len(fixtures)} fixtures\n")
        for f in fixtures[:5]:
            print(f"{f['date']} | {f['home']} {f['score']} {f['away']} | {f['venue']}")

        return fixtures

if __name__ == "__main__":
    asyncio.run(scrape_fbref())