#!/usr/bin/env python3
"""
Scrape Irish home heating oil (kerosene) prices by county from oilprices.ie
Saves results to data/ireland_prices.json for the GitHub Pages dashboard.

No API key required — public data from https://www.oilprices.ie/
"""

import json
import os
import re
from datetime import datetime, timezone, date

import requests
from bs4 import BeautifulSoup

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "data", "ireland_prices.json")
SOURCE_URL  = "https://www.oilprices.ie/"
MAX_HISTORY = 60

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def scrape_county_prices() -> dict[str, float]:
    """Return {county: price_500l} from the oilprices.ie county average table."""
    resp = requests.get(SOURCE_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", id="CountyAvgList")
    if not table:
        raise ValueError("CountyAvgList table not found on oilprices.ie")

    counties: dict[str, float] = {}
    for row in table.find("tbody").find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        county    = cells[0].get_text(strip=True)
        price_str = cells[1].get_text(strip=True).replace(",", "").replace("€", "")
        try:
            counties[county] = round(float(re.search(r"[\d.]+", price_str).group()), 2)
        except (AttributeError, ValueError):
            continue

    return counties


def main() -> None:
    now   = datetime.now(timezone.utc)
    today = date.today().isoformat()
    print(f"[{now.isoformat()}] Scraping Irish heating oil prices...")

    # Load existing
    existing: dict = {"history": [], "counties": {}}
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH) as f:
                existing = json.load(f)
        except Exception as e:
            print(f"  Warning: could not load existing data: {e}")

    # Scrape
    current = scrape_county_prices()
    if not current:
        raise RuntimeError("No county data scraped")

    prices       = list(current.values())
    national_avg = round(sum(prices) / len(prices), 2)
    print(f"  {len(current)} counties · national avg €{national_avg}/500L")

    # Build county objects with day-on-day change
    prev = existing.get("counties", {})
    county_data: dict = {}
    for county, price in sorted(current.items(), key=lambda x: x[1]):
        prev_price = prev.get(county, {}).get("price_500l")
        change     = round(price - prev_price, 2)     if prev_price is not None else None
        change_pct = round(change / prev_price * 100, 2) if prev_price is not None else None
        county_data[county] = {
            "price_500l":     price,
            "price_per_litre": round(price / 500, 4),
            "prev_500l":      prev_price,
            "change":         change,
            "change_pct":     change_pct,
        }
        arrow = ("▲ " if change and change > 0 else "▼ " if change and change < 0 else "  ")
        print(f"    {county:15s}  €{price:7.2f}  {arrow}{change if change is not None else ''}")

    # Update history (one entry per day)
    history = existing.get("history", [])
    entry   = {"date": today, "national_avg": national_avg}
    if history and history[0].get("date") == today:
        history[0] = entry
    else:
        history.insert(0, entry)
    history = history[:MAX_HISTORY]

    output = {
        "last_updated":    now.isoformat(),
        "source":          "oilprices.ie",
        "national_avg_500l": national_avg,
        "counties":        county_data,
        "history":         history,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"  Saved → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
