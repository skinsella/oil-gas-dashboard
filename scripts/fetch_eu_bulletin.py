#!/usr/bin/env python3
"""
Fetch EU Weekly Oil Bulletin — heating gas oil prices.

Downloads the historical XLSX published every Thursday by the European Commission,
extracts:
  - Ireland weekly heating gas oil prices (EUR/litre, up to 2 years history)
  - Latest EU member-state comparison for the same week

Saves results to data/eu_bulletin.json for the GitHub Pages dashboard.
No API key required.
"""

import io
import json
import os
import re
import sys
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
import openpyxl

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "data", "eu_bulletin.json")

BULLETIN_PAGE = "https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en"
BASE_URL      = "https://energy.ec.europa.eu"

HISTORY_WEEKS = 104  # ~2 years of weekly data

COUNTRY_NAMES = {
    "AT": "Austria",     "BE": "Belgium",     "BG": "Bulgaria",
    "CY": "Cyprus",      "CZ": "Czech Rep.",  "DE": "Germany",
    "DK": "Denmark",     "EE": "Estonia",     "ES": "Spain",
    "FI": "Finland",     "FR": "France",      "GR": "Greece",
    "HR": "Croatia",     "HU": "Hungary",     "IE": "Ireland",
    "IT": "Italy",       "LT": "Lithuania",   "LU": "Luxembourg",
    "LV": "Latvia",      "MT": "Malta",       "NL": "Netherlands",
    "PL": "Poland",      "PT": "Portugal",    "RO": "Romania",
    "SE": "Sweden",      "SI": "Slovenia",    "SK": "Slovakia",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; oil-dashboard-bot/1.0)"}

# ── Fetch helpers ─────────────────────────────────────────────────────────────

def find_xlsx_url() -> str:
    """Scrape the bulletin page to find the historical XLSX download link."""
    resp = requests.get(BULLETIN_PAGE, timeout=30, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "History" in href and href.lower().endswith(".xlsx"):
            return BASE_URL + href if href.startswith("/") else href
    raise RuntimeError("Could not find historical XLSX link on EU Oil Bulletin page")


def download_xlsx(url: str) -> bytes:
    resp = requests.get(url, timeout=120, headers=HEADERS)
    resp.raise_for_status()
    return resp.content


# ── Parser ────────────────────────────────────────────────────────────────────

def parse_xlsx(data: bytes) -> dict:
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)

    # Prefer "Prices with taxes" sheet
    sheet_name = None
    for name in wb.sheetnames:
        if "tax" in name.lower():
            sheet_name = name
            break
    if sheet_name is None:
        sheet_name = wb.sheetnames[0]

    ws   = wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))

    if len(rows) < 4:
        raise RuntimeError(f"Sheet '{sheet_name}' has only {len(rows)} rows")

    headers = [str(h) if h is not None else "" for h in rows[0]]

    # ── Find Ireland heating gas oil column ──────────────────────────────────
    ie_col = None
    for i, h in enumerate(headers):
        if re.search(r"IE_price_with_tax_he.*ing_oil", h, re.IGNORECASE):
            ie_col = i
            break
    # Fallback: any IE+heating column
    if ie_col is None:
        for i, h in enumerate(headers):
            if h.upper().startswith("IE") and "heat" in h.lower():
                ie_col = i
                break
    if ie_col is None:
        sample = headers[:40]
        print(f"  Headers (first 40): {sample}", file=sys.stderr)
        raise RuntimeError("Could not find Ireland heating gas oil column")

    # ── Find EU country heating oil columns ──────────────────────────────────
    country_cols: dict[str, int] = {}
    for i, h in enumerate(headers):
        m = re.match(r"^([A-Z]{2})_price_with_tax_he.*ing_oil", h, re.IGNORECASE)
        if m:
            code = m.group(1).upper()
            if code in COUNTRY_NAMES:
                country_cols[code] = i

    # ── Parse data rows (index 3+, newest first) ─────────────────────────────
    ireland_history:  list[dict]       = []
    latest_eu_prices: dict[str, float] = {}
    first_valid_date: str | None       = None

    for row in rows[3:]:
        date_val = row[0]
        if date_val is None:
            continue

        # Normalise date
        if isinstance(date_val, datetime):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)[:10]

        ie_val = row[ie_col] if ie_col < len(row) else None
        if ie_val is None:
            continue
        try:
            ie_price = round(float(ie_val) / 1000.0, 4)   # EUR/1000L → EUR/L
        except (ValueError, TypeError):
            continue

        ireland_history.append({"week": date_str, "price_per_litre": ie_price})

        # Capture EU comparison from the latest valid row only
        if first_valid_date is None:
            first_valid_date = date_str
            for code, col in country_cols.items():
                v = row[col] if col < len(row) else None
                if v is not None:
                    try:
                        latest_eu_prices[code] = round(float(v) / 1000.0, 4)
                    except (ValueError, TypeError):
                        pass

        if len(ireland_history) >= HISTORY_WEEKS:
            break

    if not ireland_history:
        raise RuntimeError("No Ireland data found in XLSX")

    # Build EU comparison sorted cheapest → most expensive
    eu_comparison = sorted(
        [
            {
                "code":           code,
                "name":           COUNTRY_NAMES.get(code, code),
                "price_per_litre": price,
                "is_ireland":     code == "IE",
            }
            for code, price in latest_eu_prices.items()
        ],
        key=lambda x: x["price_per_litre"],
    )

    return {
        "as_of":              first_valid_date,
        "ireland_history":    ireland_history,
        "eu_comparison":      eu_comparison,
        "ie_col_header":      headers[ie_col],
        "country_cols_found": list(country_cols.keys()),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    now_utc = datetime.now(timezone.utc)
    print(f"[{now_utc.isoformat()}] Fetching EU Oil Bulletin...")

    # Load existing so partial failures preserve previous values
    existing: dict = {}
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH) as f:
                existing = json.load(f)
        except Exception as e:
            print(f"  Warning: could not read existing eu_bulletin.json: {e}")

    try:
        print("  Finding XLSX download link...")
        xlsx_url = find_xlsx_url()
        print(f"  Downloading: {xlsx_url}")
        xlsx_data = download_xlsx(xlsx_url)
        print(f"  Downloaded {len(xlsx_data):,} bytes")

        print("  Parsing XLSX...")
        result = parse_xlsx(xlsx_data)

        h0 = result["ireland_history"][0]
        print(f"  Ireland: {len(result['ireland_history'])} weeks  "
              f"latest={result['as_of']}  {h0['price_per_litre']:.4f} €/L")
        print(f"  EU countries: {len(result['eu_comparison'])}  "
              f"column header: {result['ie_col_header']}")

        output = {
            "last_updated":    now_utc.isoformat(),
            "source":          "EU Weekly Oil Bulletin — European Commission",
            "as_of":           result["as_of"],
            "ireland_history": result["ireland_history"],
            "eu_comparison":   result["eu_comparison"],
        }

    except Exception as e:
        print(f"  ERROR: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        if existing:
            print("  Falling back to existing data.")
            existing["last_updated"] = now_utc.isoformat()
            output = existing
        else:
            sys.exit(1)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
