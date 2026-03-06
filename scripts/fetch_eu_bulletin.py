#!/usr/bin/env python3
"""
Fetch EU Weekly Oil Bulletin — heating gas oil prices + pass-through analysis.

Downloads the historical XLSX published every Thursday by the European Commission,
extracts:
  - Ireland weekly heating gas oil prices, with-tax and pre-tax (EUR/litre, 2yr)
  - Latest EU member-state comparison (with-tax)
  - Gross supply-chain margin history (pre-tax retail minus Brent crude cost)
  - Asymmetric pass-through statistics (β_up vs β_down)

Reads data/prices.json (written first by fetch_prices.py) for Brent/EUR/USD data.
Saves results to data/eu_bulletin.json.
No API key required.
"""

import io
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup
import openpyxl

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH  = os.path.join(SCRIPT_DIR, "..", "data", "eu_bulletin.json")
PRICES_PATH  = os.path.join(SCRIPT_DIR, "..", "data", "prices.json")

BULLETIN_PAGE = "https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en"
BASE_URL      = "https://energy.ec.europa.eu"

HISTORY_WEEKS  = 104   # ~2 years
LITRES_PER_BBL = 158.987

# ── Ireland Carbon Tax on Home Heating (kerosene / heating gas oil) ───────────
# Rate changes effective 1 May each year (Budget announcement, applied May)
CARBON_TAX_SCHEDULE = [
    ("2026-05-01", 71.00),
    ("2025-05-01", 63.50),
    ("2024-05-01", 56.00),
    ("2023-05-01", 48.50),
    ("2022-05-01", 41.00),
    ("2021-05-01", 33.50),
]
CO2_FACTOR_KEROSENE = 0.0025407  # tonnes CO2 per litre of kerosene/heating gas oil


def carbon_rate_for_week(week_str: str) -> float:
    """Return the carbon tax rate (€/tonne CO2) applicable for a given week (YYYY-MM-DD)."""
    for effective_date, rate in CARBON_TAX_SCHEDULE:
        if week_str >= effective_date:
            return rate
    return CARBON_TAX_SCHEDULE[-1][1]  # earliest known rate

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

# ── Fetch / download ──────────────────────────────────────────────────────────

def find_xlsx_url() -> str:
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


# ── XLSX parsing ──────────────────────────────────────────────────────────────

def _ie_col(headers: list, pattern: str) -> int | None:
    for i, h in enumerate(headers):
        if re.search(pattern, h, re.IGNORECASE):
            return i
    return None


def _date_str(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    s = str(val)[:10]
    return s if len(s) == 10 else None


def parse_with_tax(data: bytes) -> tuple[list, dict, str | None]:
    """
    Returns:
      - ireland_history: [{week, price_per_litre}, ...] newest first
      - eu_comparison: [{code, name, price_per_litre, is_ireland}, ...]
      - first_valid_date: str | None
    """
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    # Find sheet
    sheet_name = next((n for n in wb.sheetnames if "tax" in n.lower() and "wo" not in n.lower()), wb.sheetnames[0])
    ws   = wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 4:
        raise RuntimeError(f"Sheet '{sheet_name}' has only {len(rows)} rows")

    headers = [str(h) if h else "" for h in rows[0]]
    ie_col  = _ie_col(headers, r"IE_price_with_tax_heIEing_oil")
    if ie_col is None:
        raise RuntimeError("No Ireland with-tax column found")

    country_cols: dict[str, int] = {}
    for i, h in enumerate(headers):
        m = re.match(r"^([A-Z]{2})_price_with_tax_he.*ing_oil", h, re.I)
        if m:
            code = m.group(1).upper()
            if code in COUNTRY_NAMES:
                country_cols[code] = i

    ireland_history: list[dict]  = []
    latest_eu_prices: dict       = {}
    first_valid_date: str | None = None

    for row in rows[3:]:
        d = _date_str(row[0])
        if not d:
            continue
        v = row[ie_col] if ie_col < len(row) else None
        if v is None:
            continue
        try:
            price = round(float(v) / 1000.0, 4)
        except (ValueError, TypeError):
            continue
        ireland_history.append({"week": d, "price_per_litre": price})
        if first_valid_date is None:
            first_valid_date = d
            for code, col in country_cols.items():
                cv = row[col] if col < len(row) else None
                if cv is not None:
                    try:
                        latest_eu_prices[code] = round(float(cv) / 1000.0, 4)
                    except (ValueError, TypeError):
                        pass
        if len(ireland_history) >= HISTORY_WEEKS:
            break

    eu_comparison = sorted(
        [{"code": c, "name": COUNTRY_NAMES.get(c, c), "price_per_litre": p, "is_ireland": c == "IE"}
         for c, p in latest_eu_prices.items()],
        key=lambda x: x["price_per_litre"],
    )
    return ireland_history, eu_comparison, first_valid_date


def parse_pretax(data: bytes) -> list:
    """Extract Ireland pre-tax heating oil history from 'Prices wo taxes' sheet."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    sheet_name = next((n for n in wb.sheetnames if "wo" in n.lower()), None)
    if sheet_name is None:
        raise RuntimeError("'Prices wo taxes' sheet not found")
    ws   = wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 4:
        raise RuntimeError("Pre-tax sheet too small")

    headers = [str(h) if h else "" for h in rows[0]]
    ie_col  = _ie_col(headers, r"IE_price_wo_tax_heIEing_oil")
    if ie_col is None:
        raise RuntimeError("No Ireland pre-tax column found")

    history = []
    for row in rows[3:]:
        d = _date_str(row[0])
        if not d:
            continue
        v = row[ie_col] if ie_col < len(row) else None
        if v is None:
            continue
        try:
            history.append({"week": d, "pretax_eur_l": round(float(v) / 1000.0, 4)})
        except (ValueError, TypeError):
            pass
        if len(history) >= HISTORY_WEEKS:
            break
    return history


# ── Margin + pass-through computation ────────────────────────────────────────

def _nearest(lookup: dict, target: str, max_days: int = 7):
    """Find value in {date_str: value} lookup nearest to target date string."""
    if not lookup:
        return None
    try:
        td = datetime.strptime(target, "%Y-%m-%d")
    except ValueError:
        return None
    closest = min(lookup.keys(), key=lambda d: abs((datetime.strptime(d, "%Y-%m-%d") - td).days))
    if abs((datetime.strptime(closest, "%Y-%m-%d") - td).days) <= max_days:
        return lookup[closest]
    return None


def compute_margin_history(
    pretax_history: list,
    withtax_history: list,
    brent_weekly: list,
    eurusd_daily: list,
) -> list:
    """
    For each EU Bulletin week, compute:
      - pretax_eur_l:   Ireland ex-tax retail price (EU Bulletin)
      - withtax_eur_l:  Ireland with-tax retail price (EU Bulletin)
      - brent_eur_l:    Brent crude converted to EUR/litre
      - gross_margin:   pretax - brent  (supply chain markup, excl. tax)
      - tax_eur_l:      withtax - pretax
    """
    brent_map  = {b["period"]: b["value"] for b in brent_weekly}
    eurusd_map = {e["date"]:   e["rate"]  for e in eurusd_daily}
    withtax_map = {h["week"]: h["price_per_litre"] for h in withtax_history}

    result = []
    for h in pretax_history:
        wk      = h["week"]
        pretax  = h["pretax_eur_l"]
        withtax = withtax_map.get(wk)

        brent_usd = _nearest(brent_map,  wk, max_days=7)
        eurusd    = _nearest(eurusd_map, wk, max_days=5)

        if brent_usd is None or eurusd is None:
            continue

        brent_eur_l  = round(brent_usd / (eurusd * LITRES_PER_BBL), 4)
        gross_margin = round(pretax - brent_eur_l, 4)
        tax_eur_l    = round(withtax - pretax, 4) if withtax is not None else None

        # Tax decomposition
        carbon_eur_l    = round(carbon_rate_for_week(wk) * CO2_FACTOR_KEROSENE, 4)
        vat_eur_l       = round(withtax * (0.135 / 1.135), 4) if withtax is not None else None
        # excise = withtax/1.135 - pretax; other_mot = excise - carbon
        other_mot_eur_l = round((withtax / 1.135 - pretax) - carbon_eur_l, 4) if withtax is not None else None

        result.append({
            "week":           wk,
            "pretax_eur_l":   pretax,
            "withtax_eur_l":  withtax,
            "brent_eur_l":    brent_eur_l,
            "gross_margin":   gross_margin,
            "tax_eur_l":      tax_eur_l,
            "carbon_eur_l":   carbon_eur_l,
            "vat_eur_l":      vat_eur_l,
            "other_mot_eur_l": other_mot_eur_l,
        })

    # Return newest first (match existing convention)
    result.sort(key=lambda x: x["week"], reverse=True)
    return result


def compute_pass_through(margin_history: list) -> dict:
    """
    Asymmetric pass-through analysis.

    β_up:   mean ratio of Δ(pretax retail) / Δ(Brent EUR/L) for weeks when Brent rose
    β_down: mean ratio for weeks when Brent fell

    β_up > β_down → retailers pass on cost INCREASES faster than DECREASES
    (i.e. asymmetric — standard signal of market power / potential gouging)
    """
    # Need chronological order
    chron = sorted(margin_history, key=lambda x: x["week"])
    if len(chron) < 8:
        return {"signal": "INSUFFICIENT_DATA", "weeks_used": len(chron)}

    THRESHOLD = 0.003  # min weekly Brent move to count (€/L) — avoids noise

    d_retail = []
    d_brent  = []
    for i in range(1, len(chron)):
        dr = chron[i]["pretax_eur_l"] - chron[i-1]["pretax_eur_l"]
        db = chron[i]["brent_eur_l"]  - chron[i-1]["brent_eur_l"]
        if abs(db) >= THRESHOLD:
            d_retail.append(dr)
            d_brent.append(db)

    if not d_brent:
        return {"signal": "INSUFFICIENT_DATA", "weeks_used": 0}

    up_pairs = [(r, b) for r, b in zip(d_retail, d_brent) if b > 0]
    dn_pairs = [(r, b) for r, b in zip(d_retail, d_brent) if b < 0]

    def mean_ratio(pairs):
        if len(pairs) < 3:
            return None
        return sum(r / b for r, b in pairs) / len(pairs)

    pt_up = mean_ratio(up_pairs)
    pt_dn = mean_ratio(dn_pairs)

    if pt_up is None or pt_dn is None:
        return {
            "signal":          "INSUFFICIENT_DATA",
            "weeks_used":      len(d_brent),
            "pt_when_rising":  round(pt_up, 3) if pt_up else None,
            "pt_when_falling": round(pt_dn, 3) if pt_dn else None,
            "n_rising_weeks":  len(up_pairs),
            "n_falling_weeks": len(dn_pairs),
        }

    asymmetry = round(pt_up - pt_dn, 3)

    if asymmetry > 0.15:
        signal = "ASYMMETRIC"    # prices rise faster than they fall
    elif asymmetry < -0.15:
        signal = "FAVORABLE"     # prices fall faster than they rise (competitive)
    else:
        signal = "SYMMETRIC"     # roughly equal pass-through in both directions

    return {
        "signal":          signal,
        "asymmetry":       asymmetry,
        "pt_when_rising":  round(pt_up, 3),
        "pt_when_falling": round(pt_dn, 3),
        "weeks_used":      len(d_brent),
        "n_rising_weeks":  len(up_pairs),
        "n_falling_weeks": len(dn_pairs),
        "interpretation": (
            f"When Brent rises, retail moves {pt_up:.2f}× as much. "
            f"When Brent falls, retail moves {pt_dn:.2f}× as much."
        ),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    now_utc = datetime.now(timezone.utc)
    print(f"[{now_utc.isoformat()}] Fetching EU Oil Bulletin...")

    existing: dict = {}
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH) as f:
                existing = json.load(f)
        except Exception as e:
            print(f"  Warning: could not read existing eu_bulletin.json: {e}")

    # ── Load prices.json for Brent + EUR/USD ─────────────────────────────────
    brent_weekly: list = []
    eurusd_daily: list = []
    if os.path.exists(PRICES_PATH):
        try:
            with open(PRICES_PATH) as f:
                prices = json.load(f)
            brent_weekly = prices.get("brent_weekly", [])
            eurusd_daily = prices.get("eurusd", [])
            print(f"  Loaded prices.json: {len(brent_weekly)} weekly Brent pts, {len(eurusd_daily)} EUR/USD pts")
        except Exception as e:
            print(f"  Warning: could not read prices.json: {e}")
    else:
        print("  Warning: prices.json not found; margin analysis will be skipped")

    try:
        print("  Finding XLSX download link...")
        xlsx_url  = find_xlsx_url()
        print(f"  Downloading: {xlsx_url}")
        xlsx_data = download_xlsx(xlsx_url)
        print(f"  Downloaded {len(xlsx_data):,} bytes")

        print("  Parsing with-tax prices...")
        ireland_history, eu_comparison, as_of = parse_with_tax(xlsx_data)
        print(f"  With-tax: {len(ireland_history)} weeks  latest={as_of}  {ireland_history[0]['price_per_litre']:.4f} €/L")
        print(f"  EU comparison: {len(eu_comparison)} countries")

        print("  Parsing pre-tax (ex-tax) prices...")
        pretax_history = parse_pretax(xlsx_data)
        print(f"  Pre-tax: {len(pretax_history)} weeks  latest={pretax_history[0]['week']}  {pretax_history[0]['pretax_eur_l']:.4f} €/L")

        print("  Computing gross margin history...")
        margin_history = compute_margin_history(pretax_history, ireland_history, brent_weekly, eurusd_daily)
        print(f"  Margin history: {len(margin_history)} weeks")
        if margin_history:
            h0 = margin_history[0]
            print(f"  Latest ({h0['week']}): pretax={h0['pretax_eur_l']:.4f} brent={h0['brent_eur_l']:.4f} margin={h0['gross_margin']:.4f} €/L")

        print("  Computing pass-through stats...")
        pt_stats = compute_pass_through(margin_history)
        print(f"  Pass-through signal: {pt_stats['signal']}  "
              f"β_up={pt_stats.get('pt_when_rising','?')}  β_down={pt_stats.get('pt_when_falling','?')}")

        output = {
            "last_updated":    now_utc.isoformat(),
            "source":          "EU Weekly Oil Bulletin — European Commission",
            "as_of":           as_of,
            "ireland_history": ireland_history,
            "eu_comparison":   eu_comparison,
            "margin_history":  margin_history,
            "pass_through":    pt_stats,
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
