#!/usr/bin/env python3
"""
Fetch oil and gas prices + weekly market fundamentals from EIA Open Data API v2.
Saves results to data/prices.json for the GitHub Pages dashboard.

EIA API key required — register free at https://www.eia.gov/opendata/
Set as environment variable: EIA_API_KEY
"""

import json
import os
import sys
from datetime import datetime, timezone

import requests

# ── Config ────────────────────────────────────────────────────────────────────

EIA_API_KEY = os.environ.get("EIA_API_KEY", "").strip()
if not EIA_API_KEY:
    print("ERROR: EIA_API_KEY environment variable is not set.", file=sys.stderr)
    print("Register for a free key at https://www.eia.gov/opendata/", file=sys.stderr)
    sys.exit(1)

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "data", "prices.json")

DAYS  = 60   # days of daily spot-price history
WEEKS = 52   # weeks of weekly fundamentals history

# ── Daily spot-price series ───────────────────────────────────────────────────

PETROLEUM_SERIES = {
    "WTI": {
        "series": "RWTC",
        "name": "WTI Crude Oil",
        "unit": "$/bbl",
        "tv_symbol": "TVC:USOIL",
    },
    "BRENT": {
        "series": "RBRTE",
        "name": "Brent Crude Oil",
        "unit": "$/bbl",
        "tv_symbol": "TVC:UKOIL",
    },
    "RBOB": {
        "series": "EER_EPMRU_PF4_Y35NY_DPG",
        "name": "Gasoline (NY Harbor Conv.)",
        "unit": "$/gal",
        "tv_symbol": "NYMEX:RB1!",
    },
    "HEATINGOIL": {
        "series": "EER_EPD2F_PF4_Y35NY_DPG",
        "name": "Heating Oil No.2 (NY Harbor)",
        "unit": "$/gal",
        "tv_symbol": "NYMEX:HO1!",
    },
}

NATGAS_SERIES = {
    "NATGAS": {
        "series": "RNGWHHD",
        "name": "Natural Gas (Henry Hub)",
        "unit": "$/MMBtu",
        "tv_symbol": "TVC:NGAS",
    },
}

# ── Weekly fundamentals series (EIA Weekly Petroleum Status Report) ───────────

# Petroleum stocks endpoint
STOCKS_SERIES = {
    "CRUDE_STOCKS": {
        "series": "WCRSTUS1",
        "name": "US Crude Oil Stocks",
        "unit": "Mbbls",
        "url": "https://api.eia.gov/v2/petroleum/stoc/wstk/data/",
    },
    "GASOLINE_STOCKS": {
        "series": "WGTSTUS1",
        "name": "US Total Gasoline Stocks",
        "unit": "Mbbls",
        "url": "https://api.eia.gov/v2/petroleum/stoc/wstk/data/",
    },
    "DISTILLATE_STOCKS": {
        "series": "WDISTUS1",
        "name": "US Distillate Stocks",
        "unit": "Mbbls",
        "url": "https://api.eia.gov/v2/petroleum/stoc/wstk/data/",
    },
    "SPR_STOCKS": {
        "series": "WCSSTUS1",
        "name": "Strategic Petroleum Reserve",
        "unit": "Mbbls",
        "url": "https://api.eia.gov/v2/petroleum/stoc/wstk/data/",
    },
}

# Supply / demand weekly endpoint
SUPPLY_SERIES = {
    "CRUDE_PRODUCTION": {
        "series": "WCRFPUS2",
        "name": "US Crude Production",
        "unit": "kb/d",
        "url": "https://api.eia.gov/v2/petroleum/sum/sndw/data/",
    },
    "REFINERY_UTIL": {
        "series": "WPULEUS3",
        "name": "US Refinery Utilization",
        "unit": "%",
        "url": "https://api.eia.gov/v2/petroleum/sum/sndw/data/",
    },
}

# Trade / movements endpoint
TRADE_SERIES = {
    "CRUDE_IMPORTS": {
        "series": "WCRIMUS2",
        "name": "US Crude Imports",
        "unit": "kb/d",
        "url": "https://api.eia.gov/v2/petroleum/move/wkly/data/",
    },
}

ALL_FUND_SERIES = {**STOCKS_SERIES, **SUPPLY_SERIES, **TRADE_SERIES}

# ── ECB EUR/USD exchange rate ──────────────────────────────────────────────────

ECB_EURUSD_URL   = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"
ECB_DAYS         = 730   # 2 years of daily EUR/USD (for margin analysis alignment)
BRENT_WEEKS      = 104   # 2 years of weekly Brent (for margin history)

# ── Fetch helpers ─────────────────────────────────────────────────────────────

def _get(url: str, params: list) -> dict:
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_petroleum_spots() -> dict:
    series_ids = [v["series"] for v in PETROLEUM_SERIES.values()]
    return _get("https://api.eia.gov/v2/petroleum/pri/spt/data/", [
        ("api_key", EIA_API_KEY), ("frequency", "daily"),
        ("data[0]", "value"),
        ("sort[0][column]", "period"), ("sort[0][direction]", "desc"),
        ("length", str(DAYS)),
    ] + [("facets[series][]", s) for s in series_ids])


def fetch_natgas_spots() -> dict:
    return _get("https://api.eia.gov/v2/natural-gas/pri/fut/data/", [
        ("api_key", EIA_API_KEY), ("frequency", "daily"),
        ("data[0]", "value"),
        ("facets[series][]", "RNGWHHD"),
        ("sort[0][column]", "period"), ("sort[0][direction]", "desc"),
        ("length", str(DAYS)),
    ])


def fetch_brent_weekly() -> list:
    """Fetch BRENT_WEEKS of weekly Brent spot prices for margin analysis."""
    raw = _get("https://api.eia.gov/v2/petroleum/pri/spt/data/", [
        ("api_key", EIA_API_KEY), ("frequency", "weekly"),
        ("data[0]", "value"),
        ("facets[series][]", "RBRTE"),
        ("sort[0][column]", "period"), ("sort[0][direction]", "desc"),
        ("length", str(BRENT_WEEKS)),
    ])
    result = []
    for rec in raw.get("response", {}).get("data", []):
        v = rec.get("value")
        if v is None or v == "":
            continue
        try:
            result.append({"period": rec["period"], "value": round(float(v), 4)})
        except (ValueError, TypeError):
            pass
    result.sort(key=lambda x: x["period"], reverse=True)
    return result


def fetch_ecb_eurusd() -> list:
    """Fetch ECB daily EUR/USD rate for the last ECB_DAYS observations."""
    resp = requests.get(
        ECB_EURUSD_URL,
        params={"format": "jsondata", "lastNObservations": str(ECB_DAYS), "detail": "dataonly"},
        timeout=30,
    )
    resp.raise_for_status()
    raw = resp.json()
    dates = [d["id"] for d in raw["structure"]["dimensions"]["observation"][0]["values"]]
    obs   = raw["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]
    result = []
    for idx_str, vals in obs.items():
        idx = int(idx_str)
        if idx < len(dates) and vals[0] is not None:
            result.append({"date": dates[idx], "rate": round(float(vals[0]), 4)})
    result.sort(key=lambda x: x["date"], reverse=True)
    return result


def fetch_fund_group(series_map: dict) -> dict:
    """Fetch one group of weekly fundamentals (all series share the same URL)."""
    url = next(iter(series_map.values()))["url"]
    series_ids = [v["series"] for v in series_map.values()]
    return _get(url, [
        ("api_key", EIA_API_KEY), ("frequency", "weekly"),
        ("data[0]", "value"),
        ("sort[0][column]", "period"), ("sort[0][direction]", "desc"),
        ("length", str(WEEKS * len(series_ids))),
    ] + [("facets[series][]", s) for s in series_ids])


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_response(raw: dict, series_map: dict) -> dict[str, list]:
    """Parse an EIA v2 response into {key: [{period, value}, ...]}."""
    result: dict[str, list] = {k: [] for k in series_map}
    for record in raw.get("response", {}).get("data", []):
        sid    = record.get("series") or record.get("series-description", "")
        period = record.get("period")
        value  = record.get("value")
        if value is None or value == "":
            continue
        try:
            value = float(value)
        except (ValueError, TypeError):
            continue
        for key, meta in series_map.items():
            if sid == meta["series"]:
                result[key].append({"period": period, "value": round(value, 4)})
                break
    for key in result:
        result[key].sort(key=lambda x: x["period"], reverse=True)
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    now_utc = datetime.now(timezone.utc)
    print(f"[{now_utc.isoformat()}] Fetching EIA data...")

    # Load existing data so partial failures preserve previous values
    existing: dict = {}
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH) as f:
                existing = json.load(f)
        except Exception as e:
            print(f"  Warning: could not read existing prices.json: {e}")

    commodities    = dict(existing.get("commodities", {}))
    fundamentals   = dict(existing.get("fundamentals", {}))
    eurusd         = list(existing.get("eurusd", []))
    brent_weekly   = list(existing.get("brent_weekly", []))
    errors: list[str] = []

    # ── Daily spot prices ──────────────────────────────────────────────────────
    print("  [spot prices]")
    try:
        raw    = fetch_petroleum_spots()
        parsed = parse_response(raw, PETROLEUM_SERIES)
        for key, history in parsed.items():
            if history:
                meta = PETROLEUM_SERIES[key]
                commodities[key] = {
                    "name": meta["name"], "unit": meta["unit"],
                    "tv_symbol": meta["tv_symbol"], "history": history,
                }
                print(f"    {key:12s}  {len(history):2d} pts  {history[0]['value']:8.4f} {meta['unit']}  ({history[0]['period']})")
            else:
                print(f"    {key:12s}  no records")
    except Exception as e:
        msg = f"Petroleum spot fetch failed: {e}"
        print(f"  WARNING: {msg}", file=sys.stderr)
        errors.append(msg)

    try:
        raw    = fetch_natgas_spots()
        parsed = parse_response(raw, NATGAS_SERIES)
        for key, history in parsed.items():
            if history:
                meta = NATGAS_SERIES[key]
                commodities[key] = {
                    "name": meta["name"], "unit": meta["unit"],
                    "tv_symbol": meta["tv_symbol"], "history": history,
                }
                print(f"    {key:12s}  {len(history):2d} pts  {history[0]['value']:8.4f} {meta['unit']}  ({history[0]['period']})")
            else:
                print(f"    {key:12s}  no records")
    except Exception as e:
        msg = f"Natural gas fetch failed: {e}"
        print(f"  WARNING: {msg}", file=sys.stderr)
        errors.append(msg)

    # ── Weekly fundamentals ────────────────────────────────────────────────────
    print("  [weekly fundamentals]")
    for group_name, group_map in [
        ("stocks",  STOCKS_SERIES),
        ("supply",  SUPPLY_SERIES),
        ("trade",   TRADE_SERIES),
    ]:
        try:
            raw    = fetch_fund_group(group_map)
            parsed = parse_response(raw, group_map)
            for key, history in parsed.items():
                if history:
                    meta = ALL_FUND_SERIES[key]
                    fundamentals[key] = {
                        "name": meta["name"],
                        "unit": meta["unit"],
                        "history": history,
                    }
                    h0 = history[0]
                    print(f"    {key:22s}  {len(history):2d} pts  {h0['value']} {meta['unit']}  ({h0['period']})")
                else:
                    print(f"    {key:22s}  no records")
        except Exception as e:
            msg = f"Fundamentals ({group_name}) fetch failed: {e}"
            print(f"  WARNING: {msg}", file=sys.stderr)
            errors.append(msg)

    # ── Weekly Brent (2-year history for margin analysis) ───────────────────────
    print("  [weekly Brent — 2yr]")
    try:
        brent_weekly = fetch_brent_weekly()
        if brent_weekly:
            print(f"    BRENT weekly  {len(brent_weekly):3d} pts  {brent_weekly[0]['value']:.4f} $/bbl  ({brent_weekly[0]['period']})")
        else:
            print("    BRENT weekly  no records")
    except Exception as e:
        msg = f"Weekly Brent fetch failed: {e}"
        print(f"  WARNING: {msg}", file=sys.stderr)
        errors.append(msg)

    # ── ECB EUR/USD ─────────────────────────────────────────────────────────────
    print("  [ECB EUR/USD — 2yr]")
    try:
        eurusd = fetch_ecb_eurusd()
        if eurusd:
            print(f"    EUR/USD  {len(eurusd):2d} pts  {eurusd[0]['rate']:.4f}  ({eurusd[0]['date']})")
        else:
            print("    EUR/USD  no records")
    except Exception as e:
        msg = f"ECB EUR/USD fetch failed: {e}"
        print(f"  WARNING: {msg}", file=sys.stderr)
        errors.append(msg)

    # ── Write output ───────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    output = {
        "last_updated": now_utc.isoformat(),
        "source":       "EIA Open Data API v2 — https://www.eia.gov/opendata/",
        "errors":       errors if errors else None,
        "commodities":  commodities,
        "fundamentals": fundamentals,
        "eurusd":        eurusd,
        "brent_weekly":  brent_weekly,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved → {OUTPUT_PATH}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
