#!/usr/bin/env python3
"""
Fetch oil and gas daily spot prices from EIA Open Data API v2.
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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "data", "prices.json")

DAYS = 60  # How many days of history to keep

# ── EIA series definitions ────────────────────────────────────────────────────

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

# ── Fetch helpers ─────────────────────────────────────────────────────────────

def fetch_petroleum_spots() -> dict:
    """Fetch petroleum spot prices from EIA API v2."""
    url = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
    series_ids = [v["series"] for v in PETROLEUM_SERIES.values()]

    params = [
        ("api_key", EIA_API_KEY),
        ("frequency", "daily"),
        ("data[0]", "value"),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "desc"),
        ("length", str(DAYS)),
    ] + [("facets[series][]", s) for s in series_ids]

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_natgas_spots() -> dict:
    """Fetch natural gas Henry Hub spot price from EIA API v2."""
    url = "https://api.eia.gov/v2/natural-gas/pri/fut/data/"

    params = [
        ("api_key", EIA_API_KEY),
        ("frequency", "daily"),
        ("data[0]", "value"),
        ("facets[series][]", "RNGWHHD"),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "desc"),
        ("length", str(DAYS)),
    ]

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_response(raw: dict, series_map: dict) -> dict[str, list]:
    """Parse EIA v2 response into {commodity_key: [{period, value}, ...]}."""
    result: dict[str, list] = {k: [] for k in series_map}

    for record in raw.get("response", {}).get("data", []):
        sid = record.get("series") or record.get("series-description", "")
        period = record.get("period")
        value = record.get("value")

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
    print(f"[{now_utc.isoformat()}] Fetching EIA spot prices...")

    # Load existing data so we can preserve anything that fails to refresh
    existing_commodities: dict = {}
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH) as f:
                existing_commodities = json.load(f).get("commodities", {})
        except Exception as e:
            print(f"  Warning: could not read existing prices.json: {e}")

    commodities = dict(existing_commodities)
    errors: list[str] = []

    # -- Petroleum ----
    try:
        raw = fetch_petroleum_spots()
        parsed = parse_response(raw, PETROLEUM_SERIES)
        for key, history in parsed.items():
            if history:
                meta = PETROLEUM_SERIES[key]
                commodities[key] = {
                    "name": meta["name"],
                    "unit": meta["unit"],
                    "tv_symbol": meta["tv_symbol"],
                    "history": history,
                }
                print(f"  {key:12s}  {len(history):2d} records  latest={history[0]['value']:8.4f} {meta['unit']}  ({history[0]['period']})")
            else:
                print(f"  {key:12s}  no records returned")
    except Exception as e:
        msg = f"Petroleum fetch failed: {e}"
        print(f"  WARNING: {msg}", file=sys.stderr)
        errors.append(msg)

    # -- Natural gas ----
    try:
        raw = fetch_natgas_spots()
        parsed = parse_response(raw, NATGAS_SERIES)
        for key, history in parsed.items():
            if history:
                meta = NATGAS_SERIES[key]
                commodities[key] = {
                    "name": meta["name"],
                    "unit": meta["unit"],
                    "tv_symbol": meta["tv_symbol"],
                    "history": history,
                }
                print(f"  {key:12s}  {len(history):2d} records  latest={history[0]['value']:8.4f} {meta['unit']}  ({history[0]['period']})")
            else:
                print(f"  {key:12s}  no records returned")
    except Exception as e:
        msg = f"Natural gas fetch failed: {e}"
        print(f"  WARNING: {msg}", file=sys.stderr)
        errors.append(msg)

    # -- Write output ----
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    output = {
        "last_updated": now_utc.isoformat(),
        "source": "EIA Open Data API v2 — https://www.eia.gov/opendata/",
        "errors": errors if errors else None,
        "commodities": commodities,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved → {OUTPUT_PATH}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
