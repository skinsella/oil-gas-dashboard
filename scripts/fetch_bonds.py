#!/usr/bin/env python3
"""
Fetch sovereign bond yields from multiple free public APIs.
Saves results to data/bonds.json for the GitHub Pages dashboard.

Data sources (all free, no paid plans required):
  - US 2Y & 10Y:  FRED (Federal Reserve Economic Data) — daily
  - DE 2Y & 10Y:  Bundesbank SDMX API — daily
  - IE 10Y:       ECB Interest Rate Statistics — monthly
  - UK 10Y:       FRED (OECD series) — monthly

FRED API key required — register free at https://fred.stlouisfed.org/docs/api/api_key.html
Set as environment variable: FRED_API_KEY
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta

import requests

# ── Config ────────────────────────────────────────────────────────────────────

FRED_API_KEY = (os.environ.get("FRED_API_KEY") or "").strip() or "99bc741f2afe7c4764777470602bd2a3"

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "data", "bonds.json")

FRED_DAYS  = 120   # ~4 months of daily observations
BBK_DAYS   = 120   # ~4 months from Bundesbank

# ── FRED series ──────────────────────────────────────────────────────────────

FRED_SERIES = {
    "US10Y": {"series_id": "DGS10",           "name": "US 10-Year Treasury",  "country": "US", "maturity": "10Y", "freq": "daily"},
    "US02Y": {"series_id": "DGS2",            "name": "US 2-Year Treasury",   "country": "US", "maturity": "2Y",  "freq": "daily"},
    "UK10Y": {"series_id": "IRLTLT01GBM156N", "name": "UK 10-Year Gilt",      "country": "GB", "maturity": "10Y", "freq": "monthly"},
}

# ── Bundesbank series (German Bunds) ────────────────────────────────────────

BBK_SERIES = {
    "DE10Y": {
        "key": "BBSIS/D.I.ZAR.ZI.EUR.S1311.B.A604.R10XX.R.A.A._Z._Z.A",
        "name": "German 10-Year Bund",
        "country": "DE", "maturity": "10Y", "freq": "daily",
    },
    "DE02Y": {
        "key": "BBSIS/D.I.ZAR.ZI.EUR.S1311.B.A604.R02XX.R.A.A._Z._Z.A",
        "name": "German 2-Year Schatz",
        "country": "DE", "maturity": "2Y", "freq": "daily",
    },
}

# ── ECB series (Irish bonds) ────────────────────────────────────────────────

ECB_SERIES = {
    "IE10Y": {
        "key": "IRS/M.IE.L.L40.CI.0000.EUR.N.Z",
        "name": "Irish 10-Year Bond",
        "country": "IE", "maturity": "10Y", "freq": "monthly",
    },
}

# ── Fetch helpers ────────────────────────────────────────────────────────────

def fetch_fred(series_id: str, days: int = FRED_DAYS) -> list:
    """Fetch observations from FRED API. Returns [{date, value}, ...] newest-first."""
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    resp = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "observation_start": start,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    result = []
    for obs in data.get("observations", []):
        val = obs.get("value", ".")
        if val == "." or val is None:
            continue
        try:
            result.append({"date": obs["date"], "value": round(float(val), 3)})
        except (ValueError, TypeError):
            pass
    return result


def fetch_bundesbank(key: str, days: int = BBK_DAYS) -> list:
    """Fetch daily yield data from Bundesbank SDMX API. Returns [{date, value}, ...] newest-first."""
    resp = requests.get(
        f"https://api.statistiken.bundesbank.de/rest/data/{key}",
        params={"lastNObservations": str(days), "format": "csv"},
        timeout=30,
    )
    resp.raise_for_status()
    result = []
    for line in resp.text.splitlines():
        # CSV format: date;value;flags  (German decimal: comma)
        parts = line.split(";")
        if len(parts) < 2:
            continue
        date_str = parts[0].strip().strip('"')
        val_str  = parts[1].strip().strip('"')
        # Skip header/metadata lines
        if not date_str or not date_str[0].isdigit():
            continue
        # Skip missing values (Bundesbank uses "." for no data)
        if val_str == "." or not val_str:
            continue
        try:
            # German decimal format: comma → dot
            value = round(float(val_str.replace(",", ".")), 3)
            result.append({"date": date_str, "value": value})
        except (ValueError, TypeError):
            pass
    result.sort(key=lambda x: x["date"], reverse=True)
    return result


def fetch_ecb(key: str, n_obs: int = 24) -> list:
    """Fetch monthly data from ECB SDMX API. Returns [{date, value}, ...] newest-first."""
    resp = requests.get(
        f"https://data-api.ecb.europa.eu/service/data/{key}",
        params={"format": "jsondata", "lastNObservations": str(n_obs), "detail": "dataonly"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status"):
        raise RuntimeError(f"ECB error: {data.get('detail', 'unknown')}")
    series = list(data["dataSets"][0]["series"].values())[0]
    obs    = series["observations"]
    dates  = [v["id"] for v in data["structure"]["dimensions"]["observation"][0]["values"]]
    result = []
    for idx_str, vals in obs.items():
        idx = int(idx_str)
        if idx < len(dates) and vals[0] is not None:
            result.append({"date": dates[idx], "value": round(float(vals[0]), 3)})
    result.sort(key=lambda x: x["date"], reverse=True)
    return result


def compute_spreads(yields_data: dict) -> dict:
    """Compute 10Y-2Y yield spread for each country where both maturities exist."""
    # Map display country code → key prefix used in yields_data
    PREFIX = {"US": "US", "DE": "DE", "GB": "UK", "IE": "IE"}
    spreads = {}
    for country in ["US", "DE", "GB", "IE"]:
        p = PREFIX[country]
        key_10y = f"{p}10Y"
        key_2y  = f"{p}02Y"
        hist_10y = {h["date"]: h["value"] for h in yields_data.get(key_10y, {}).get("history", [])}
        hist_2y  = {h["date"]: h["value"] for h in yields_data.get(key_2y, {}).get("history", [])}
        common_dates = sorted(set(hist_10y) & set(hist_2y), reverse=True)
        if common_dates:
            spreads[country] = [
                {"date": d, "value": round(hist_10y[d] - hist_2y[d], 3)}
                for d in common_dates
            ]
        else:
            spreads[country] = None
    return spreads


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    now_utc = datetime.now(timezone.utc)
    print(f"[{now_utc.isoformat()}] Fetching sovereign bond yields...")

    # Load existing data so partial failures preserve previous values
    existing: dict = {}
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH) as f:
                existing = json.load(f)
        except Exception as e:
            print(f"  Warning: could not read existing bonds.json: {e}")

    yields_data: dict = dict(existing.get("yields", {}))
    errors: list[str] = []

    # ── FRED (US daily + UK monthly) ─────────────────────────────────────────
    print("  [FRED]")
    for key, meta in FRED_SERIES.items():
        print(f"    {key:6s}", end=" ", flush=True)
        try:
            history = fetch_fred(meta["series_id"])
            if history:
                yields_data[key] = {
                    "name": meta["name"],
                    "country": meta["country"],
                    "maturity": meta["maturity"],
                    "unit": "%",
                    "freq": meta["freq"],
                    "history": history,
                }
                print(f"{len(history):3d} pts  {history[0]['value']:.3f}%  ({history[0]['date']})")
            else:
                print("no records")
        except Exception as e:
            msg = f"{key} (FRED) fetch failed: {e}"
            print(f"FAILED — {e}")
            errors.append(msg)

    # ── Bundesbank (German Bunds daily) ──────────────────────────────────────
    print("  [Bundesbank]")
    for key, meta in BBK_SERIES.items():
        print(f"    {key:6s}", end=" ", flush=True)
        try:
            history = fetch_bundesbank(meta["key"])
            if history:
                yields_data[key] = {
                    "name": meta["name"],
                    "country": meta["country"],
                    "maturity": meta["maturity"],
                    "unit": "%",
                    "freq": meta["freq"],
                    "history": history,
                }
                print(f"{len(history):3d} pts  {history[0]['value']:.3f}%  ({history[0]['date']})")
            else:
                print("no records")
        except Exception as e:
            msg = f"{key} (Bundesbank) fetch failed: {e}"
            print(f"FAILED — {e}")
            errors.append(msg)

    # ── ECB (Irish bonds monthly) ────────────────────────────────────────────
    print("  [ECB]")
    for key, meta in ECB_SERIES.items():
        print(f"    {key:6s}", end=" ", flush=True)
        try:
            history = fetch_ecb(meta["key"])
            if history:
                yields_data[key] = {
                    "name": meta["name"],
                    "country": meta["country"],
                    "maturity": meta["maturity"],
                    "unit": "%",
                    "freq": meta["freq"],
                    "history": history,
                }
                print(f"{len(history):3d} pts  {history[0]['value']:.3f}%  ({history[0]['date']})")
            else:
                print("no records")
        except Exception as e:
            msg = f"{key} (ECB) fetch failed: {e}"
            print(f"FAILED — {e}")
            errors.append(msg)

    # ── Compute yield spreads ────────────────────────────────────────────────
    spreads = compute_spreads(yields_data)

    # ── Write output ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    output = {
        "last_updated": now_utc.isoformat(),
        "source": "FRED, Bundesbank, ECB",
        "errors": errors if errors else None,
        "yields": yields_data,
        "spreads": spreads,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved -> {OUTPUT_PATH}")
    if errors:
        print(f"  {len(errors)} error(s) — see bonds.json for details", file=sys.stderr)


if __name__ == "__main__":
    main()
