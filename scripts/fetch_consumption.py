#!/usr/bin/env python3
"""
Fetch Irish oil consumption data from NORA (National Oil Reserves Agency).
Downloads the monthly volumes XLS file, parses all years (2008-present),
runs a Holt-Winters seasonal forecast, and saves to data/consumption.json.

Source: https://www.nora.ie/volumes-of-oil-consumption
Data: Monthly volumes subject to NORA Levy in litres, converted to megalitres.
"""

import json
import math
import os
import sys
import tempfile
from datetime import datetime, timezone

import requests
import xlrd

# ── Config ────────────────────────────────────────────────────────────────────

NORA_XLS_URL = "https://www.nora.ie/_files/ugd/d76141_fb80440f942c4c60bb532a84ba6c15d8.xls"

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "data", "consumption.json")

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

LITRES_PER_ML = 1_000_000  # convert litres → megalitres


# ── XLS Parsing ──────────────────────────────────────────────────────────────

def download_xls(url: str) -> str:
    """Download XLS to a temp file, return its path."""
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    fd, path = tempfile.mkstemp(suffix=".xls")
    with os.fdopen(fd, "wb") as f:
        f.write(resp.content)
    return path


def _find_header_row(sh) -> int:
    """Find the row containing 'Month' in column 1."""
    for r in range(sh.nrows):
        val = str(sh.cell_value(r, 1)).strip()
        if val.lower() == "month":
            return r
    return -1


def _find_col_indices(sh, header_row: int) -> dict:
    """Map category names to column indices from the header row."""
    headers = [str(sh.cell_value(header_row, c)).strip().upper() for c in range(sh.ncols)]
    indices = {}

    for c, h in enumerate(headers):
        if h == "GASOLINE" and "gasoline" not in indices:
            indices["gasoline"] = c
        elif h == "KEROSENE":
            indices["kerosene"] = c
        elif h == "GASOIL" or h == "GASOIL 1000 PPM":
            indices["gasoil_1"] = c
        elif h == "GASOIL 10 PPM":
            indices["gasoil_2"] = c
        elif h == "MOTOR DIESEL" and "diesel" not in indices:
            indices["diesel"] = c
        elif h == "FUEL OIL":
            indices["fuel_oil"] = c
        elif h == "ALL FUELS":
            indices["total"] = c

    return indices


def _safe_float(val) -> float:
    """Convert cell value to float, return 0 for empty/non-numeric."""
    if val == "" or val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def parse_xls(path: str) -> list[dict]:
    """Parse the NORA XLS workbook. Returns list of monthly records sorted by date."""
    wb = xlrd.open_workbook(path)
    records = []

    for si in range(wb.nsheets):
        sh = wb.sheet_by_index(si)
        name = sh.name

        # Extract year from sheet name (last 2 digits)
        digits = "".join(c for c in name if c.isdigit())
        if not digits:
            continue
        year = 2000 + int(digits[-2:])

        header_row = _find_header_row(sh)
        if header_row < 0:
            print(f"  Warning: no header row in sheet '{name}', skipping")
            continue

        cols = _find_col_indices(sh, header_row)
        if "total" not in cols:
            print(f"  Warning: no ALL FUELS column in sheet '{name}', skipping")
            continue

        # Data starts 3 rows after header (header, sub-header, blank)
        # But for 2018+ sheets it's 3 rows after header_row
        # Detect: find first row after header where col 1 matches a month name
        data_start = header_row + 1
        for r in range(header_row + 1, min(header_row + 6, sh.nrows)):
            val = str(sh.cell_value(r, 1)).strip()
            if val in MONTHS:
                data_start = r
                break

        for mi, month_name in enumerate(MONTHS):
            r = data_start + mi
            if r >= sh.nrows:
                break

            # Verify this row is the expected month
            row_label = str(sh.cell_value(r, 1)).strip()
            if row_label not in MONTHS:
                continue

            total_litres = _safe_float(sh.cell_value(r, cols["total"]))
            if total_litres <= 0:
                continue  # no data yet (e.g. future months in current year)

            rec = {
                "date": f"{year}-{mi+1:02d}",
                "total": round(total_litres / LITRES_PER_ML, 2),
            }

            # Category breakdowns
            if "gasoline" in cols:
                rec["gasoline"] = round(_safe_float(sh.cell_value(r, cols["gasoline"])) / LITRES_PER_ML, 2)
            if "kerosene" in cols:
                rec["kerosene"] = round(_safe_float(sh.cell_value(r, cols["kerosene"])) / LITRES_PER_ML, 2)

            # Combine gasoil variants
            gasoil = _safe_float(sh.cell_value(r, cols["gasoil_1"])) if "gasoil_1" in cols else 0
            if "gasoil_2" in cols:
                gasoil += _safe_float(sh.cell_value(r, cols["gasoil_2"]))
            rec["gasoil"] = round(gasoil / LITRES_PER_ML, 2)

            if "diesel" in cols:
                rec["diesel"] = round(_safe_float(sh.cell_value(r, cols["diesel"])) / LITRES_PER_ML, 2)
            if "fuel_oil" in cols:
                rec["fuel_oil"] = round(_safe_float(sh.cell_value(r, cols["fuel_oil"])) / LITRES_PER_ML, 2)

            records.append(rec)

    records.sort(key=lambda x: x["date"])
    return records


# ── Holt-Winters Additive Seasonal Forecast ──────────────────────────────────

def _init_seasonal(data: list[float], period: int) -> list[float]:
    """Initialize seasonal components from first few complete cycles."""
    n_cycles = min(len(data) // period, 3)
    if n_cycles == 0:
        return [0.0] * period

    seasonal = [0.0] * period
    for i in range(period):
        vals = [data[c * period + i] for c in range(n_cycles)]
        cycle_means = [
            sum(data[c * period:(c + 1) * period]) / period
            for c in range(n_cycles)
        ]
        seasonal[i] = sum(v - m for v, m in zip(vals, cycle_means)) / n_cycles

    # Normalize so seasonal components sum to zero
    mean_s = sum(seasonal) / period
    return [s - mean_s for s in seasonal]


def holt_winters_forecast(
    data: list[float],
    period: int = 12,
    horizon: int = 12,
    alpha: float = 0.3,
    beta: float = 0.05,
    gamma: float = 0.3,
) -> dict:
    """
    Additive Holt-Winters exponential smoothing.
    Returns {fitted, forecast, rmse, params}.
    """
    n = len(data)
    if n < 2 * period:
        raise ValueError(f"Need at least {2*period} observations, got {n}")

    # Initialize level and trend from first cycle
    level = sum(data[:period]) / period
    trend = (sum(data[period:2*period]) - sum(data[:period])) / (period * period)
    seasonal = _init_seasonal(data, period)

    fitted = [0.0] * n
    errors = []

    for t in range(n):
        si = t % period
        fitted_val = level + trend + seasonal[si]
        fitted[t] = fitted_val
        error = data[t] - fitted_val
        errors.append(error)

        # Update
        new_level = alpha * (data[t] - seasonal[si]) + (1 - alpha) * (level + trend)
        new_trend = beta * (new_level - level) + (1 - beta) * trend
        seasonal[si] = gamma * (data[t] - new_level) + (1 - gamma) * seasonal[si]
        level = new_level
        trend = new_trend

    # RMSE for prediction intervals
    rmse = math.sqrt(sum(e * e for e in errors) / len(errors))

    # Forecast
    forecasts = []
    for h in range(1, horizon + 1):
        si = (n + h - 1) % period  # seasonal index wraps
        point = level + h * trend + seasonal[si]
        # Prediction interval widens with sqrt(h)
        margin = 1.28 * rmse * math.sqrt(h)  # ~80% CI
        forecasts.append({
            "point": round(max(point, 0), 2),
            "lower": round(max(point - margin, 0), 2),
            "upper": round(point + margin, 2),
        })

    return {
        "fitted": [round(f, 2) for f in fitted],
        "forecast": forecasts,
        "rmse": round(rmse, 2),
        "params": {"alpha": alpha, "beta": beta, "gamma": gamma},
    }


def build_forecast(history: list[dict], horizon: int = 12) -> dict:
    """Run Holt-Winters on total consumption, return forecast block."""
    totals = [rec["total"] for rec in history]

    # Grid search for best alpha/beta/gamma (simple coarse search)
    best_rmse = float("inf")
    best_params = (0.3, 0.05, 0.3)

    for a in [0.1, 0.2, 0.3, 0.4, 0.5]:
        for b in [0.01, 0.03, 0.05, 0.1]:
            for g in [0.1, 0.2, 0.3, 0.4, 0.5]:
                try:
                    result = holt_winters_forecast(totals, alpha=a, beta=b, gamma=g, horizon=1)
                    if result["rmse"] < best_rmse:
                        best_rmse = result["rmse"]
                        best_params = (a, b, g)
                except ValueError:
                    pass

    print(f"  Best params: alpha={best_params[0]}, beta={best_params[1]}, gamma={best_params[2]}, RMSE={best_rmse:.2f} ML")

    # Run with best params
    result = holt_winters_forecast(
        totals, alpha=best_params[0], beta=best_params[1], gamma=best_params[2], horizon=horizon,
    )

    # Build forecast dates continuing from last history date
    last_date = history[-1]["date"]
    last_year, last_month = int(last_date[:4]), int(last_date[5:7])

    forecast_points = []
    for i, fc in enumerate(result["forecast"]):
        m = last_month + i + 1
        y = last_year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        forecast_points.append({
            "date": f"{y}-{m:02d}",
            "total": fc["point"],
            "lower": fc["lower"],
            "upper": fc["upper"],
        })

    return {
        "method": "Holt-Winters additive seasonal",
        "horizon": horizon,
        "rmse_ml": result["rmse"],
        "params": result["params"],
        "points": forecast_points,
    }


def build_annual_summary(history: list[dict]) -> list[dict]:
    """Aggregate monthly data into annual totals."""
    years: dict[int, float] = {}
    for rec in history:
        y = int(rec["date"][:4])
        years[y] = years.get(y, 0) + rec["total"]
    return [{"year": y, "total": round(t, 2)} for y, t in sorted(years.items())]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    now_utc = datetime.now(timezone.utc)
    print(f"[{now_utc.isoformat()}] Fetching NORA oil consumption data...")

    # Download XLS
    print("  Downloading XLS from NORA...")
    try:
        xls_path = download_xls(NORA_XLS_URL)
    except Exception as e:
        print(f"  FATAL: download failed — {e}", file=sys.stderr)
        sys.exit(1)

    # Parse
    print("  Parsing workbook...")
    try:
        history = parse_xls(xls_path)
    finally:
        os.unlink(xls_path)

    if not history:
        print("  FATAL: no data parsed from XLS", file=sys.stderr)
        sys.exit(1)

    print(f"  Parsed {len(history)} monthly records ({history[0]['date']} to {history[-1]['date']})")

    # Forecast
    print("  Running Holt-Winters forecast...")
    try:
        forecast = build_forecast(history, horizon=12)
        print(f"  Forecast: {forecast['points'][0]['date']} to {forecast['points'][-1]['date']}")
    except Exception as e:
        print(f"  WARNING: forecast failed — {e}", file=sys.stderr)
        forecast = None

    # Annual summary
    annual = build_annual_summary(history)

    # Write output
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    output = {
        "last_updated": now_utc.isoformat(),
        "source": "NORA — nora.ie",
        "unit": "megalitres",
        "categories": ["gasoline", "kerosene", "gasoil", "diesel", "fuel_oil", "total"],
        "history": history,
        "forecast": forecast,
        "annual_summary": annual,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
