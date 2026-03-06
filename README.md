# Oil & Gas Price Dashboard

Live energy price dashboard tracking WTI Crude, Brent Crude, Natural Gas (Henry Hub), RBOB Gasoline, and Heating Oil.

**Live site:** https://skinsella.github.io/oil-gas-dashboard

## Data sources

| Layer | Source | Freshness |
|-------|--------|-----------|
| Live streaming charts | [TradingView](https://www.tradingview.com/widget/) (free widgets) | Real-time during market hours |
| Daily official spot prices | [EIA Open Data API v2](https://www.eia.gov/opendata/) | Next business day |
| Automated updates | GitHub Actions (this repo) | Twice daily, weekdays |

## One-time setup

### 1. Get a free EIA API key (1 minute)

Register at **https://www.eia.gov/opendata/** — click *Register* and confirm your email. You'll receive your key immediately.

### 2. Add the key as a GitHub Secret

In this repository:  
**Settings → Secrets and variables → Actions → New repository secret**

| Name | Value |
|------|-------|
| `EIA_API_KEY` | your EIA API key |

### 3. Enable GitHub Pages

**Settings → Pages → Source: Deploy from a branch → Branch: `main`, folder: `/` (root)**

Your dashboard will be live at `https://skinsella.github.io/oil-gas-dashboard` within a few minutes.

### 4. Trigger the first data fetch

**Actions → Update Oil & Gas Prices → Run workflow**

Historical price data (60 days) will populate in `data/prices.json` immediately.

## Automated schedule

The GitHub Actions workflow runs automatically:
- **21:00 UTC weekdays** — after US market close (~17:00 ET)
- **00:00 UTC weekdays** — overnight refresh

You can also trigger it manually from the Actions tab at any time.

## Local development

```bash
# Install Python dependency
pip install requests

# Run fetcher (requires EIA_API_KEY in environment)
EIA_API_KEY=your_key_here python scripts/fetch_prices.py

# Serve dashboard locally
python -m http.server 8080
# Then open http://localhost:8080
```

## Disclaimer

Not financial advice. For informational purposes only.
