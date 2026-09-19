# Live Momentum Scanner

This folder now contains a browser UI plus a small Kite-backed server. The server keeps Kite credentials on the backend and exposes only calculated scanner rows to the page.

## Start locally

From this folder:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-live.txt
export KITE_API_KEY="your_kite_api_key"
export KITE_ACCESS_TOKEN="your_daily_access_token"
python live_scanner_server.py
```

Open `http://127.0.0.1:8050/` in a browser. Do not open the HTML file directly if you want live values; the page must be served by `live_scanner_server.py` so it can call `/api/scan`.

## What the server does

- Loads the supplied NSE sector universe from `sector_definitions.py`.
- Uses Kite historical candles to seed 5-minute and daily indicators.
- Uses KiteTicker full-mode ticks for current price, volume, and five-point sparklines.
- Calculates RSI, ADX, 21 EMA distance, time-adjusted volume ratio, volatility bucket, and momentum rank.
- Calculates and exposes a live RFactor value; click any Sector flow bar to see its stocks sorted by RFactor.
- Shows the cumulative KiteTicker tick count beside the feed status.
- Serves `/api/scan` for the page and `/api/health` for feed status.

The initial historical seed is deliberately paced and can take a few minutes for the full universe. Until enough history is available, the page remains in demo mode. Kite access tokens normally expire daily, so provide a fresh token before starting the server.

For a single-process production deployment, use one worker because the process owns one KiteTicker connection:

```bash
gunicorn --workers 1 --bind 0.0.0.0:8050 live_scanner_server:app
```

This is research context only. It is not an order-entry system or a trading signal.
