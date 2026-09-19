# Live Momentum Scanner

This folder now contains a browser UI plus a small Kite-backed server. The server keeps Kite credentials on the backend and exposes only calculated scanner rows to the page.

## Start locally

From this folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-live.txt
export KITE_API_KEY="your_kite_api_key"
export KITE_ACCESS_TOKEN="your_daily_access_token"
python3 live_scanner_server.py
```

Open `http://127.0.0.1:8050/` in a browser. Do not open the HTML file directly if you want live values; the page must be served by `live_scanner_server.py` so it can call `/api/scan`.

## What the server does

- Loads the supplied NSE sector universe from `sector_definitions.py`.
- By default, watches and calculates detailed indicators for every stock in the configured NSE universe.
- Sector flow is calculated separately from quote ticks across the whole NSE universe.
- Uses Kite historical candles to seed 5-minute and daily indicators.
- Uses KiteTicker full-mode ticks for current price, volume, and five-point sparklines.
- Calculates RSI, ADX, 21 EMA distance, time-adjusted volume ratio, recent-bar continuation, session trend quality, volume confirmation, RFactor, volatility bucket, and momentum rank. RFactor and directional continuation now directly affect rank, so an early spike loses rank when the stock goes sideways instead of continuing. The same logic supports persistent upside and downside trends.
- Calculates and exposes a live RFactor value; click any Sector flow bar to see its stocks sorted by RFactor.
- Shows the cumulative KiteTicker tick count beside the feed status.
- Serves `/api/scan` for the page and `/api/health` for feed status.
- Starts market-data initialization in the background under Uvicorn, so the dashboard opens while history is still seeding.
- Recomputes detailed rows and whole-universe Sector flow in a background cache; `/api/scan` only reads that cache, so browser polling does not rerun indicators or RFactor calculations.
- Detects the next market day, clears prior-session ticks/history/cache, and reseeds fresh Kite history automatically without requiring a Render restart.
- Outside market hours, the page labels loaded data as `Previous session` until the next session begins.

Full-universe mode is enabled by default. Set `FAST_MODE=true` to optionally watch all stocks with lightweight quote ticks while limiting detailed history/order-book work to `FAST_SYMBOL_LIMIT` stocks. `FAST_SELECTION_WAIT_SEC` controls how long startup waits for live quotes before selecting the Fast mode list, and `FAST_RESELECT_SEC` controls how often that list rotates (default: 300 seconds).
`SCAN_COMPUTE_EVERY_SEC` controls the background cache refresh interval (default: 3 seconds).

The initial historical seed is deliberately paced and can take a few minutes for the full universe. Until enough history is available, the page remains in demo mode. Kite access tokens normally expire daily, so provide a fresh token before starting the server.

For a single-process production deployment, use one worker because the process owns one KiteTicker connection:

```bash
python3 -m uvicorn app:app --host 0.0.0.0 --port 8050 --workers 1
```

## Deploy on Render

1. Put the files in this folder in a GitHub repository.
2. In Render, create a new Web Service from that repository.
3. Set the Root Directory to `outputs` if the folder is inside a larger repository.
4. Use Build Command: `python3 -m pip install -r requirements.txt`.
5. Use Start Command: `python3 -m uvicorn app:app --host 0.0.0.0 --port $PORT --workers 1`.
6. Add `KITE_API_KEY` and `KITE_ACCESS_TOKEN` as secret environment variables.
7. Deploy and open the Render URL. The health check is `/api/health`.

`render.yaml` contains the same setup. Use an always-on instance for dependable market-hours streaming; sleeping instances can miss ticks. Kite access tokens usually expire daily, so update `KITE_ACCESS_TOKEN` in Render before the next session.

This is research context only. It is not an order-entry system or a trading signal.
