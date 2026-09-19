"""Live backend for intraday-momentum-scanner.html.

Run this file with Kite credentials in KITE_API_KEY and KITE_ACCESS_TOKEN.
The browser never receives either credential; it only calls /api/scan.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from collections import deque
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from flask import Flask, jsonify, request, send_file
from kiteconnect import KiteConnect, KiteTicker

from sector_definitions import ALL_SYMBOLS, SECTOR_DEFINITIONS


BASE_DIR = Path(__file__).resolve().parent
IST = ZoneInfo("Asia/Kolkata")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("momentum-scanner")


def clean_env(value: str) -> str:
    return (value or "").strip().strip('"').strip("'")


API_KEY = clean_env(os.getenv("KITE_API_KEY", ""))
ACCESS_TOKEN = clean_env(os.getenv("KITE_ACCESS_TOKEN", ""))
HISTORY_SLEEP_SEC = float(os.getenv("HISTORY_SLEEP_SEC", "0.35"))
SEED_DAYS_5M = int(os.getenv("SEED_DAYS_5M", "15"))
SEED_DAYS_DAILY = int(os.getenv("SEED_DAYS_DAILY", "240"))
TICK_STALE_SEC = int(os.getenv("TICK_STALE_SEC", "20"))
PORT = int(os.getenv("PORT", "8050"))

app = Flask(__name__)
kite: Optional[KiteConnect] = None
SYMBOL_TO_TOKEN: Dict[str, int] = {}
TOKEN_TO_SYMBOL: Dict[int, str] = {}
TICK_STATE: Dict[int, Dict[str, Any]] = {}
PRICE_HISTORY: Dict[int, deque] = {}
HISTORY: Dict[int, Dict[str, pd.DataFrame]] = {}
DATA_LOCK = threading.RLock()
LAST_TICK_TS = 0.0
TOTAL_TICKS = 0
TICKER_CONNECTED = False
TICKER_STARTED = False
SEED_STARTED = False
LIVE_INITIALIZED = False
SEED_PROGRESS = {"done": 0, "total": 0, "errors": 0}


def market_is_open(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return dtime(9, 15) <= now.time() <= dtime(15, 30)


def _as_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _to_ist_series(values: pd.Series) -> pd.Series:
    dates = pd.to_datetime(values, errors="coerce")
    if dates.dt.tz is None:
        return dates.dt.tz_localize(IST)
    return dates.dt.tz_convert(IST)


def _normalize_history(candles: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(candles)
    if frame.empty:
        return frame
    required = {"date", "open", "high", "low", "close", "volume"}
    if not required.issubset(frame.columns):
        return pd.DataFrame()
    frame["date"] = _to_ist_series(frame["date"])
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)


def load_instruments() -> None:
    global kite
    if not API_KEY or not ACCESS_TOKEN:
        log.warning("Kite credentials are missing; the page will remain in demo mode.")
        return
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)
    frame = pd.DataFrame(kite.instruments("NSE"))
    if frame.empty or "tradingsymbol" not in frame.columns:
        raise RuntimeError("Kite returned no NSE instruments")
    frame = frame[frame["tradingsymbol"].isin(ALL_SYMBOLS)].copy()
    with DATA_LOCK:
        SYMBOL_TO_TOKEN.update({row.tradingsymbol: int(row.instrument_token) for row in frame.itertuples()})
        TOKEN_TO_SYMBOL.update({int(row.instrument_token): row.tradingsymbol for row in frame.itertuples()})
    missing = sorted(set(ALL_SYMBOLS) - set(SYMBOL_TO_TOKEN))
    log.info("Loaded %s/%s NSE symbols", len(SYMBOL_TO_TOKEN), len(ALL_SYMBOLS))
    if missing:
        log.warning("Symbols not available in NSE instrument list: %s", ", ".join(missing))


def _update_tick(tick: dict) -> None:
    token = tick.get("instrument_token")
    ltp = _as_float(tick.get("last_price"))
    if token is None or ltp is None or ltp <= 0:
        return
    token = int(token)
    ohlc = tick.get("ohlc") or {}
    volume = _as_float(tick.get("volume_traded"))
    timestamp = time.time()
    TICK_STATE[token] = {"ltp": ltp, "volume": volume, "ohlc": ohlc, "ts": timestamp}
    history = PRICE_HISTORY.setdefault(token, deque(maxlen=120))
    history.append((timestamp, ltp))


def _start_ticker() -> None:
    global TICKER_STARTED, TICKER_CONNECTED, LAST_TICK_TS
    if TICKER_STARTED or kite is None or not SYMBOL_TO_TOKEN:
        return
    TICKER_STARTED = True
    tokens = sorted(TOKEN_TO_SYMBOL)

    def run() -> None:
        global TICKER_CONNECTED, LAST_TICK_TS
        while True:
            try:
                ticker = KiteTicker(API_KEY, ACCESS_TOKEN)

                def on_connect(ws, _response):
                    global TICKER_CONNECTED
                    ws.subscribe(tokens)
                    ws.set_mode(ws.MODE_FULL, tokens)
                    TICKER_CONNECTED = True
                    log.info("KiteTicker connected and subscribed to %s tokens", len(tokens))

                def on_ticks(_ws, ticks):
                    global LAST_TICK_TS, TOTAL_TICKS
                    with DATA_LOCK:
                        for tick in ticks:
                            _update_tick(tick)
                        if ticks:
                            TOTAL_TICKS += len(ticks)
                            LAST_TICK_TS = time.time()

                def on_close(_ws, _code, _reason):
                    global TICKER_CONNECTED
                    TICKER_CONNECTED = False
                    log.warning("KiteTicker connection closed")

                ticker.on_connect = on_connect
                ticker.on_ticks = on_ticks
                ticker.on_close = on_close
                ticker.connect(threaded=False)
            except Exception:
                TICKER_CONNECTED = False
                log.exception("KiteTicker stopped; retrying in 5 seconds")
                time.sleep(5)


    threading.Thread(target=run, name="kite-ticker", daemon=True).start()


def _seed_symbol(symbol: str, token: int) -> None:
    if kite is None:
        return
    now = datetime.now(IST)
    try:
        five = kite.historical_data(
            instrument_token=token,
            from_date=now - timedelta(days=SEED_DAYS_5M),
            to_date=now,
            interval="5minute",
            continuous=False,
            oi=False,
        )
        time.sleep(HISTORY_SLEEP_SEC)
        daily = kite.historical_data(
            instrument_token=token,
            from_date=now - timedelta(days=SEED_DAYS_DAILY),
            to_date=now,
            interval="day",
            continuous=False,
            oi=False,
        )
        with DATA_LOCK:
            HISTORY[token] = {"intraday": _normalize_history(five), "regular": _normalize_history(daily)}
    finally:
        time.sleep(HISTORY_SLEEP_SEC)


def _start_history_seed() -> None:
    global SEED_STARTED
    if SEED_STARTED or kite is None:
        return
    SEED_STARTED = True
    tokens = sorted(TOKEN_TO_SYMBOL.items())
    SEED_PROGRESS["total"] = len(tokens)

    def run() -> None:
        for token, symbol in tokens:
            try:
                _seed_symbol(symbol, token)
            except Exception:
                SEED_PROGRESS["errors"] += 1
                log.exception("History seed failed for %s", symbol)
            finally:
                SEED_PROGRESS["done"] += 1

    threading.Thread(target=run, name="history-seed", daemon=True).start()


def _ema(values: pd.Series, period: int = 21) -> Optional[float]:
    if values.empty:
        return None
    return _as_float(values.ewm(span=period, adjust=False, min_periods=1).mean().iloc[-1])


def _rsi(values: pd.Series, period: int = 14) -> Optional[float]:
    if len(values) < 3:
        return None
    delta = values.diff()
    gains = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False, min_periods=1).mean()
    losses = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False, min_periods=1).mean()
    last_loss = float(losses.iloc[-1])
    if last_loss <= 1e-12:
        return 100.0 if float(gains.iloc[-1]) > 0 else 50.0
    result = 100.0 - (100.0 / (1.0 + float(gains.iloc[-1]) / last_loss))
    return max(0.0, min(100.0, result))


def _adx(frame: pd.DataFrame, period: int = 14) -> Optional[float]:
    if len(frame) < 4:
        return None
    high, low, close = frame["high"], frame["low"], frame["close"]
    previous_close = close.shift(1)
    true_range = pd.concat([high - low, (high - previous_close).abs(), (low - previous_close).abs()], axis=1).max(axis=1)
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    atr = true_range.ewm(alpha=1 / period, adjust=False, min_periods=1).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=1).mean() / (atr + 1e-9)
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=1).mean() / (atr + 1e-9)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-9)
    return max(0.0, min(100.0, float(dx.ewm(alpha=1 / period, adjust=False, min_periods=1).mean().iloc[-1])))


def _history_state(token: int, timeframe: str) -> Optional[tuple[pd.DataFrame, float, float, dict]]:
    with DATA_LOCK:
        frame = HISTORY.get(token, {}).get(timeframe)
        tick = dict(TICK_STATE.get(token) or {})
    if frame is None or frame.empty:
        return None
    last = frame.iloc[-1]
    ltp = _as_float(tick.get("ltp")) or _as_float(last.get("close"))
    volume = _as_float(tick.get("volume")) or _as_float(last.get("volume")) or 0.0
    ohlc = tick.get("ohlc") if isinstance(tick.get("ohlc"), dict) else {}
    if not ohlc:
        ohlc = {"open": last.get("open"), "high": last.get("high"), "low": last.get("low"), "close": last.get("close")}
    if ltp is None:
        return None
    return frame.copy(), float(ltp), float(volume), ohlc


def _volume_ratio(token: int, timeframe: str, volume: float, now: datetime) -> float:
    with DATA_LOCK:
        daily = HISTORY.get(token, {}).get("regular")
    if daily is None or daily.empty:
        return 1.0
    daily = daily[daily["volume"] > 0]
    if daily.empty:
        return 1.0
    if timeframe == "regular":
        baseline = float(daily["volume"].tail(61).head(60).mean())
        return max(0.0, volume / (baseline + 1e-9))
    baseline = float(daily["volume"].tail(20).mean())
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    elapsed = (now - market_open).total_seconds() / 60.0
    if now <= market_open:
        elapsed = 1.0
    else:
        elapsed = max(1.0, min(375.0, elapsed))
    if now >= market_close:
        elapsed = 375.0
    expected = baseline * (elapsed / 375.0)
    return max(0.0, volume / (expected + 1e-9))


def _sparkline(prices: List[float], positive: bool) -> str:
    values = prices[-5:]
    if len(values) < 2:
        return ""
    low, high = min(values), max(values)
    spread = max(high - low, 1e-9)
    coords = " ".join(f"{i * 18},{24 - ((value - low) / spread) * 19:.1f}" for i, value in enumerate(values))
    color = "#0d9b73" if positive else "#d44e57"
    return f'<svg class="spark" viewBox="0 0 72 26" aria-label="Five candle trend"><polyline stroke="{color}" points="{coords}"></polyline></svg>'


def _rfactor(token: int, timeframe: str, ltp: float, volume: float, high: float, low: float, change: float) -> float:
    """Compact live equivalent of the supplied dashboard RFactor formula."""
    with DATA_LOCK:
        daily = HISTORY.get(token, {}).get("regular")
    if daily is None or daily.empty:
        return 0.0
    stats = daily[daily["volume"] > 0].copy()
    if market_is_open() and not stats.empty and stats.iloc[-1]["date"].date() == datetime.now(IST).date():
        stats = stats.iloc[:-1]
    stats = stats.tail(20)
    if stats.empty:
        return 0.0
    avg_vol = float(stats["volume"].mean())
    avg_range = float((stats["high"] - stats["low"]).mean())
    avg_move = float((((stats["close"] - stats["open"]) / stats["open"]) * 100.0).abs().mean())
    if min(avg_vol, avg_range, avg_move) <= 0:
        return 0.0
    now = datetime.now(IST)
    if timeframe == "intraday":
        opened = now.replace(hour=9, minute=15, second=0, microsecond=0)
        elapsed = 1.0 if now <= opened else max(1.0, min(375.0, (now - opened).total_seconds() / 60.0))
        expected_volume = avg_vol * (elapsed / 375.0)
    else:
        expected_volume = avg_vol
    rvol = max(volume / (expected_volume + 1e-9), 0.001)
    range_factor = max((high - low) / (avg_range + 1e-9), 0.001)
    move_factor = max(abs(change) / (avg_move + 1e-9), 0.001)
    raw = (rvol ** 0.55) * (range_factor ** 0.30) * (move_factor ** 0.15)
    span = max(high - low, 1e-9)
    position = max(0.0, min(1.0, (ltp - low) / span))
    freshness = position ** 3 if change >= 0 else (1.0 - position) ** 3
    if (high - low) / max(ltp, 1e-9) * 100.0 < 0.60:
        raw *= 0.12
    raw *= max(freshness, 0.001)
    return round(3.5 * math.log1p(max(raw, 0.0)), 2)


def _build_row(symbol: str, sector: str, timeframe: str) -> Optional[dict]:
    token = SYMBOL_TO_TOKEN.get(symbol)
    if not token:
        return None
    state = _history_state(token, timeframe)
    if state is None:
        return None
    frame, ltp, volume, ohlc = state
    open_price = _as_float(ohlc.get("open")) or _as_float(frame.iloc[-1]["open"])
    day_high = _as_float(ohlc.get("high")) or _as_float(frame.iloc[-1]["high"])
    day_low = _as_float(ohlc.get("low")) or _as_float(frame.iloc[-1]["low"])
    if not open_price or not day_high or not day_low:
        return None

    if timeframe == "regular" and len(frame) >= 2:
        previous_close = _as_float(frame.iloc[-2]["close"]) or open_price
        change = (ltp - previous_close) / (previous_close + 1e-9) * 100.0
    else:
        change = (ltp - open_price) / (open_price + 1e-9) * 100.0

    closes = pd.to_numeric(frame["close"], errors="coerce").dropna()
    close_values = closes.tolist() + [ltp]
    indicator_closes = pd.Series(close_values, dtype="float64")
    indicator_frame = frame[["high", "low", "close"]].copy()
    live_row = indicator_frame.iloc[-1].copy()
    live_row["high"] = max(float(live_row["high"]), ltp)
    live_row["low"] = min(float(live_row["low"]), ltp)
    live_row["close"] = ltp
    indicator_frame = pd.concat([indicator_frame.iloc[:-1], pd.DataFrame([live_row])], ignore_index=True)
    rsi = _rsi(indicator_closes)
    adx = _adx(indicator_frame)
    ema = _ema(indicator_closes)
    if rsi is None or adx is None or ema is None or ema <= 0:
        return None

    ratio = _volume_ratio(token, timeframe, volume, datetime.now(IST))
    ema_gap = (ltp - ema) / ema * 100.0
    score = abs(change) * 1.8 + ratio * 1.6 + max(0.0, adx - 18.0) / 12.0 + abs(ema_gap) * 0.65 + abs(rsi - 50.0) / 24.0
    rfactor = _rfactor(token, timeframe, ltp, volume, day_high, day_low, change)
    volatility = "high" if abs(change) >= 2.5 or abs(ema_gap) >= 2.5 else "medium" if abs(change) >= 1.0 or abs(ema_gap) >= 1.2 else "low"
    positive = change >= 0
    return {
        "symbol": symbol,
        "display": symbol,
        "sector": sector,
        "direction": 1 if positive else -1,
        "change": round(change, 2),
        "volume": round((ratio - 1.0) * 100.0, 2),
        "ratio": round(ratio, 2),
        "rsi": round(rsi, 1),
        "adx": round(adx, 1),
        "ema": round(ema_gap, 2),
        "score": round(score, 4),
        "rfactor": rfactor,
        "volatility": volatility,
        "rank": 0,
        "spark": _sparkline(close_values, positive),
        "isIndex": False,
    }


INDEX_GROUPS = {
    "NIFTY 50": SECTOR_DEFINITIONS["NIFTY_50"],
    "BANK NIFTY": SECTOR_DEFINITIONS["BANK"] + SECTOR_DEFINITIONS["PSUBANK"],
    "NIFTY IT": SECTOR_DEFINITIONS["IT"],
    "NIFTY AUTO": SECTOR_DEFINITIONS["AUTO"],
    "NIFTY METAL": SECTOR_DEFINITIONS["METAL"],
    "NIFTY PHARMA": SECTOR_DEFINITIONS["PHARMA"],
    "NIFTY ENERGY": SECTOR_DEFINITIONS["ENERGY"],
    "NIFTY REALTY": SECTOR_DEFINITIONS["REALTY"],
}


def _aggregate_index(name: str, rows: List[dict]) -> Optional[dict]:
    if not rows:
        return None
    weights = [max(float(row.get("ratio") or 1.0), 0.1) for row in rows]
    total_weight = sum(weights)

    def average(key: str) -> float:
        return sum(float(row.get(key) or 0.0) * weight for row, weight in zip(rows, weights)) / total_weight

    change = average("change")
    ratio = average("ratio")
    ema = average("ema")
    score = average("score")
    strongest = max(rows, key=lambda row: float(row.get("score") or 0.0))
    volatility = "high" if abs(change) >= 1.8 else "medium" if abs(change) >= 0.8 else "low"
    return {
        "symbol": name,
        "display": name,
        "sector": "INDEX",
        "direction": 1 if change >= 0 else -1,
        "change": round(change, 2),
        "volume": round((ratio - 1.0) * 100.0, 2),
        "ratio": round(ratio, 2),
        "rsi": round(average("rsi"), 1),
        "adx": round(average("adx"), 1),
        "ema": round(ema, 2),
        "score": round(score, 4),
        "rfactor": round(average("rfactor"), 2),
        "volatility": volatility,
        "rank": 0,
        "spark": strongest.get("spark", ""),
        "isIndex": True,
    }


def _rank(rows: List[dict]) -> List[dict]:
    rows.sort(key=lambda row: float(row.get("score") or 0.0), reverse=True)
    for position, row in enumerate(rows, start=1):
        row["rank"] = position
        row.pop("score", None)
    return rows


def build_rows(timeframe: str, universe: str, sector: str) -> List[dict]:
    if universe == "index":
        rows = []
        for name, symbols in INDEX_GROUPS.items():
            children = [_build_row(symbol, "INDEX", timeframe) for symbol in dict.fromkeys(symbols)]
            aggregate = _aggregate_index(name, [row for row in children if row])
            if aggregate:
                rows.append(aggregate)
        return _rank(rows)

    membership: Dict[str, str] = {}
    for group, symbols in SECTOR_DEFINITIONS.items():
        for symbol in symbols:
            membership.setdefault(symbol, group)
    symbols = list(dict.fromkeys(SECTOR_DEFINITIONS.get(sector, []))) if sector != "all" else list(membership)
    rows = [_build_row(symbol, membership.get(symbol, sector), timeframe) for symbol in symbols]
    return _rank([row for row in rows if row])


def _feed_status() -> tuple[str, bool]:
    if kite is None or not SYMBOL_TO_TOKEN:
        return "missing_credentials", False
    if not market_is_open():
        return "market_closed", False
    fresh = LAST_TICK_TS and (time.time() - LAST_TICK_TS) <= TICK_STALE_SEC
    if TICKER_CONNECTED and fresh:
        return "live", True
    if SEED_PROGRESS["done"] < SEED_PROGRESS["total"]:
        return "seeding", False
    return "waiting_for_ticks", False


@app.get("/")
def index():
    return send_file(BASE_DIR / "intraday-momentum-scanner.html")


@app.get("/api/health")
def health():
    status, live = _feed_status()
    return jsonify({
        "status": status,
        "live": live,
        "market_open": market_is_open(),
        "seed": dict(SEED_PROGRESS),
        "symbols": len(SYMBOL_TO_TOKEN),
        "ticks": TOTAL_TICKS,
        "last_tick": datetime.fromtimestamp(LAST_TICK_TS, IST).isoformat() if LAST_TICK_TS else None,
    })


@app.get("/api/scan")
def scan():
    timeframe = request.args.get("type", "intraday").lower()
    universe = request.args.get("universe", "stocks").lower()
    sector = request.args.get("sector", "all").upper()
    if timeframe not in {"intraday", "regular"}:
        return jsonify({"error": "type must be intraday or regular"}), 400
    if universe not in {"stocks", "index"}:
        return jsonify({"error": "universe must be stocks or index"}), 400
    if sector not in SECTOR_DEFINITIONS and sector != "ALL":
        sector = "all"
    rows = build_rows(timeframe, universe, sector.lower())
    status, live = _feed_status()
    return jsonify({
        "live": live,
        "status": status,
        "market_open": market_is_open(),
        "updated_at": datetime.now(IST).strftime("%H:%M:%S IST"),
        "seed": dict(SEED_PROGRESS),
        "ticks": TOTAL_TICKS,
        "rows": rows,
    })


def initialize_live() -> None:
    """Start live services once for both Python and Gunicorn entrypoints."""
    global LIVE_INITIALIZED
    if LIVE_INITIALIZED:
        return
    LIVE_INITIALIZED = True
    try:
        load_instruments()
        _start_history_seed()
        _start_ticker()
    except Exception:
        log.exception("Live market startup failed; serving demo UI")


def start() -> None:
    initialize_live()
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)


initialize_live()

if __name__ == "__main__":
    start()
