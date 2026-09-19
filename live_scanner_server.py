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
from datetime import date, datetime, time as dtime, timedelta
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
FAST_MODE = os.getenv("FAST_MODE", "false").strip().lower() not in {"0", "false", "no", "off"}
FAST_SYMBOL_LIMIT = int(os.getenv("FAST_SYMBOL_LIMIT", "25"))
FAST_SELECTION_WAIT_SEC = int(os.getenv("FAST_SELECTION_WAIT_SEC", "20"))
FAST_RESELECT_SEC = int(os.getenv("FAST_RESELECT_SEC", "300"))
SCAN_COMPUTE_EVERY_SEC = float(os.getenv("SCAN_COMPUTE_EVERY_SEC", "3"))

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
TICKER_WS: Any = None
SEED_STARTED = False
LIVE_INITIALIZED = False
LIVE_INIT_LOCK = threading.Lock()
SEED_PROGRESS = {"done": 0, "total": 0, "errors": 0}
DETAIL_SYMBOLS: set[str] = set()
SEED_IN_PROGRESS = False
FAST_ROTATION_STARTED = False
HISTORY_SEED_DATE: Optional[date] = None
DAILY_REFRESH_STARTED = False
DAILY_REFRESH_START_LOCK = threading.Lock()
SCAN_CACHE: Dict[str, Dict[str, List[dict]]] = {
    "intraday": {"stocks": [], "index": []},
    "regular": {"stocks": [], "index": []},
}
SECTOR_FLOW_CACHE: List[dict] = []
SCAN_CACHE_UPDATED_AT = 0.0
SCAN_COMPUTE_STARTED = False
SCAN_CACHE_LOCK = threading.RLock()
SCAN_COMPUTE_START_LOCK = threading.Lock()


def market_is_open(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return dtime(9, 15) <= now.time() <= dtime(15, 30)


def _has_current_session_data(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    if LAST_TICK_TS and datetime.fromtimestamp(LAST_TICK_TS, IST).date() == now.date():
        return True
    with DATA_LOCK:
        for histories in HISTORY.values():
            for frame in histories.values():
                if not frame.empty and frame.iloc[-1]["date"].date() == now.date():
                    return True
    return False


def _as_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _order_book_delta(depth: dict) -> Optional[float]:
    """Return resting bid-vs-ask quantity imbalance from Kite full-mode depth."""
    buy = sum(_as_float(level.get("quantity")) or 0.0 for level in depth.get("buy", [])[:5])
    sell = sum(_as_float(level.get("quantity")) or 0.0 for level in depth.get("sell", [])[:5])
    total = buy + sell
    if total <= 0:
        return None
    return (buy - sell) / total


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
    flow_delta = _order_book_delta(tick.get("depth") or {})
    volume = _as_float(tick.get("volume_traded"))
    if volume is None:
        volume = _as_float((TICK_STATE.get(token) or {}).get("volume")) or 0.0
    timestamp = time.time()
    TICK_STATE[token] = {"ltp": ltp, "volume": volume, "ohlc": ohlc, "flow_delta": flow_delta, "ts": timestamp}
    history = PRICE_HISTORY.setdefault(token, deque(maxlen=3600))
    history.append((timestamp, ltp, volume, flow_delta))


def _set_ticker_modes(selected_symbols: set[str]) -> None:
    with DATA_LOCK:
        ticker_ws = TICKER_WS
        all_tokens = sorted(TOKEN_TO_SYMBOL)
        selected_tokens = [SYMBOL_TO_TOKEN[symbol] for symbol in selected_symbols if symbol in SYMBOL_TO_TOKEN]
    if not ticker_ws:
        return
    try:
        ticker_ws.set_mode(ticker_ws.MODE_QUOTE, all_tokens)
        if selected_tokens:
            ticker_ws.set_mode(ticker_ws.MODE_FULL, selected_tokens)
    except Exception:
        log.exception("Unable to update Fast mode ticker subscriptions")


def _start_ticker() -> None:
    global TICKER_STARTED, TICKER_CONNECTED, TICKER_WS, LAST_TICK_TS
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
                    global TICKER_CONNECTED, TICKER_WS
                    TICKER_WS = ws
                    ws.subscribe(tokens)
                    ws.set_mode(ws.MODE_QUOTE, tokens)
                    with DATA_LOCK:
                        selected_tokens = [SYMBOL_TO_TOKEN[symbol] for symbol in DETAIL_SYMBOLS if symbol in SYMBOL_TO_TOKEN]
                    if selected_tokens:
                        ws.set_mode(ws.MODE_FULL, selected_tokens)
                    TICKER_CONNECTED = True
                    log.info("KiteTicker connected: %s quote tokens, %s full-depth tokens", len(tokens), len(selected_tokens))

                def on_ticks(_ws, ticks):
                    global LAST_TICK_TS, TOTAL_TICKS
                    with DATA_LOCK:
                        for tick in ticks:
                            _update_tick(tick)
                        if ticks:
                            TOTAL_TICKS += len(ticks)
                            LAST_TICK_TS = time.time()

                def on_close(_ws, _code, _reason):
                    global TICKER_CONNECTED, TICKER_WS
                    TICKER_CONNECTED = False
                    TICKER_WS = None
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


def _fast_symbol_candidates(include_fallback: bool = True) -> list[str]:
    candidates = []
    with DATA_LOCK:
        for symbol, token in SYMBOL_TO_TOKEN.items():
            tick = TICK_STATE.get(token) or {}
            ltp = _as_float(tick.get("ltp"))
            ohlc = tick.get("ohlc") if isinstance(tick.get("ohlc"), dict) else {}
            open_price = _as_float(ohlc.get("open"))
            volume = _as_float(tick.get("volume")) or 0.0
            if not ltp or not open_price:
                continue
            change = abs((ltp - open_price) / open_price * 100.0)
            candidates.append((change, volume, symbol))
    candidates.sort(reverse=True)
    selected = [symbol for _, _, symbol in candidates[:FAST_SYMBOL_LIMIT]]
    if not include_fallback:
        return selected
    fallback = list(dict.fromkeys(SECTOR_DEFINITIONS.get("NIFTY_50", []) + list(ALL_SYMBOLS)))
    for symbol in fallback:
        if len(selected) >= FAST_SYMBOL_LIMIT:
            break
        if symbol in SYMBOL_TO_TOKEN and symbol not in selected:
            selected.append(symbol)
    return selected


def _select_fast_symbols() -> list[str]:
    deadline = time.time() + FAST_SELECTION_WAIT_SEC
    while time.time() < deadline:
        live_selected = _fast_symbol_candidates(include_fallback=False)
        if len(live_selected) >= FAST_SYMBOL_LIMIT or not market_is_open():
            break
        time.sleep(1)
    selected = _fast_symbol_candidates()
    with DATA_LOCK:
        DETAIL_SYMBOLS.clear()
        DETAIL_SYMBOLS.update(selected[:FAST_SYMBOL_LIMIT])
        selected_symbols = set(DETAIL_SYMBOLS)
    _set_ticker_modes(selected_symbols)
    log.info("Fast mode selected %s/%s symbols for detailed history", len(DETAIL_SYMBOLS), len(SYMBOL_TO_TOKEN))
    return list(DETAIL_SYMBOLS)


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


def _rotate_fast_symbols() -> None:
    global SEED_IN_PROGRESS
    if not FAST_MODE or not market_is_open() or not TICKER_CONNECTED:
        return
    selected = set(_fast_symbol_candidates())
    with DATA_LOCK:
        current = set(DETAIL_SYMBOLS)
    added = selected - current
    if not added:
        return
    _set_ticker_modes(selected)
    SEED_IN_PROGRESS = True
    SEED_PROGRESS.update({"done": 0, "total": len(added), "errors": 0})
    ready = set()
    try:
        for symbol in sorted(added):
            token = SYMBOL_TO_TOKEN.get(symbol)
            if not token:
                continue
            try:
                _seed_symbol(symbol, token)
                with DATA_LOCK:
                    if not HISTORY.get(token, {}).get("intraday", pd.DataFrame()).empty:
                        ready.add(symbol)
            except Exception:
                SEED_PROGRESS["errors"] += 1
                log.exception("Fast rotation history seed failed for %s", symbol)
            finally:
                SEED_PROGRESS["done"] += 1
    finally:
        with DATA_LOCK:
            DETAIL_SYMBOLS.clear()
            DETAIL_SYMBOLS.update((current & selected) | ready)
            active_symbols = set(DETAIL_SYMBOLS)
        _set_ticker_modes(active_symbols)
        SEED_IN_PROGRESS = False
    log.info("Fast mode rotated: %s detailed symbols, %s new histories", len(active_symbols), len(ready))


def _start_fast_rotation() -> None:
    global FAST_ROTATION_STARTED
    if not FAST_MODE or FAST_ROTATION_STARTED or FAST_RESELECT_SEC <= 0:
        return
    FAST_ROTATION_STARTED = True

    def run() -> None:
        while True:
            time.sleep(FAST_RESELECT_SEC)
            try:
                _rotate_fast_symbols()
            except Exception:
                log.exception("Fast mode rotation failed")

    threading.Thread(target=run, name="fast-rotation", daemon=True).start()


def _reset_for_new_market_day(today: date) -> None:
    """Drop prior-session state before reseeding the new market session."""
    global LAST_TICK_TS, TOTAL_TICKS, SCAN_CACHE_UPDATED_AT
    with DATA_LOCK:
        HISTORY.clear()
        PRICE_HISTORY.clear()
        TICK_STATE.clear()
        LAST_TICK_TS = 0.0
        TOTAL_TICKS = 0
    with SCAN_CACHE_LOCK:
        for timeframe in SCAN_CACHE:
            for universe in SCAN_CACHE[timeframe]:
                SCAN_CACHE[timeframe][universe] = []
        SECTOR_FLOW_CACHE.clear()
        SCAN_CACHE_UPDATED_AT = 0.0
    log.info("Started fresh market session for %s", today.isoformat())


def _start_daily_refresh() -> None:
    global DAILY_REFRESH_STARTED
    with DAILY_REFRESH_START_LOCK:
        if DAILY_REFRESH_STARTED:
            return
        DAILY_REFRESH_STARTED = True

    def run() -> None:
        while True:
            now = datetime.now(IST)
            seeded_date = HISTORY_SEED_DATE
            if market_is_open(now) and seeded_date is not None and seeded_date < now.date() and _has_current_session_data(now):
                _reset_for_new_market_day(now.date())
                try:
                    _start_history_seed(force=True)
                except Exception:
                    log.exception("New market-day history seed failed")
            time.sleep(30)

    threading.Thread(target=run, name="daily-refresh", daemon=True).start()


def _start_history_seed(force: bool = False) -> None:
    global SEED_IN_PROGRESS, SEED_STARTED, HISTORY_SEED_DATE
    if SEED_STARTED or kite is None:
        if not force or kite is None:
            return
    today = datetime.now(IST).date()
    if SEED_IN_PROGRESS or HISTORY_SEED_DATE == today:
        return
    SEED_STARTED = True
    HISTORY_SEED_DATE = today
    SEED_PROGRESS.update({"done": 0, "total": 0, "errors": 0})
    if FAST_MODE:
        SEED_PROGRESS["total"] = FAST_SYMBOL_LIMIT
    SEED_IN_PROGRESS = True
    if FAST_MODE:
        selected_symbols = _select_fast_symbols()
    else:
        selected_symbols = list(SYMBOL_TO_TOKEN)
        with DATA_LOCK:
            DETAIL_SYMBOLS.clear()
            DETAIL_SYMBOLS.update(selected_symbols)
    tokens = sorted((token, symbol) for symbol, token in SYMBOL_TO_TOKEN.items() if symbol in selected_symbols)
    SEED_PROGRESS["total"] = len(tokens)

    def run() -> None:
        global SEED_IN_PROGRESS
        try:
            for token, symbol in tokens:
                try:
                    _seed_symbol(symbol, token)
                except Exception:
                    SEED_PROGRESS["errors"] += 1
                    log.exception("History seed failed for %s", symbol)
                finally:
                    SEED_PROGRESS["done"] += 1
        finally:
            SEED_IN_PROGRESS = False
            _start_fast_rotation()

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
    tick_ltp = _as_float(tick.get("ltp"))
    ltp = tick_ltp or _as_float(last.get("close"))
    volume = _as_float(tick.get("volume")) or _as_float(last.get("volume")) or 0.0
    ohlc = tick.get("ohlc") if isinstance(tick.get("ohlc"), dict) else {}
    if not ohlc:
        if timeframe == "intraday":
            latest_date = last["date"].date()
            session = frame[frame["date"].dt.date == latest_date]
            ohlc = {
                "open": session.iloc[0]["open"],
                "high": session["high"].max(),
                "low": session["low"].min(),
                "close": session.iloc[-1]["close"],
            }
            if tick_ltp is None:
                volume = float(session["volume"].sum())
        else:
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
    if not market_is_open(now) or not _has_current_session_data(now):
        elapsed = 375.0
    else:
        elapsed = (now - market_open).total_seconds() / 60.0
        elapsed = max(1.0, min(375.0, elapsed))
    expected = baseline * (elapsed / 375.0)
    return max(0.0, volume / (expected + 1e-9))


def _live_candles(token: int, now: datetime) -> list[dict]:
    """Build rolling 5-minute candles from recent full-mode ticks."""
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    cutoff = max(market_open.timestamp(), now.timestamp() - 20 * 60)
    with DATA_LOCK:
        ticks = list(PRICE_HISTORY.get(token, ()))
    buckets: dict[int, dict] = {}
    for tick in ticks:
        timestamp, price, volume = tick[:3]
        flow_delta = tick[3] if len(tick) > 3 else None
        if timestamp < cutoff or timestamp < market_open.timestamp() or price <= 0:
            continue
        bucket = int((timestamp - market_open.timestamp()) // 300)
        candle = buckets.setdefault(bucket, {"open": price, "high": price, "low": price, "close": price, "last_volume": volume, "delta_sum": 0.0, "delta_count": 0})
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["last_volume"] = volume
        if flow_delta is not None:
            candle["delta_sum"] += flow_delta
            candle["delta_count"] += 1
    candles = []
    previous_volume = None
    for bucket in sorted(buckets):
        candle = buckets[bucket]
        last_volume = float(candle.get("last_volume") or 0.0)
        candle_volume = max(0.0, last_volume - previous_volume) if previous_volume is not None else 0.0
        flow = candle["delta_sum"] / candle["delta_count"] if candle["delta_count"] else None
        candles.append({**candle, "volume": candle_volume, "flow_delta": flow})
        previous_volume = last_volume
    return candles


def _session_trend_quality(
    frame: pd.DataFrame,
    ltp: float,
    change: float,
    now: datetime,
    live_candles: Optional[list[dict]] = None,
) -> float:
    """Reward a directional 5-minute path and penalize sideways candles."""
    if frame.empty:
        return 1.0
    today = frame[frame["date"].dt.date == now.date()]
    if today.empty and not live_candles:
        latest_date = frame.iloc[-1]["date"].date()
        today = frame[frame["date"].dt.date == latest_date]
    expected_direction = 1.0 if change >= 0 else -1.0
    quality = 1.0
    if not today.empty:
        values = [_as_float(today.iloc[0]["open"])] + [
            _as_float(value) for value in today["close"].tolist()
        ]
        values = [value for value in values if value is not None and value > 0]
        if len(values) >= 2:
            values.append(ltp)
            meaningful_moves = []
            for previous, current in zip(values, values[1:]):
                move = (current - previous) / previous * 100.0 * expected_direction
                meaningful_moves.append(move >= 0.02)
            if meaningful_moves:
                continuity = sum(meaningful_moves) / len(meaningful_moves)
                quality = max(0.05, continuity * continuity)
    if live_candles and len(live_candles) >= 2:
        live_moves = [
            ((candle["close"] - candle["open"]) / candle["open"] * 100.0) * expected_direction
            for candle in live_candles[-3:]
        ]
        live_continuity = sum(move >= 0.02 for move in live_moves) / len(live_moves)
        quality = min(quality, max(0.05, live_continuity * live_continuity))
    return quality


def _volume_confirmation(
    frame: pd.DataFrame,
    expected_direction: float,
    now: datetime,
    live_candles: Optional[list[dict]] = None,
) -> float:
    """Reward recent directional candles that are backed by above-baseline volume."""
    if frame.empty:
        return 1.0
    intraday = frame[frame["volume"] > 0]
    today = intraday[intraday["date"].dt.date == now.date()]
    session_date = now.date()
    if today.empty and not live_candles:
        session_date = intraday.iloc[-1]["date"].date() if not intraday.empty else now.date()
        today = intraday[intraday["date"].dt.date == session_date]
    history = intraday[intraday["date"].dt.date < session_date]
    baseline_source = history["volume"].tail(120) if not history.empty else intraday["volume"].iloc[:-3]
    baseline = float(baseline_source.median()) if not baseline_source.empty else 0.0
    if baseline <= 0:
        return 1.0
    if live_candles and any(candle["volume"] > 0 for candle in live_candles):
        recent_candles = live_candles[-3:]
        moves = [
            ((candle["close"] - candle["open"]) / candle["open"] * 100.0) * expected_direction
            for candle in recent_candles
        ]
        volumes = [float(candle["volume"]) for candle in recent_candles]
        directional_rate = sum(move >= 0.02 and volume >= baseline * 0.80 for move, volume in zip(moves, volumes)) / len(recent_candles)
        recent_volume_ratio = sum(volumes) / len(volumes) / baseline
    elif today.empty:
        return 1.0
    else:
        recent = today.tail(3)
        moves = ((recent["close"] - recent["open"]) / recent["open"] * 100.0) * expected_direction
        confirmed = (moves >= 0.02) & (recent["volume"] >= baseline * 0.80)
        directional_rate = float(confirmed.mean()) if len(confirmed) else 0.0
        recent_volume_ratio = float(recent["volume"].mean()) / baseline
    activity = min(1.0, recent_volume_ratio / 1.20)
    return max(0.10, min(1.0, 0.20 + directional_rate * 0.55 + activity * 0.25))


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
    """Match the RFactor formula from dashboard_clean.py."""
    with DATA_LOCK:
        daily = HISTORY.get(token, {}).get("regular")
    if daily is None or daily.empty:
        return 0.0
    stats = daily[daily["volume"] > 0].copy()
    if stats.empty:
        return 0.0
    stats["change"] = stats["close"].pct_change() * 100.0
    stats = stats.tail(20)
    close = float(ltp or 0.0)
    vol = float(volume or 0.0)
    if vol <= 0 or close <= 0 or stats.empty:
        return 0.0
    avg_vol = float(stats["volume"].mean())
    avg_range = float((stats["high"] - stats["low"]).mean())
    avg_move = float(stats["change"].abs().mean())
    baselines = (avg_vol, avg_range, avg_move)
    if not all(math.isfinite(value) and value > 0 for value in baselines):
        return 0.0
    rvol = vol / avg_vol
    range_factor = (high - low) / avg_range
    move_factor = abs(change) / avg_move
    raw = (rvol ** 0.55) * (range_factor ** 0.30) * (move_factor ** 0.15)
    position = (close - low) / ((high - low) + 1e-9)
    freshness = position ** 3 if change >= 0 else (1.0 - position) ** 3
    if (high - low) / close * 100.0 < 0.60:
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

    now = datetime.now(IST)
    ratio = _volume_ratio(token, timeframe, volume, now)
    ema_gap = (ltp - ema) / ema * 100.0
    live_candles = _live_candles(token, now) if timeframe == "intraday" else []
    recent_change = 0.0
    if len(live_candles) >= 2:
        recent_base = live_candles[max(0, len(live_candles) - 3)]["open"]
        recent_change = (ltp - recent_base) / recent_base * 100.0
    elif len(close_values) >= 4 and close_values[-4]:
        recent_change = (ltp - close_values[-4]) / close_values[-4] * 100.0
    aligned_recent = recent_change if change >= 0 else -recent_change
    freshness = max(0.25, min(1.0, 0.25 + max(0.0, aligned_recent) / 0.80))
    effective_ratio = min(ratio, 1.0) + max(ratio - 1.0, 0.0) * freshness
    expected_direction = 1.0 if change >= 0 else -1.0
    trend_quality = _session_trend_quality(frame, ltp, change, now, live_candles) if timeframe == "intraday" else 1.0
    volume_quality = _volume_confirmation(frame, expected_direction, now, live_candles) if timeframe == "intraday" else 1.0
    flow_values = [candle["flow_delta"] for candle in live_candles[-3:] if candle.get("flow_delta") is not None]
    buy_sell_delta = sum(flow_values) / len(flow_values) if flow_values else None
    flow_alignment = expected_direction * buy_sell_delta if buy_sell_delta is not None else 0.0
    flow_quality = max(0.70, min(1.20, 1.0 + 0.20 * flow_alignment)) if buy_sell_delta is not None else 1.0
    stale_trend = (
        abs(change) * 0.40
        + effective_ratio * 1.6
        + max(0.0, adx - 18.0) / 12.0
        + abs(ema_gap) * 0.20
        + abs(rsi - 50.0) / 24.0 * 0.35
    )
    rfactor = _rfactor(token, timeframe, ltp, volume, day_high, day_low, change)
    rfactor_quality = max(0.0, min(1.0, rfactor / 4.0))
    continuation_quality = max(0.05, min(1.0, trend_quality * volume_quality))
    base_score = max(0.0, aligned_recent) * 2.3 + stale_trend * (0.35 + 0.65 * freshness)
    score = base_score * continuation_quality * (0.65 + 0.35 * rfactor_quality) * flow_quality
    volatility = "high" if abs(change) >= 2.5 or abs(ema_gap) >= 2.5 else "medium" if abs(change) >= 1.0 or abs(ema_gap) >= 1.2 else "low"
    positive = change >= 0
    return {
        "symbol": symbol,
        "display": symbol,
        "sector": sector,
        "direction": 1 if positive else -1,
        "change": round(change, 2),
        "recent": round(recent_change, 2),
        "trendQuality": round(trend_quality, 2),
        "volumeConfirm": round(volume_quality, 2),
        "continuation": round(continuation_quality, 2),
        "buySellDelta": round(buy_sell_delta, 2) if buy_sell_delta is not None else None,
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


def _directional_rank(rows: List[dict], score_key: str) -> List[dict]:
    positive = [row for row in rows if int(row.get("direction") or 0) >= 0]
    negative = [row for row in rows if int(row.get("direction") or 0) < 0]
    positive.sort(key=lambda row: float(row.get(score_key) or 0.0), reverse=True)
    negative.sort(key=lambda row: float(row.get(score_key) or 0.0), reverse=True)
    for group in (positive, negative):
        for position, row in enumerate(group, start=1):
            row["rank"] = position
    return positive + negative


def _rank(rows: List[dict]) -> List[dict]:
    rows[:] = _directional_rank(rows, "score")
    for row in rows:
        row["_scan_score"] = row.get("score")
        row.pop("score", None)
    return rows


def build_rows(timeframe: str, universe: str, sector: str) -> List[dict]:
    sector = sector.upper()
    with DATA_LOCK:
        detail_symbols = set(DETAIL_SYMBOLS)
    if universe == "index":
        rows = []
        for name, symbols in INDEX_GROUPS.items():
            selected = [symbol for symbol in dict.fromkeys(symbols) if not FAST_MODE or symbol in detail_symbols]
            children = [_build_row(symbol, "INDEX", timeframe) for symbol in selected]
            aggregate = _aggregate_index(name, [row for row in children if row])
            if aggregate:
                rows.append(aggregate)
        return _rank(rows)

    membership: Dict[str, str] = {}
    for group, symbols in SECTOR_DEFINITIONS.items():
        for symbol in symbols:
            membership.setdefault(symbol, group)
    symbols = list(dict.fromkeys(SECTOR_DEFINITIONS.get(sector, []))) if sector != "ALL" else list(membership)
    if FAST_MODE:
        symbols = [symbol for symbol in symbols if symbol in detail_symbols]
    rows = [_build_row(symbol, membership.get(symbol, sector), timeframe) for symbol in symbols]
    return _rank([row for row in rows if row])


def _rows_from_cache(timeframe: str, universe: str, sector: str) -> List[dict]:
    """Return a request-specific view without recalculating indicators."""
    with SCAN_CACHE_LOCK:
        rows = [dict(row) for row in SCAN_CACHE.get(timeframe, {}).get(universe, [])]
    if universe == "stocks" and sector != "ALL":
        rows = [row for row in rows if row.get("sector") == sector]
    rows = _directional_rank(rows, "_scan_score")
    for row in rows:
        row.pop("_scan_score", None)
    return rows


def _refresh_scan_cache() -> None:
    snapshots = {
        timeframe: {
            "stocks": build_rows(timeframe, "stocks", "ALL"),
            "index": build_rows(timeframe, "index", "ALL"),
        }
        for timeframe in ("intraday", "regular")
    }
    sector_flow = build_sector_flow()
    global SCAN_CACHE_UPDATED_AT
    with SCAN_CACHE_LOCK:
        for timeframe, universes in snapshots.items():
            SCAN_CACHE[timeframe]["stocks"] = universes["stocks"]
            SCAN_CACHE[timeframe]["index"] = universes["index"]
        SECTOR_FLOW_CACHE.clear()
        SECTOR_FLOW_CACHE.extend(sector_flow)
        SCAN_CACHE_UPDATED_AT = time.time()


def _start_scan_compute() -> None:
    global SCAN_COMPUTE_STARTED
    with SCAN_COMPUTE_START_LOCK:
        if SCAN_COMPUTE_STARTED:
            return
        SCAN_COMPUTE_STARTED = True

    def run() -> None:
        while True:
            started = time.monotonic()
            try:
                _refresh_scan_cache()
            except Exception:
                log.exception("Background scan cache refresh failed")
            elapsed = time.monotonic() - started
            time.sleep(max(0.1, SCAN_COMPUTE_EVERY_SEC - elapsed))

    threading.Thread(target=run, name="scan-compute", daemon=True).start()


def build_sector_flow() -> List[dict]:
    membership: Dict[str, str] = {}
    for group, symbols in SECTOR_DEFINITIONS.items():
        for symbol in symbols:
            membership.setdefault(symbol, group)
    grouped: Dict[str, dict] = {}
    with DATA_LOCK:
        ticks = {symbol: dict(TICK_STATE.get(token) or {}) for symbol, token in SYMBOL_TO_TOKEN.items()}
    for symbol, tick in ticks.items():
        sector = membership.get(symbol)
        if not sector:
            continue
        ltp = _as_float(tick.get("ltp"))
        ohlc = tick.get("ohlc") if isinstance(tick.get("ohlc"), dict) else {}
        open_price = _as_float(ohlc.get("open"))
        if not ltp or not open_price:
            continue
        change = (ltp - open_price) / open_price * 100.0
        group = grouped.setdefault(sector, {"name": sector, "sum": 0.0, "count": 0, "up": 0, "down": 0})
        group["sum"] += change
        group["count"] += 1
        group["up"] += int(change >= 0)
        group["down"] += int(change < 0)
    return [
        {
            "name": group["name"],
            "mean": round(group["sum"] / max(group["count"], 1), 2),
            "count": group["count"],
            "up": group["up"],
            "down": group["down"],
        }
        for group in sorted(grouped.values(), key=lambda item: item["sum"] / max(item["count"], 1), reverse=True)
    ]


def _feed_status() -> tuple[str, bool]:
    if kite is None or not SYMBOL_TO_TOKEN:
        return "missing_credentials", False
    if not market_is_open():
        return "previous_session", False
    fresh = LAST_TICK_TS and (time.time() - LAST_TICK_TS) <= TICK_STALE_SEC
    if SEED_IN_PROGRESS:
        return "seeding", bool(TICKER_CONNECTED and fresh)
    if TICKER_CONNECTED and fresh:
        return "live", True
    if not _has_current_session_data():
        return "previous_session", False
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
        "fast_mode": FAST_MODE,
        "detail_symbols": len(DETAIL_SYMBOLS),
        "ticks": TOTAL_TICKS,
        "last_tick": datetime.fromtimestamp(LAST_TICK_TS, IST).isoformat() if LAST_TICK_TS else None,
        "cache_ready": bool(SCAN_CACHE_UPDATED_AT),
        "cache_updated_at": datetime.fromtimestamp(SCAN_CACHE_UPDATED_AT, IST).isoformat() if SCAN_CACHE_UPDATED_AT else None,
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
        sector = "ALL"
    rows = _rows_from_cache(timeframe, universe, sector)
    with SCAN_CACHE_LOCK:
        sector_flow = [dict(item) for item in SECTOR_FLOW_CACHE]
        cache_updated_at = SCAN_CACHE_UPDATED_AT
    status, live = _feed_status()
    return jsonify({
        "live": live,
        "status": status,
        "market_open": market_is_open(),
        "updated_at": datetime.fromtimestamp(cache_updated_at, IST).strftime("%H:%M:%S IST") if cache_updated_at else None,
        "seed": dict(SEED_PROGRESS),
        "fast_mode": FAST_MODE,
        "universe_size": len(SYMBOL_TO_TOKEN),
        "detail_symbols": len(DETAIL_SYMBOLS),
        "sector_flow": sector_flow,
        "ticks": TOTAL_TICKS,
        "rows": rows,
    })


def initialize_live() -> None:
    """Start live services once for both Python and Gunicorn entrypoints."""
    global LIVE_INITIALIZED
    with LIVE_INIT_LOCK:
        if LIVE_INITIALIZED:
            return
        LIVE_INITIALIZED = True
    _start_scan_compute()
    try:
        load_instruments()
        _start_history_seed()
        _start_ticker()
    except Exception:
        log.exception("Live market startup failed; serving demo UI")
    finally:
        _start_daily_refresh()


def start() -> None:
    initialize_live()
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)


if __name__ == "__main__":
    start()
else:
    threading.Thread(target=initialize_live, name="live-init", daemon=True).start()
