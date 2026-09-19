"""
Live backend for intraday-momentum-scanner.html (single-file version).

Run with Kite credentials in:
- KITE_API_KEY
- KITE_ACCESS_TOKEN

Endpoints:
- GET /               -> serves intraday-momentum-scanner.html
- GET /api/meta       -> sector names & limits (used by frontend; removes need for SECTOR_DEFINITIONS in HTML)
- GET /api/scan       -> heavy scan (top-N) using history + indicators
- GET /api/sector_flow-> full-universe sector averages (tick-based, fast)
- GET /api/health     -> status
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
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from flask import Flask, jsonify, request, send_file
from kiteconnect import KiteConnect, KiteTicker

# ---------------- Universe / sectors (INLINED) ----------------

SECTOR_DEFINITIONS: Dict[str, List[str]] = {
    "METAL": [
        "ADANIENT", "APLAPOLLO", "BHARATFORG", "COALINDIA",
        "HINDALCO", "HINDZINC", "JSWSTEEL", "JINDALSTEL", "NMDC",
        "NATIONALUM", "SAIL", "TATASTEEL", "VEDL",
    ],
    "REALTY": [
        "PHOENIXLTD", "GODREJPROP", "LODHA", "OBEROIRLTY", "DLF",
        "PRESTIGE", "NBCC", "RVNL",
    ],
    "ENERGY": [
        "RELIANCE", "ONGC", "IOC", "BPCL", "OIL", "NTPC", "POWERGRID",
        "POWERINDIA", "TATAPOWER", "JSWENERGY", "ADANIGREEN", "ADANIENSOL",
        "NHPC", "IREDA", "SUZLON", "INOXWIND", "WAAREEENER", "PREMIERENE",
        "PETRONET", "GAIL", "HINDPETRO",
    ],
    "AUTO": [
        "BOSCHLTD", "TIINDIA", "HEROMOTOCO", "M&M", "EICHERMOT",
        "BAJAJ-AUTO", "ASHOKLEY", "MARUTI", "TVSMOTOR", "MOTHERSON",
        "SONACOMS", "UNOMINDA", "TMPV", "HYUNDAI", "AMBER",
    ],
    "IT": [
        "INFY", "TCS", "HCLTECH", "WIPRO", "TECHM", "LTM", "MPHASIS",
        "KPITTECH", "COFORGE", "PERSISTENT", "TATAELXSI", "OFSS", "CAMS",
        "NAUKRI", "KAYNES",
    ],
    "PHARMA": [
        "CIPLA", "ALKEM", "BIOCON", "DRREDDY", "MANKIND", "TORNTPHARM",
        "ZYDUSLIFE", "DIVISLAB", "LUPIN", "LAURUSLABS", "FORTIS", "AUROPHARMA",
        "GLENMARK", "SUNPHARMA", "MAXHEALTH", "APOLLOHOSP",
    ],
    "FMCG": [
        "HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "DABUR", "MARICO",
        "COLPAL", "GODREJCP", "TATACONSUM", "PATANJALI", "UNITDSPR", "RADICO",
        "VBL", "DMART", "NYKAA", "ETERNAL", "SWIGGY", "TITAN", "TRENT",
        "VMM", "KALYANKJIL", "JUBLFOOD", "ASIANPAINT",
    ],
    "CEMENT": [
        "ULTRACEMCO", "SHREECEM", "AMBUJACEM", "DALBHARAT", "GRASIM", "ASTRAL",
        "PIDILITIND", "SUPREMEIND",
    ],
    "FINSERVICE": [
        "BAJFINANCE", "BAJAJFINSV", "BAJAJHLDNG", "ICICIPRULI", "ICICIGI", "SBILIFE",
        "HDFCLIFE", "LICI", "LICHSGFIN", "PNBHOUSING", "MUTHOOTFIN", "MANAPPURAM",
        "CHOLAFIN", "PFC", "RECLTD", "MOTILALOFS", "HDFCAMC", "360ONE", "KFINTECH",
        "NUVAMA", "PAYTM", "POLICYBZR", "SBICARD", "JIOFIN", "SHRIRAMFIN", "ANGELONE",
        "BSE", "CDSL", "MCX", "IRFC",
    ],
    "BANK": [
        "HDFCBANK", "ICICIBANK", "AXISBANK", "KOTAKBANK", "IDFCFIRSTB", "FEDERALBNK",
        "INDUSINDBK", "AUBANK", "BANDHANBNK", "RBLBANK",
    ],
    "PSUBANK": [
        "SBIN", "PNB", "BANKBARODA", "CANBK", "UNIONBANK", "BANKINDIA", "INDIANB",
    ],
    "DURABLES": [
        "INDUSTOWER", "HAVELLS", "KEI", "POLYCAB", "CROMPTON", "VOLTAS", "PGEL",
        "DIXON", "SRF",
    ],
    "LOGISTICS": [
        "CONCOR", "DELHIVERY", "INDIGO", "INDHOTEL", "IRCTC", "BLUESTARCO",
        "GMRAIRPORT", "PAGEIND", "UPL", "ADANIPORTS",
    ],
    "DEFENCE": [
        "ABB", "BEL", "BDL", "BHEL", "CGPOWER", "CUMMINSIND", "HAL", "LT",
        "MAZDOCK", "COCHINSHIP", "SIEMENS", "SOLARINDS",
    ],
    "NIFTY_50": [
        "ADANIENT", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO", "BAJFINANCE",
        "BAJAJFINSV", "BEL", "BHARTIARTL", "BPCL", "CIPLA", "COALINDIA", "DRREDDY",
        "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HINDALCO",
        "HINDUNILVR", "ICICIBANK", "INFY", "INDIGO", "ITC", "JIOFIN", "JSWSTEEL",
        "KOTAKBANK", "LT", "M&M", "MARUTI", "MAXHEALTH", "NESTLEIND", "NTPC", "ONGC",
        "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN", "SUNPHARMA", "TCS",
        "TATACONSUM", "TATASTEEL", "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
        "TMPV", "ETERNAL",
    ],
}

ALL_SYMBOLS = sorted({s for group in SECTOR_DEFINITIONS.values() for s in group})

# For sector classification/flow, exclude NIFTY_50 bucket to avoid hijacking "true" sector
SECTOR_FLOW_KEYS = [k for k in SECTOR_DEFINITIONS.keys() if k != "NIFTY_50"]
SYMBOL_TO_SECTOR: Dict[str, str] = {}
for sec in SECTOR_FLOW_KEYS:
    for sym in SECTOR_DEFINITIONS.get(sec, []):
        SYMBOL_TO_SECTOR.setdefault(sym, sec)

# ---------------- App config ----------------

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

SCAN_LIMIT_DEFAULT = int(os.getenv("SCAN_LIMIT_DEFAULT", "20"))
CANDIDATE_MULTIPLIER = int(os.getenv("CANDIDATE_MULTIPLIER", "8"))
CANDIDATE_TICK_FRESH_SEC = int(os.getenv("CANDIDATE_TICK_FRESH_SEC", "75"))

SECTOR_MODAL_LIMIT_MAX = int(os.getenv("SECTOR_MODAL_LIMIT_MAX", "200"))  # frontend popup limit cap
SECTOR_FLOW_CACHE_TTL_SEC = float(os.getenv("SECTOR_FLOW_CACHE_TTL_SEC", "2.0"))

app = Flask(__name__)

kite: Optional[KiteConnect] = None
SYMBOL_TO_TOKEN: Dict[str, int] = {}
TOKEN_TO_SYMBOL: Dict[int, str] = {}

TICK_STATE: Dict[int, Dict[str, Any]] = {}
PRICE_HISTORY: Dict[int, deque] = {}
HISTORY: Dict[int, Dict[str, pd.DataFrame]] = {}

DATA_LOCK = threading.RLock()
SEED_LOCK = threading.Lock()

LAST_TICK_TS = 0.0
TOTAL_TICKS = 0
TICKER_CONNECTED = False
TICKER_STARTED = False
SEED_STARTED = False
LIVE_INITIALIZED = False

SEED_PROGRESS = {"done": 0, "total": 0, "errors": 0}
SECTOR_FLOW_CACHE: Dict[Tuple[str, str, str], Dict[str, Any]] = {}


# ---------------- Helpers ----------------

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


def _order_book_delta(depth: dict) -> Optional[float]:
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
        log.warning("Kite credentials missing; API will run but no live data.")
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
        log.warning("Missing symbols (not in NSE instruments): %s", ", ".join(missing))


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

    ts = time.time()
    TICK_STATE[token] = {"ltp": ltp, "volume": volume, "ohlc": ohlc, "flow_delta": flow_delta, "ts": ts}

    history = PRICE_HISTORY.setdefault(token, deque(maxlen=3600))
    history.append((ts, ltp, volume, flow_delta))


def _start_ticker() -> None:
    global TICKER_STARTED, TICKER_CONNECTED
    if TICKER_STARTED or kite is None or not SYMBOL_TO_TOKEN:
        return

    TICKER_STARTED = True
    tokens = sorted(TOKEN_TO_SYMBOL)

    def run() -> None:
        global TICKER_CONNECTED, LAST_TICK_TS, TOTAL_TICKS
        while True:
            try:
                ticker = KiteTicker(API_KEY, ACCESS_TOKEN)

                def on_connect(ws, _response):
                    global TICKER_CONNECTED
                    ws.subscribe(tokens)
                    ws.set_mode(ws.MODE_FULL, tokens)
                    TICKER_CONNECTED = True
                    log.info("KiteTicker connected; subscribed to %s tokens", len(tokens))

                def on_ticks(_ws, ticks):
                    global LAST_TICK_TS, TOTAL_TICKS
                    with DATA_LOCK:
                        for t in ticks:
                            _update_tick(t)
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

    time.sleep(HISTORY_SLEEP_SEC)


def _start_history_seed() -> None:
    global SEED_STARTED
    if SEED_STARTED or kite is None:
        return

    SEED_STARTED = True
    tokens = sorted(TOKEN_TO_SYMBOL.items())

    with SEED_LOCK:
        SEED_PROGRESS["total"] = len(tokens)

    def run() -> None:
        for token, symbol in tokens:
            try:
                _seed_symbol(symbol, token)
            except Exception:
                with SEED_LOCK:
                    SEED_PROGRESS["errors"] += 1
                log.exception("History seed failed for %s", symbol)
            finally:
                with SEED_LOCK:
                    SEED_PROGRESS["done"] += 1

    threading.Thread(target=run, name="history-seed", daemon=True).start()


# ---------------- Status ----------------

def _tick_status() -> tuple[str, bool]:
    """Status based on tick feed only (no history dependency)."""
    if kite is None or not SYMBOL_TO_TOKEN:
        return "missing_credentials", False
    if not market_is_open():
        return "market_closed", False
    fresh = LAST_TICK_TS and (time.time() - LAST_TICK_TS) <= TICK_STALE_SEC
    if TICKER_CONNECTED and fresh:
        return "live", True
    return "waiting_for_ticks", False


def _scan_status() -> tuple[str, bool]:
    """Status for indicator scan (history required)."""
    status, live = _tick_status()
    if status in {"missing_credentials", "market_closed"}:
        return status, False
    with SEED_LOCK:
        seeding = SEED_PROGRESS["total"] > 0 and SEED_PROGRESS["done"] < SEED_PROGRESS["total"]
    if seeding:
        return "seeding", False
    return status, live


# ---------------- Indicators (scan) ----------------

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
    true_range = pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
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

    return frame.copy(deep=False), float(ltp), float(volume), ohlc


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
    coords = " ".join(f"{i * 18},{24 - ((v - low) / spread) * 19:.1f}" for i, v in enumerate(values))
    color = "#0d9b73" if positive else "#d44e57"
    return (
        '<svg class="spark" viewBox="0 0 72 26" aria-label="Five candle trend">'
        f'<polyline stroke="{color}" points="{coords}"></polyline>'
        "</svg>"
    )


def _rfactor(token: int, timeframe: str, ltp: float, volume: float, high: float, low: float, change: float) -> float:
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

    raw = (rvol**0.55) * (range_factor**0.30) * (move_factor**0.15)

    span = max(high - low, 1e-9)
    position = max(0.0, min(1.0, (ltp - low) / span))
    freshness = position**3 if change >= 0 else (1.0 - position) ** 3

    if (high - low) / max(ltp, 1e-9) * 100.0 < 0.60:
        raw *= 0.12

    raw *= max(freshness, 0.001)
    return round(3.5 * math.log1p(max(raw, 0.0)), 2)


def _micro_change(token: int, lookback_sec: int, now_ts: Optional[float] = None) -> Optional[float]:
    now_ts = now_ts or time.time()
    with DATA_LOCK:
        ticks = list(PRICE_HISTORY.get(token, ()))
    if len(ticks) < 2:
        return None

    latest_price = _as_float(ticks[-1][1])
    if latest_price is None or latest_price <= 0:
        return None

    target = now_ts - lookback_sec
    base_price = None
    for ts, price, *_ in reversed(ticks):
        if ts <= target:
            price = _as_float(price)
            if price is not None and price > 0:
                base_price = price
                break

    if base_price is None or base_price <= 0:
        return None
    return (latest_price - base_price) / base_price * 100.0


def _rank_value(row: dict) -> float:
    return float(row.get("_rank") or 0.0)


def _pick_candidates(symbols: List[str], timeframe: str, limit: int) -> List[str]:
    target_n = max(30, min(len(symbols), limit * max(3, CANDIDATE_MULTIPLIER)))
    now_ts = time.time()

    scored: List[tuple[float, str]] = []
    with DATA_LOCK:
        for symbol in symbols:
            token = SYMBOL_TO_TOKEN.get(symbol)
            if not token:
                continue
            tick = TICK_STATE.get(token)
            if not tick:
                continue
            ts = _as_float(tick.get("ts")) or 0.0
            if now_ts - ts > CANDIDATE_TICK_FRESH_SEC:
                continue

            ltp = _as_float(tick.get("ltp")) or 0.0
            vol = _as_float(tick.get("volume")) or 0.0
            flow = _as_float(tick.get("flow_delta")) or 0.0
            ohlc = tick.get("ohlc") if isinstance(tick.get("ohlc"), dict) else {}
            op = _as_float(ohlc.get("open")) or 0.0

            chg = 0.0
            if timeframe == "intraday" and op > 0:
                chg = (ltp - op) / op * 100.0

            quick = abs(chg) * 1.8 + math.log1p(max(vol, 0.0)) * 0.06 + abs(flow) * 0.25
            scored.append((quick, symbol))

    if not scored:
        return symbols[:target_n]

    scored.sort(key=lambda x: x[0], reverse=True)
    return [sym for _, sym in scored[:target_n]]


def _build_row(symbol: str, sector: str, timeframe: str) -> Optional[dict]:
    token = SYMBOL_TO_TOKEN.get(symbol)
    if not token:
        return None

    hs = _history_state(token, timeframe)
    if hs is None:
        return None

    frame, ltp, volume, ohlc = hs

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

    # micro momentum factor (5m)
    micro_factor = 1.0
    micro_5m = None
    if timeframe == "intraday":
        micro_5m = _micro_change(token, 300, time.time())
        if micro_5m is not None:
            expected_direction = 1.0 if change >= 0 else -1.0
            aligned_micro = expected_direction * micro_5m
            micro_factor = max(0.70, min(1.30, 1.0 + aligned_micro / 2.0))

    rfactor = _rfactor(token, timeframe, ltp, volume, day_high, day_low, change)

    # ranking momentum
    expected_direction = 1.0 if change >= 0 else -1.0
    freshness = 1.0 if (micro_5m is None) else max(0.7, min(1.3, 1.0 + (expected_direction * micro_5m) / 2.0))
    base_score = (
        abs(change) * 0.40
        + max(0.0, ratio) * 1.6
        + max(0.0, adx - 18.0) / 12.0
        + abs(ema_gap) * 0.20
        + abs(rsi - 50.0) / 24.0 * 0.35
    )
    rfactor_mult = 1.0 + max(0.0, float(rfactor)) / 8.0
    momentum = base_score * rfactor_mult * micro_factor * freshness

    volatility = (
        "high" if abs(change) >= 2.5 or abs(ema_gap) >= 2.5
        else "medium" if abs(change) >= 1.0 or abs(ema_gap) >= 1.2
        else "low"
    )
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
        "rfactor": rfactor,
        "momentum": round(momentum, 4),
        "micro5m": round(micro_5m, 3) if micro_5m is not None else None,
        "volatility": volatility,
        "rank": 0,
        "spark": _sparkline(close_values, positive),
        "isIndex": False,
        "_rank": float(momentum),
    }


def _rank(rows: List[dict]) -> List[dict]:
    rows.sort(key=_rank_value, reverse=True)
    for pos, row in enumerate(rows, start=1):
        row["rank"] = pos
        row.pop("_rank", None)
    return rows


def build_rows(timeframe: str, universe: str, sector: str, limit: int) -> tuple[List[dict], int]:
    """
    sector must be:
      - "all"
      - OR an UPPERCASE key in SECTOR_DEFINITIONS
    """
    if universe != "stocks":
        return [], 0

    membership: Dict[str, str] = dict(SYMBOL_TO_SECTOR)
    all_symbols = list(membership.keys()) if sector == "all" else list(dict.fromkeys(SECTOR_DEFINITIONS.get(sector, [])))
    universe_total = len(all_symbols)

    candidates = _pick_candidates(all_symbols, timeframe, limit) if sector == "all" else all_symbols
    rows = [_build_row(sym, membership.get(sym, sector), timeframe) for sym in candidates]
    ranked = _rank([r for r in rows if r])
    return ranked[:limit], universe_total


# ---------------- Sector flow (full universe, tick-based) ----------------

def _tick_change_percent(token: int, timeframe: str) -> Optional[float]:
    """
    Tick-based % change:
      - intraday: (LTP - day_open) / day_open
      - regular : (LTP - prev_close) / prev_close   (Kite tick ohlc.close is prev close)
    """
    with DATA_LOCK:
        tick = TICK_STATE.get(token)
        if not tick:
            return None
        ltp = _as_float(tick.get("ltp"))
        ohlc = tick.get("ohlc") if isinstance(tick.get("ohlc"), dict) else {}

    if ltp is None or ltp <= 0:
        return None

    base_key = "open" if timeframe == "intraday" else "close"
    base = _as_float(ohlc.get(base_key))
    if base is None or base <= 0:
        return None

    return (ltp - base) / base * 100.0


def _sector_flow_items(timeframe: str, sector_key: str) -> tuple[list[dict], int, int]:
    """
    Returns: (items, universe_total, priced_count)
    items: [{name, mean, count, up, down}]
    """
    sectors = SECTOR_FLOW_KEYS if sector_key == "all" else ([sector_key] if sector_key in SECTOR_DEFINITIONS else SECTOR_FLOW_KEYS)

    # "full universe" total = unique across non-NIFTY_50 buckets
    universe_total = len(SYMBOL_TO_SECTOR)
    priced_total = 0
    items: list[dict] = []

    for sector in sectors:
        syms = list(dict.fromkeys(SECTOR_DEFINITIONS.get(sector, [])))
        changes: list[float] = []

        for sym in syms:
            token = SYMBOL_TO_TOKEN.get(sym)
            if not token:
                continue
            chg = _tick_change_percent(token, timeframe)
            if chg is None:
                continue
            changes.append(chg)

        if not changes:
            continue

        priced_total += len(changes)
        mean = sum(changes) / len(changes)
        up = sum(1 for c in changes if c >= 0)
        down = len(changes) - up
        items.append({"name": sector, "mean": round(mean, 2), "count": len(changes), "up": up, "down": down})

    items.sort(key=lambda x: float(x["mean"]), reverse=True)
    return items, universe_total, priced_total


# ---------------- Routes ----------------

@app.get("/")
def index():
    return send_file(BASE_DIR / "intraday-momentum-scanner.html")


@app.get("/api/meta")
def meta():
    """Frontend uses this to populate sector dropdown (no SECTOR_DEFINITIONS in HTML)."""
    return jsonify(
        {
            "sectors": sorted([k for k in SECTOR_DEFINITIONS.keys() if k != "NIFTY_50"]),
            "scan_limit_default": SCAN_LIMIT_DEFAULT,
            "scan_limit_max": 200,
            "sector_modal_limit_max": SECTOR_MODAL_LIMIT_MAX,
            "market_open": market_is_open(),
        }
    )


@app.get("/api/health")
def health():
    tick_status, tick_live = _tick_status()
    scan_status, scan_live = _scan_status()
    with SEED_LOCK:
        seed = dict(SEED_PROGRESS)
    return jsonify(
        {
            "tick_status": tick_status,
            "tick_live": tick_live,
            "scan_status": scan_status,
            "scan_live": scan_live,
            "market_open": market_is_open(),
            "seed": seed,
            "symbols": len(SYMBOL_TO_TOKEN),
            "ticks": TOTAL_TICKS,
            "last_tick": datetime.fromtimestamp(LAST_TICK_TS, IST).isoformat() if LAST_TICK_TS else None,
        }
    )


@app.get("/api/scan")
def scan():
    timeframe = request.args.get("type", "intraday").lower()
    universe = request.args.get("universe", "stocks").lower()
    sector_raw = (request.args.get("sector", "all") or "all").strip()

    limit_raw = request.args.get("limit", str(SCAN_LIMIT_DEFAULT))
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = SCAN_LIMIT_DEFAULT
    limit = max(1, min(200, limit))

    if timeframe not in {"intraday", "regular"}:
        return jsonify({"error": "type must be intraday or regular"}), 400
    if universe not in {"stocks"}:
        return jsonify({"error": "universe must be stocks (index removed in single-file build)"}), 400

    sector_upper = sector_raw.upper()
    if sector_upper == "ALL":
        sector_key = "all"
    elif sector_upper in SECTOR_DEFINITIONS:
        sector_key = sector_upper
    else:
        sector_key = "all"

    rows, universe_total = build_rows(timeframe, universe, sector_key, limit)
    status, live = _scan_status()

    with SEED_LOCK:
        seed = dict(SEED_PROGRESS)

    return jsonify(
        {
            "live": live,
            "status": status,
            "market_open": market_is_open(),
            "updated_at": datetime.now(IST).strftime("%H:%M:%S IST"),
            "seed": seed,
            "ticks": TOTAL_TICKS,
            "limit": limit,
            "universe_total": universe_total,
            "returned": len(rows),
            "rows": rows,
        }
    )


@app.get("/api/sector_flow")
def sector_flow():
    timeframe = request.args.get("type", "intraday").lower()
    sector_raw = (request.args.get("sector", "all") or "all").strip()

    if timeframe not in {"intraday", "regular"}:
        return jsonify({"error": "type must be intraday or regular"}), 400

    sector_upper = sector_raw.upper()
    if sector_upper == "ALL":
        sector_key = "all"
    elif sector_upper in SECTOR_DEFINITIONS:
        sector_key = sector_upper
    else:
        sector_key = "all"

    cache_key = (timeframe, "stocks", sector_key)
    now_ts = time.time()

    cached = SECTOR_FLOW_CACHE.get(cache_key)
    if cached and (now_ts - float(cached["ts"])) <= SECTOR_FLOW_CACHE_TTL_SEC:
        return jsonify(cached["payload"])

    items, universe_total, priced = _sector_flow_items(timeframe, sector_key)
    status, live = _tick_status()

    payload = {
        "live": live,
        "status": status,
        "market_open": market_is_open(),
        "updated_at": datetime.now(IST).strftime("%H:%M:%S IST"),
        "ticks": TOTAL_TICKS,
        "type": timeframe,
        "sector": sector_key,
        "universe_total": universe_total,
        "priced": priced,
        "items": items,
    }

    SECTOR_FLOW_CACHE[cache_key] = {"ts": now_ts, "payload": payload}
    return jsonify(payload)


def initialize_live() -> None:
    global LIVE_INITIALIZED
    if LIVE_INITIALIZED:
        return
    LIVE_INITIALIZED = True
    try:
        load_instruments()
        _start_history_seed()
        _start_ticker()
    except Exception:
        log.exception("Live startup failed; API will still serve UI, but without live ticks.")


def start() -> None:
    initialize_live()
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)


initialize_live()

if __name__ == "__main__":
    start()