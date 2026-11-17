import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
from prefect import flow, get_run_logger

RSI_PERIOD = 14
NOBITEX_BASE = "https://apiv2.nobitex.ir"
HEADERS = {"User-Agent": "TraderBot/RSI-Scanner-2.2"}

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))

GOLD_SYMBOL = "PAXGUSDT"
OTHER_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

GOLD_LEVELS = [(32, "720", "12h"), (32, "360", "6h"),
               (68, "720", "12h"), (68, "360", "6h")]

OTHER_LEVELS = [(25, "720", "12h"), (25, "360", "6h"),
                (75, "720", "12h"), (75, "360", "6h"),
                (47, "720", "12h"), (47, "360", "6h")]

MIN_STABLE_CANDLES = 100
MAX_RETRIES = 5


def get_closes(symbol, resolution, limit=500):
    """
    Returns (closes, timestamps) INCLUDING the current forming candle.
    The caller decides whether to drop it.
    """
    url = f"{NOBITEX_BASE}/market/udf/history"
    now = int(time.time())
    seconds_per_candle = 360 if resolution == "360" else 720
    aligned_now = (now // seconds_per_candle) * seconds_per_candle
    start = aligned_now - (limit - 1) * seconds_per_candle

    params = {
        "symbol": symbol, "resolution": resolution,
        "from": start, "to": now, "countback": limit,
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
            if resp.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            data = resp.json()
            if data.get("s") != "ok":
                return None, None
            closes = data.get("c", [])
            timestamps = data.get("t", [])
            if not closes or len(closes) < RSI_PERIOD + 3:
                return None, None

            # داده کامل برمی‌گردانیم، شامل کندل ناقص
            return pd.Series([float(c) for c in closes]), timestamps
        except Exception:
            time.sleep(1 + attempt)

    return None, None


def calculate_rsi(closes, period=14):
    """Wilder's RSI matching TradingView."""
    closes = pd.Series(closes).astype(float).reset_index(drop=True)
    delta = closes.diff()

    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    n = len(closes)
    rsi = pd.Series([float("nan")] * n, dtype=float)

    if n <= period:
        return rsi

    avg_gain = gain.iloc[1:period + 1].mean()
    avg_loss = loss.iloc[1:period + 1].mean()

    def _rsi(ag, al):
        if al == 0:
            return 100.0
        if ag == 0:
            return 0.0
        rs = ag / al
        return 100.0 - (100.0 / (1.0 + rs))

    rsi.iloc[period] = _rsi(avg_gain, avg_loss)
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gain.iloc[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss.iloc[i]) / period
        rsi.iloc[i] = _rsi(avg_gain, avg_loss)

    return rsi


def build_two_series(closes, timestamps):
    """
    Returns:
      rsi_live: RSI INCLUDING the current forming candle (for display)
      rsi_closed: RSI EXCLUDING the current forming candle (for cross detection)
      ts_closed: timestamps aligned with rsi_closed
    """
    rsi_live = calculate_rsi(closes)
    ts_live = list(timestamps)

    # برای کراس: کندل آخر (ناقص) حذف می‌شود
    closes_closed = closes.iloc[:-1].reset_index(drop=True)
    ts_closed = list(timestamps[:-1])
    rsi_closed = calculate_rsi(closes_closed)

    return rsi_live, ts_live, rsi_closed, ts_closed


def find_crosses_last_12h(rsi_series, timestamps, level):
    if rsi_series is None or timestamps is None:
        return None, False, None, None, 0

    rsi = pd.Series(rsi_series).astype(float).reset_index(drop=True)
    ts = list(timestamps)

    n = min(len(rsi), len(ts))
    if n < 2:
        return None, False, None, None, 0
    rsi = rsi.iloc[-n:].reset_index(drop=True)
    ts = ts[-n:]

    valid_idx = [i for i in range(n) if pd.notna(rsi.iloc[i])]
    if len(valid_idx) < 2:
        return None, False, None, None, 0

    last_ts = ts[valid_idx[-1]]
    cutoff = last_ts - 12 * 3600
    EPS = 1e-9

    latest_closed_cross_time = None
    cross_count = 0
    prev_cross_ts = None

    for k in range(1, len(valid_idx)):
        i_prev = valid_idx[k - 1]
        i_curr = valid_idx[k]
        t_curr = ts[i_curr]
        if t_curr < cutoff:
            continue
        prev_rsi = float(rsi.iloc[i_prev])
        curr_rsi = float(rsi.iloc[i_curr])

        crossed_up = (prev_rsi < level - EPS) and (curr_rsi >= level - EPS)
        if crossed_up and prev_cross_ts != t_curr:
            cross_count += 1
            prev_cross_ts = t_curr
            latest_closed_cross_time = datetime.fromtimestamp(
                t_curr, tz=IRAN_TZ
            ).strftime("%Y-%m-%d %H:%M")

    curr_rsi = float(rsi.iloc[valid_idx[-1]])
    prev_rsi = float(rsi.iloc[valid_idx[-2]]) if len(valid_idx) >= 2 else None
    return latest_closed_cross_time, False, curr_rsi, prev_rsi, cross_count


def format_check(level, tf_label, last_closed_cross, curr_rsi, cross_count, n_closed):
    if curr_rsi is None:
        return f"    ❌ cross {level} ({tf_label}): data unavailable\n"
    if last_closed_cross:
        return (f"    ✅ cross {level} ({tf_label}): READY "
                f"| last closed cross: {last_closed_cross} | count(12h): {cross_count}\n")
    warn = ""
    if n_closed < MIN_STABLE_CANDLES:
        warn = f" [warn: only {n_closed} closed candles]"
    return (f"    ❌ cross {level} ({tf_label}): no cross in last 12h"
            f"{warn}\n")


def fetch_tf_data(symbols, resolution):
    """
    Returns dict:
      sym -> {
        'live': (rsi_live, ts_live),
        'closed': (rsi_closed, ts_closed),
      }
    """
    data = {}
    for sym in symbols:
        closes, timestamps = get_closes(sym, resolution)
        if closes is None:
            data[sym] = {"live": (None, None), "closed": (None, None)}
            continue
        rsi_live, ts_live, rsi_closed, ts_closed = build_two_series(closes, timestamps)
        data[sym] = {
            "live": (rsi_live, ts_live),
            "closed": (rsi_closed, ts_closed),
        }
    return data


def emit_asset_block(logger, title, symbol, levels, data_12h, data_6h):
    logger.info(f"\n{title}")
    logger.info("-" * 70)

    # RSI لحظه‌ای: از سری live (شامل کندل ناقص)
    rsi_live_12h, _ = data_12h.get(symbol, {}).get("live", (None, None))
    if rsi_live_12h is not None and len(rsi_live_12h.dropna()) > 0:
        logger.info(f"  RSI (live): {float(rsi_live_12h.dropna().iloc[-1]):.2f}")
    else:
        logger.info("  RSI (live): data unavailable")
    logger.info("")

    for level, resolution, tf_label in levels:
        source = data_12h if resolution == "720" else data_6h
        rsi_closed, ts_closed = source.get(symbol, {}).get("closed", (None, None))
        if rsi_closed is None:
            logger.info(f"    ❌ cross {level} ({tf_label}): data unavailable")
            continue
        n_closed = int(rsi_closed.notna().sum())
        last_closed_cross, _, _, _, cross_count = find_crosses_last_12h(
            rsi_closed, ts_closed, level
        )
        logger.info(format_check(level, tf_label, last_closed_cross, None, cross_count, n_closed))


@flow(log_prints=True)
def run_scan():
    logger = get_run_logger()
    now_str = datetime.now(IRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 70)
    logger.info(f"RSI Cross Scanner v2.3 | {now_str} (Iran Time)")
    logger.info("=" * 70)

    all_symbols = [GOLD_SYMBOL] + OTHER_SYMBOLS

    data_12h = fetch_tf_data(all_symbols, "720")
    data_6h = fetch_tf_data(all_symbols, "360")

    emit_asset_block(logger, "[Block 1] PAXG (Gold)", GOLD_SYMBOL, GOLD_LEVELS, data_12h, data_6h)

    for idx, sym in enumerate(OTHER_SYMBOLS, start=2):
        emit_asset_block(
            logger, f"[Block {idx}] {sym.replace('USDT', '')}", sym, OTHER_LEVELS, data_12h, data_6h
        )

    logger.info("\nScan completed.")


if __name__ == "__main__":
    run_scan()
