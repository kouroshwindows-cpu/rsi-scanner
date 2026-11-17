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

# (level, resolution, tf_label)
GOLD_LEVELS = [(32, "720", "12h"), (32, "360", "6h"),
               (68, "720", "12h"), (68, "360", "6h")]

OTHER_LEVELS = [(25, "720", "12h"), (25, "360", "6h"),
                (75, "720", "12h"), (75, "360", "6h"),
                (47, "720", "12h"), (47, "360", "6h")]

# حداقل تعداد کندل بسته‌شده برای RSI پایدار
MIN_STABLE_CANDLES = 100
MAX_RETRIES = 5


def get_closes(symbol, resolution, limit=500):
    """
    Fetch close prices and timestamps from Nobitex.
    Returns (closes_series, timestamps) where closes_series EXCLUDES the
    current incomplete candle (we drop the last element).
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

            # Drop the last (incomplete) candle from both series.
            # Nobitex history endpoint includes the currently-forming candle.
            closes = closes[:-1]
            timestamps = timestamps[:-1]

            if len(closes) < RSI_PERIOD + 2:
                return None, None

            return pd.Series([float(c) for c in closes]), timestamps
        except Exception:
            time.sleep(1 + attempt)

    return None, None


def calculate_rsi(closes, period=14):
    """
    Standard Wilder RSI matching TradingView:
      - seed = simple average of first `period` gains/losses
      - then recursive smoothing: avg = (prev_avg*(period-1) + current)/period
    Returns a pandas Series aligned with `closes`, with NaN for the first
    `period` entries.
    """
    closes = pd.Series(closes).astype(float).reset_index(drop=True)
    delta = closes.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    # First `period` valid deltas are indices 1..period
    n = len(closes)
    avg_gain = pd.Series([float("nan")] * n, dtype=float)
    avg_loss = pd.Series([float("nan")] * n, dtype=float)

    if n <= period:
        return 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

    # Seed
    seed_gain = gain.iloc[1:period + 1].mean()
    seed_loss = loss.iloc[1:period + 1].mean()
    avg_gain.iloc[period] = seed_gain
    avg_loss.iloc[period] = seed_loss

    # Recursive Wilder smoothing
    for i in range(period + 1, n):
        avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (period - 1) + loss.iloc[i]) / period

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    # If avg_loss == 0 -> RSI = 100 ; if avg_gain == 0 -> RSI = 0
    rsi = rsi.where(avg_loss != 0, 100.0)
    rsi = rsi.where(avg_gain != 0, 0.0)
    rsi.iloc[:period] = float("nan")
    return rsi


def find_crosses_last_12h(rsi_series, timestamps, level):
    """
    Check upward cross (prev < level <= curr) in the last 12 hours.
    Only CLOSED candles are considered (incomplete candle already dropped).

    Output:
      - last_closed_cross_time: time of most recent closed-candle cross
      - pending_cross: always False (kept for signature compatibility)
      - curr_rsi / prev_rsi
      - cross_count: number of crosses in the last 12h (closed candles only)
    """
    if rsi_series is None or len(rsi_series.dropna()) < 2:
        return None, False, None, None, 0

    # Align: rsi_series may have leading NaN
    valid_mask = rsi_series.notna()
    rsi_valid = rsi_series[valid_mask]
    ts_valid = [timestamps[i] for i in range(len(timestamps)) if valid_mask.iloc[i]]

    if len(rsi_valid) < 2:
        return None, False, None, None, 0

    cutoff = time.time() - 12 * 3600
    latest_closed_cross_time = None
    cross_count = 0

    for i in range(1, len(rsi_valid)):
        ts = ts_valid[i]
        if ts < cutoff:
            continue
        prev_rsi = float(rsi_valid.iloc[i - 1])
        curr_rsi = float(rsi_valid.iloc[i])
        if prev_rsi < level <= curr_rsi:
            cross_count += 1
            latest_closed_cross_time = datetime.fromtimestamp(ts, tz=IRAN_TZ).strftime("%Y-%m-%d %H:%M")

    curr_rsi = float(rsi_valid.iloc[-1])
    prev_rsi = float(rsi_valid.iloc[-2])
    return latest_closed_cross_time, False, curr_rsi, prev_rsi, cross_count


def format_check(level, tf_label, last_closed_cross, curr_rsi, cross_count, n_closed):
    """Format one cross-check line (English output)."""
    if curr_rsi is None:
        return f"    cross {level} ({tf_label}): data unavailable\n"
    if last_closed_cross:
        return (f"    cross {level} ({tf_label}): READY (RSI {curr_rsi:.2f}) "
                f"| last closed cross: {last_closed_cross} | count(12h): {cross_count}\n")
    warn = ""
    if n_closed < MIN_STABLE_CANDLES:
        warn = f" [warn: only {n_closed} closed candles, RSI may be unstable]"
    return (f"    cross {level} ({tf_label}): no cross in last 12h "
            f"(RSI {curr_rsi:.2f}){warn}\n")


def fetch_tf_data(symbols, resolution):
    """Fetch data for all symbols at one timeframe once, compute RSI."""
    data = {}
    for sym in symbols:
        closes, timestamps = get_closes(sym, resolution)
        if closes is None:
            data[sym] = (None, None)
            continue
        rsi = calculate_rsi(closes)
        valid_ts = timestamps[-len(rsi):]
        data[sym] = (rsi, valid_ts)
    return data


def emit_asset_block(logger, title, symbol, levels, data_12h, data_6h):
    """Print a full block for one asset."""
    logger.info(f"\n{title}")
    logger.info("-" * 70)

    rsi_12h, _ = data_12h.get(symbol, (None, None))
    if rsi_12h is not None and len(rsi_12h.dropna()) > 0:
        logger.info(f"  Current RSI (12h series): {float(rsi_12h.dropna().iloc[-1]):.2f}")
    else:
        logger.info("  Current RSI: data unavailable")
    logger.info("")

    for level, resolution, tf_label in levels:
        source = data_12h if resolution == "720" else data_6h
        rsi, valid_ts = source.get(symbol, (None, None))
        if rsi is None:
            logger.info(f"    cross {level} ({tf_label}): data unavailable")
            continue
        n_closed = int(rsi.notna().sum())
        last_closed_cross, _, curr_rsi, prev_rsi, cross_count = find_crosses_last_12h(
            rsi, valid_ts, level
        )
        logger.info(format_check(level, tf_label, last_closed_cross, curr_rsi, cross_count, n_closed))


@flow(log_prints=True)
def run_scan():
    logger = get_run_logger()
    now_str = datetime.now(IRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 70)
    logger.info(f"RSI Cross Scanner v2.2 | {now_str} (Iran Time)")
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
