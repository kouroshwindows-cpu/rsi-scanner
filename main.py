from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))
UTC = timezone.utc

# ================= Config =================
# توجه: PAXG در Nobitex عمدتاً بازار IRT دارد. اگر PAXGUSDT وجود ندارد،
# این خط را به PAXGIRT تغییر دهید یا نماد را حذف کنید.
GOLD_SYMBOL = "PAXGUSDT"
OTHER_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

GOLD_LEVELS = [(30, "720", "12h"), (50, "720", "12h"), (70, "720", "12h")]
OTHER_LEVELS = [(30, "720", "12h"), (50, "720", "12h"), (70, "720", "12h")]

WINDOW_HOURS = 12
LEVEL_BUFFER = 0.0
NOBITEX_OHLC_URL = "https://apiv2.nobitex.ir/market/udf/history"


# ================= Time helpers =================
def candle_seconds(resolution: str) -> int:
    return int(resolution) * 60


def now_utc() -> datetime:
    return datetime.now(UTC)


def minutes_to_next_boundary(now: datetime, resolution: str) -> float:
    step = candle_seconds(resolution)
    epoch = now.timestamp()
    return (step - (epoch % step)) / 60.0


def warn_if_near_boundary(logger, now, resolution, tf_label):
    m = minutes_to_next_boundary(now, resolution)
    if m <= 5:
        logger.warning(
            f"WARNING {tf_label}: only {m:.1f} min to candle close. "
            f"Signal may be on a borderline candle."
        )


# ================= Nobitex data =================
def get_closes(symbol: str, resolution: str):
    """
    Returns (closes: pd.Series, timestamps: pd.Series) or (None, None).
    Only closed candles are returned.
    """
    step = candle_seconds(resolution)
    now_ts = int(now_utc().timestamp())
    from_ts = now_ts - (step * 60)

    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": from_ts,
        "to": now_ts,
    }
    try:
        r = requests.get(NOBITEX_OHLC_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None, None

    if data.get("s") != "ok" or not data.get("t"):
        return None, None

    closed_idx = [i for i, open_ts in enumerate(data["t"]) if open_ts + step <= now_ts]
    if len(closed_idx) < 20:
        return None, None

    closes = pd.Series([float(data["c"][i]) for i in closed_idx])
    timestamps = pd.Series([pd.to_datetime(data["t"][i], unit="s", utc=True) for i in closed_idx])
    return closes, timestamps


def calculate_rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    """
    Wilder 標準 RSI：
    1) دوره اول از SMA به عنوان مقدار اولیه میانگین سود/زیان استفاده می‌کند
    2) سپس هموارسازی بازگشتی Wilder اعمال می‌شود
    نتیجه با RSI استاندارد TradingView سازگار است.
    """
    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = [None] * len(closes)
    avg_loss = [None] * len(closes)

    # مقدار اولیه: میانگین ساده period دوره اول
    avg_gain[period] = gain.iloc[1:period + 1].mean()
    avg_loss[period] = loss.iloc[1:period + 1].mean()

    # هموارسازی بازگشتی Wilder
    for i in range(period + 1, len(closes)):
        avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gain.iloc[i]) / period
        avg_loss[i] = (avg_loss[i - 1] * (period - 1) + loss.iloc[i]) / period

    avg_gain = pd.Series(avg_gain, index=closes.index)
    avg_loss = pd.Series(avg_loss, index=closes.index)

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def build_rsi_frame(closes, timestamps):
    df = pd.DataFrame({"timestamp": timestamps, "close": closes.values})
    df["rsi"] = calculate_rsi(df["close"])
    return df.dropna(subset=["rsi"]).reset_index(drop=True)


def fetch_tf_data(logger, symbols, resolution):
    data = {}
    for sym in symbols:
        closes, timestamps = get_closes(sym, resolution)
        if closes is None:
            logger.error(f"ERROR data unavailable for {sym} ({resolution})")
            data[sym] = None
            continue
        data[sym] = build_rsi_frame(closes, timestamps)
    return data


# ================= Cross logic (closed candles only, bullish only) =================
def find_crosses_last_12h(df, level, now, window_hours=WINDOW_HOURS, level_buffer=LEVEL_BUFFER):
    if df is None or df.empty or len(df) < 2:
        return None, False, None, None, 0

    window_start = now - timedelta(hours=window_hours)
    in_window = df[df["timestamp"] >= window_start].reset_index(drop=True)

    crosses = []
    for i in range(1, len(in_window)):
        prev_rsi = in_window.loc[i - 1, "rsi"]
        curr_rsi = in_window.loc[i, "rsi"]
        if prev_rsi < (level - level_buffer) and curr_rsi >= (level + level_buffer):
            crosses.append(in_window.loc[i, "timestamp"])

    prev_rsi = float(df.iloc[-2]["rsi"])
    curr_rsi = float(df.iloc[-1]["rsi"])
    last_ts = df.iloc[-1]["timestamp"]

    pending_cross = prev_rsi < (level - level_buffer) and curr_rsi >= (level + level_buffer)
    last_closed_cross = last_ts if pending_cross else (crosses[-1] if crosses else None)

    return last_closed_cross, pending_cross, curr_rsi, prev_rsi, len(crosses)


# ================= Output =================
def format_check(level, tf_label, last_closed_cross, pending_cross, curr_rsi, prev_rsi, cross_count):
    if curr_rsi is None:
        return f"    cross {level} ({tf_label}): data unavailable\n"

    rsi_txt = f"RSI {curr_rsi:.2f} (prev {prev_rsi:.2f})"
    if pending_cross and last_closed_cross is not None:
        return (
            f"    cross {level} ({tf_label}): READY - fresh cross on closed candle "
            f"({rsi_txt}) | closed cross: {last_closed_cross} | count(12h): {cross_count}\n"
        )
    if last_closed_cross is not None:
        return (
            f"    cross {level} ({tf_label}): IN WINDOW - no fresh signal "
            f"({rsi_txt}) | last closed cross: {last_closed_cross} | count(12h): {cross_count}\n"
        )
    return f"    cross {level} ({tf_label}): NO bullish cross in last 12h ({rsi_txt})\n"


def emit_asset_block(logger, title, symbol, levels, data_by_res, now):
    logger.info(f"\n{title}")
    logger.info("-" * 70)

    df12 = data_by_res.get("720", {}).get(symbol)
    if df12 is not None and not df12.empty:
        logger.info(f"  Instant RSI (last closed 12h candle): {float(df12.iloc[-1]['rsi']):.2f}")
    else:
        logger.info("  Instant RSI: data unavailable")
    logger.info("")

    for level, resolution, tf_label in levels:
        df = data_by_res.get(resolution, {}).get(symbol)
        if df is None:
            logger.info(f"    cross {level} ({tf_label}): data unavailable")
            continue

        warn_if_near_boundary(logger, now, resolution, tf_label)
        last_closed_cross, pending_cross, curr_rsi, prev_rsi, cross_count = find_crosses_last_12h(
            df, level, now
        )
        logger.info(format_check(level, tf_label, last_closed_cross, pending_cross,
                                 curr_rsi, prev_rsi, cross_count))


# ================= Run =================
def run_scan():
    logger = get_run_logger()
    now = now_utc()
    logger.info("=" * 70)
    logger.info(f"RSI Cross Scanner v2.3 | {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info(f"Window: {WINDOW_HOURS}h | closed candles only | bullish only | Wilder RSI")
    logger.info("=" * 70)

    all_symbols = [GOLD_SYMBOL] + OTHER_SYMBOLS
    data_by_res = {
        "720": fetch_tf_data(logger, all_symbols, "720"),
        "360": fetch_tf_data(logger, all_symbols, "360"),
    }

    emit_asset_block(logger, "[Block 1] PAXG (Gold)", GOLD_SYMBOL, GOLD_LEVELS, data_by_res, now)

    for idx, sym in enumerate(OTHER_SYMBOLS, start=2):
        emit_asset_block(
            logger, f"[Block {idx}] {sym.replace('USDT', '')}",
            sym, OTHER_LEVELS, data_by_res, now
        )

    logger.info("\nScan completed.")


if __name__ == "__main__":
    run_scan()
