import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
from prefect import flow, get_run_logger

RSI_PERIOD = 14
RSI_LEVEL = 47
RSI_LEVEL_30 = 30
NOBITEX_BASE = "https://apiv2.nobitex.ir"
HEADERS = {"User-Agent": "TraderBot/RSI-Scanner-1.4"}

SYMBOLS = ["PAXGUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "GRAMUSDT"]

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))


def get_closes(symbol, resolution, limit=500):
    """دریافت closeها و timestampهای کندل از نوبیتکس (شامل کندل ناقص جاری)"""
    url = f"{NOBITEX_BASE}/market/udf/history"
    now = int(time.time())
    seconds_per_candle = 360 if resolution == "360" else 720
    aligned_now = (now // seconds_per_candle) * seconds_per_candle
    start = aligned_now - (limit - 1) * seconds_per_candle

    params = {
        "symbol": symbol, "resolution": resolution,
        "from": start, "to": now, "countback": limit,
    }

    for attempt in range(2):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=8)
            if resp.status_code == 429:
                time.sleep(3)
                continue
            data = resp.json()
            if data.get("s") != "ok":
                return None, None
            closes = data.get("c", [])
            timestamps = data.get("t", [])
            if not closes or len(closes) < RSI_PERIOD + 3:
                return None, None
            return pd.Series([float(c) for c in closes]), timestamps
        except Exception:
            time.sleep(1)
    return None, None


def calculate_rsi(closes, period=14):
    """RSI استاندارد Wilder - مطابق TradingView"""
    closes = pd.Series(closes).astype(float)
    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.where(avg_loss != 0, 100.0)
    return rsi


def find_crosses_last_12h(rsi_series, timestamps, level):
    """همه کراس‌های صعودی از سطح در ۱۲ ساعت گذشته (شامل کندل ناقص جاری)"""
    if rsi_series is None or len(rsi_series) < 2:
        return None, None, None, None, 0

    cutoff = time.time() - 12 * 3600
    latest_cross_time = None
    cross_count = 0

    for i in range(1, len(rsi_series)):
        ts = timestamps[i]
        if ts < cutoff:
            continue
        prev_rsi = float(rsi_series.iloc[i - 1])
        curr_rsi = float(rsi_series.iloc[i])
        if prev_rsi < level <= curr_rsi:
            cross_count += 1
            latest_cross_time = datetime.fromtimestamp(ts, tz=IRAN_TZ).strftime("%Y-%m-%d %H:%M")

    curr_rsi = float(rsi_series.iloc[-1])
    prev_rsi = float(rsi_series.iloc[-2])
    crossed = cross_count > 0
    return crossed, latest_cross_time, curr_rsi, prev_rsi, cross_count


def format_line(base, crossed, curr_rsi, prev_rsi, cross_time, cross_count):
    """قالب‌بندی یک خط خروجی مثل قبل"""
    if crossed is None:
        return f"  {base:<6} | data unavailable\n"
    if crossed:
        return (f"  {base:<6} | RSI: {curr_rsi:6.2f} (prev {prev_rsi:6.2f}) | "
                f"✅ {cross_count} cross(es) in last 12h, latest: {cross_time}\n")
    return (f"  {base:<6} | RSI: {curr_rsi:6.2f} (prev {prev_rsi:6.2f}) | "
            f"❌ no cross in last 12h\n")


def fetch_tf_data(resolution):
    """داده همه نمادها برای یک تایم‌فریم رو یک بار می‌گیره و RSI حساب می‌کنه"""
    data = {}
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        closes, timestamps = get_closes(sym, resolution)
        if closes is None:
            data[sym] = (base, None, None)
            continue
        rsi = calculate_rsi(closes).dropna()
        valid_ts = timestamps[-len(rsi):]
        data[sym] = (base, rsi, valid_ts)
    return data


def emit_block(logger, title, tf_data, level):
    """چاپ یک بلوک کامل از داده‌ای که قبلاً گرفته شده"""
    logger.info(f"\n{title}")
    logger.info("-" * 55)
    for sym in SYMBOLS:
        base, rsi, valid_ts = tf_data[sym]
        if rsi is None:
            logger.info(f"  {base:<6} | data unavailable")
            continue
        crossed, cross_time, curr_rsi, prev_rsi, cross_count = find_crosses_last_12h(
            rsi, valid_ts, level
        )
        logger.info(format_line(base, crossed, curr_rsi, prev_rsi, cross_time, cross_count))


@flow(log_prints=True)
def run_scan():
    logger = get_run_logger()
    now_str = datetime.now(IRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 55)
    logger.info(f"RSI Cross Scanner | {now_str} (Iran Time)")
    logger.info("=" * 55)

    # داده ۱۲ ساعته رو یک بار بگیر
    data_12h = fetch_tf_data("720")
    # داده ۶ ساعته رو یک بار بگیر
    data_6h = fetch_tf_data("360")

    # حالا ۴ بلوک جدا مثل قبل چاپ کن (بدون درخواست اضافه)
    emit_block(logger, "[Block 1] 12H RSI crossed above 47 (last 12h, may be unclosed)", data_12h, RSI_LEVEL)
    emit_block(logger, "[Block 2] 12H RSI crossed above 30 (last 12h, may be unclosed)", data_12h, RSI_LEVEL_30)
    emit_block(logger, "[Block 3] 6H RSI crossed above 47 (last 12h, may be unclosed)", data_6h, RSI_LEVEL)
    emit_block(logger, "[Block 4] 6H RSI crossed above 30 (last 12h, may be unclosed)", data_6h, RSI_LEVEL_30)

    logger.info("\nScan completed.")


if __name__ == "__main__":
    run_scan()
