import requests
import pandas as pd
import time
from datetime import datetime
from prefect import flow, get_run_logger

RSI_PERIOD = 14
RSI_LEVEL = 47
RSI_LEVEL_30 = 30
NOBITEX_BASE = "https://apiv2.nobitex.ir"
HEADERS = {"User-Agent": "TraderBot/RSI-Scanner-1.3"}

SYMBOLS = ["PAXGUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "GRAMUSDT"]


def get_closes(symbol, resolution, limit=500):
    """دریافت closeها و timestampهای کندل از نوبیتکس"""
    url = f"{NOBITEX_BASE}/market/udf/history"
    now = int(time.time())
    seconds_per_candle = 360 if resolution == "360" else 720
    aligned_now = (now // seconds_per_candle) * seconds_per_candle
    start = aligned_now - (limit - 1) * seconds_per_candle

    params = {
        "symbol": symbol, "resolution": resolution,
        "from": start, "to": now, "countback": limit,
    }

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
            if resp.status_code == 429:
                time.sleep(10)
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
            time.sleep(2)
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


def analyze_cross_in_last_12h(rsi_series, timestamps, level):
    """
    بررسی می‌کند آیا در 12 ساعت گذشته کراس از پایین به بالای سطح رخ داده.
    برمی‌گرداند: (crossed: bool, latest_time: str or None, curr_rsi: float)
    """
    if rsi_series is None or len(rsi_series) < 2:
        return None, None, None

    cutoff = time.time() - 12 * 3600
    latest_cross_time = None

    for i in range(1, len(rsi_series)):
        if timestamps[i] < cutoff:
            continue
        prev_rsi = float(rsi_series.iloc[i - 1])
        curr_rsi = float(rsi_series.iloc[i])
        if prev_rsi < level <= curr_rsi:
            latest_cross_time = datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d %H:%M")

    curr_rsi = float(rsi_series.iloc[-1])
    crossed = latest_cross_time is not None
    return crossed, latest_cross_time, curr_rsi


def format_line(base, crossed, curr_rsi, cross_time):
    """قالب‌بندی یک خط خروجی"""
    if crossed is None:
        return f"  {base:<6} | data unavailable\n"
    if crossed:
        return (f"  {base:<6} | RSI: {curr_rsi:6.2f} | "
                f"✅ crossed {cross_time}\n")
    return f"  {base:<6} | RSI: {curr_rsi:6.2f} | ❌ not crossed\n"


def emit_block(logger, title, resolution, level):
    """چاپ یک بلوک کامل"""
    logger.info(f"\n{title}")
    logger.info("-" * 55)
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        closes, timestamps = get_closes(sym, resolution)
        if closes is None:
            logger.info(f"  {base:<6} | data unavailable")
            continue
        rsi = calculate_rsi(closes).dropna()
        valid_ts = timestamps[-len(rsi):]
        crossed, cross_time, curr_rsi = analyze_cross_in_last_12h(rsi, valid_ts, level)
        logger.info(format_line(base, crossed, curr_rsi, cross_time))


@flow(log_prints=True)
def run_scan():
    logger = get_run_logger()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 55)
    logger.info(f"RSI Cross Scanner | {now_str}")
    logger.info("=" * 55)

    # بلوک ۱: 12H RSI > 47
    emit_block(logger, "[Block 1] 12H RSI crossed above 47 (last 12h)", "720", RSI_LEVEL)

    # بلوک ۲: 12H RSI > 30
    emit_block(logger, "[Block 2] 12H RSI crossed above 30 (last 12h)", "720", RSI_LEVEL_30)

    # بلوک ۳: 6H RSI > 47
    emit_block(logger, "[Block 3] 6H RSI crossed above 47 (last 12h)", "360", RSI_LEVEL)

    # بلوک ۴: 6H RSI > 30
    emit_block(logger, "[Block 4] 6H RSI crossed above 30 (last 12h)", "360", RSI_LEVEL_30)

    logger.info("\nScan completed.")


if __name__ == "__main__":
    run_scan()
