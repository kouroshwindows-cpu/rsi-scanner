import requests
import pandas as pd
import time
from datetime import datetime
from prefect import flow, get_run_logger

RSI_PERIOD = 14
RSI_LEVEL = 47
RSI_LEVEL_30 = 30
NOBITEX_BASE = "https://apiv2.nobitex.ir"
HEADERS = {"User-Agent": "TraderBot/RSI-Scanner-1.1"}

SYMBOLS = ["PAXGUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "GRAMUSDT"]

def get_closes(symbol, resolution, limit=200):
    url = f"{NOBITEX_BASE}/market/udf/history"
    now = int(time.time())
    seconds_per_candle = 6 * 3600 if resolution == "360" else 12 * 3600
    aligned_now = (now // seconds_per_candle) * seconds_per_candle
    start = aligned_now - (limit - 1) * seconds_per_candle
    params = {"symbol": symbol, "resolution": resolution, "from": start, "to": now, "countback": limit}
    
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
            if resp.status_code == 429:
                time.sleep(10)
                continue
            data = resp.json()
            if data.get("s") != "ok":
                return None
            closes = data.get("c", [])
            if not closes or len(closes) < RSI_PERIOD + 3:
                return None
            return pd.Series([float(c) for c in closes])
        except Exception:
            time.sleep(2)
    return None

def calculate_rsi(closes, period=14):
    closes = pd.Series(closes).astype(float)
    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1.0/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.where(avg_loss != 0, 100.0)
    return rsi

def check_cross_from_series(rsi_series, level):
    """بررسی کراس از سری RSI موجود (بدون درخواست مجدد)"""
    if rsi_series is None or len(rsi_series) < 2:
        return None, None, None
    prev_rsi = float(rsi_series.iloc[-2])
    curr_rsi = float(rsi_series.iloc[-1])
    crossed = prev_rsi < level <= curr_rsi
    return crossed, curr_rsi, prev_rsi

@flow(log_prints=True)
def run_scan():
    logger = get_run_logger()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"RSI Cross Scanner | {now_str}")

    # پیش‌واکشی داده‌ها: هر ارز یک بار 12H و یک بار 6H
    data_12h = {}
    data_6h = {}
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        closes_12h = get_closes(sym, "720")
        if closes_12h is not None:
            data_12h[sym] = calculate_rsi(closes_12h, RSI_PERIOD).dropna()
        else:
            data_12h[sym] = None
        
        closes_6h = get_closes(sym, "360")
        if closes_6h is not None:
            data_6h[sym] = calculate_rsi(closes_6h, RSI_PERIOD).dropna()
        else:
            data_6h[sym] = None

    # بلوک ۱: 12H RSI > 47
    logger.info("\n[Block 1] 12H RSI crossed above 47")
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        crossed, curr, prev = check_cross_from_series(data_12h.get(sym), RSI_LEVEL)
        if crossed is None:
            logger.info(f"  {base:<6} | data unavailable")
        elif crossed:
            logger.info(f"  {base:<6} | RSI: {curr:6.2f} (prev: {prev:5.2f}) | ✅ CONFIRMED")
        else:
            logger.info(f"  {base:<6} | RSI: {curr:6.2f} | ❌ not crossed")

    # بلوک ۲ و ۳: از داده‌های 6H مشترک استفاده می‌کنند
    logger.info("\n[Block 2] 6H RSI crossed above 47")
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        crossed, curr, prev = check_cross_from_series(data_6h.get(sym), RSI_LEVEL)
        if crossed is None:
            logger.info(f"  {base:<6} | data unavailable")
        elif crossed:
            logger.info(f"  {base:<6} | RSI: {curr:6.2f} (prev: {prev:5.2f}) | ✅ CONFIRMED")
        else:
            logger.info(f"  {base:<6} | RSI: {curr:6.2f} | ❌ not crossed")

    logger.info("\n[Block 3] 6H RSI crossed above 30")
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        crossed, curr, prev = check_cross_from_series(data_6h.get(sym), RSI_LEVEL_30)
        if crossed is None:
            logger.info(f"  {base:<6} | data unavailable")
        elif crossed:
            logger.info(f"  {base:<6} | RSI: {curr:6.2f} (prev: {prev:5.2f}) | ✅ CONFIRMED")
        else:
            logger.info(f"  {base:<6} | RSI: {curr:6.2f} | ❌ not crossed")

    logger.info("\nScan completed.")

if __name__ == "__main__":
    run_scan()
