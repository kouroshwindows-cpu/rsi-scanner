import requests
import pandas as pd
import time
import json
import os
from datetime import datetime, timedelta
from prefect import flow, get_run_logger

RSI_PERIOD = 14
RSI_LEVEL = 47
RSI_LEVEL_30 = 30
NOBITEX_BASE = "https://apiv2.nobitex.ir"
HEADERS = {"User-Agent": "TraderBot/RSI-Scanner-1.2"}
STATE_FILE = "/tmp/rsi_state.json"  # Prefect اجراها ممکن است فایل را از دست بدهند، بنابراین فقط برای مرجع است

SYMBOLS = ["PAXGUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "GRAMUSDT"]


def get_closes(symbol, resolution, limit=500):
    """دریافت داده‌های کندل، حداکثر 500 کندل در هر درخواست"""
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


def find_recent_crosses(rsi_series, timestamps, level, hours_back=12):
    """یافتن همه کراس‌های اخیر از پایین به بالا در بازه hours_back ساعت گذشته"""
    if rsi_series is None or len(rsi_series) < 2:
        return []
    
    cutoff = time.time() - hours_back * 3600
    crosses = []
    
    for i in range(1, len(rsi_series)):
        if timestamps[i] < cutoff:
            continue
        prev_rsi = float(rsi_series.iloc[i-1])
        curr_rsi = float(rsi_series.iloc[i])
        if prev_rsi < level <= curr_rsi:
            candle_time = datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%d %H:%M")
            crosses.append({
                "time": candle_time,
                "prev_rsi": round(prev_rsi, 2),
                "curr_rsi": round(curr_rsi, 2),
                "level": level
            })
    return crosses


@flow(log_prints=True)
def run_scan():
    logger = get_run_logger()
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"RSI Cross Scanner (12h lookback) | {now_str}")

    # خواندن وضعیت قبلی (برای تشخیص کراس جدید)
    prev_state = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                prev_state = json.load(f)
        except:
            prev_state = {}

    current_state = {}

    for tf_name, resolution in [("12H", "720"), ("6H", "360")]:
        logger.info(f"\n{'='*50}")
        logger.info(f"[{tf_name}] RSI crosses in last 12 hours")
        logger.info(f"{'='*50}")

        for sym in SYMBOLS:
            base = sym.replace("USDT", "")
            closes, timestamps = get_closes(sym, resolution)
            if closes is None:
                logger.info(f"  {base:<6} | data unavailable")
                continue

            rsi = calculate_rsi(closes).dropna()
            # همترازی timestamps با rsi (dropna باعث حذف می‌شود)
            valid_ts = timestamps[-len(rsi):]

            for level, level_name in [(RSI_LEVEL, "47"), (RSI_LEVEL_30, "30")]:
                crosses = find_recent_crosses(rsi, valid_ts, level)
                state_key = f"{tf_name}_{base}_{level_name}"
                
                if crosses:
                    # بررسی اینکه آیا کراس جدیدی نسبت به اجرای قبلی وجود دارد
                    latest_cross_time = crosses[-1]["time"]
                    prev_latest = prev_state.get(state_key, {}).get("latest_time")
                    
                    is_new = (prev_latest != latest_cross_time)
                    
                    for c in crosses:
                        marker = "🆕 " if (is_new and c["time"] == latest_cross_time) else "   "
                        logger.info(f"  {marker}{base:<6} | {tf_name} RSI crossed above {level_name} | "
                                   f"candle: {c['time']} | RSI: {c['prev_rsi']} -> {c['curr_rsi']}")
                    
                    current_state[state_key] = {"latest_time": latest_cross_time}
                else:
                    logger.info(f"     {base:<6} | {tf_name} RSI>={level_name}: no cross in last 12h")

    # ذخیره وضعیت
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(current_state, f)
    except:
        pass  # Prefect محیط موقت است، ممکن است نتواند بنویسد

    logger.info("\nScan completed.")


if __name__ == "__main__":
    run_scan()
