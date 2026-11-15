import requests
import pandas as pd
import time
from datetime import datetime

# ========== تنظیمات ==========
RSI_PERIOD = 14
RSI_LEVEL = 47
RSI_LEVEL_30 = 30
NOBITEX_BASE = "https://apiv2.nobitex.ir"
HEADERS = {"User-Agent": "TraderBot/RSI-Scanner-1.0"}

# 6 ارز هدف (بازار USDT)
SYMBOLS = [
    "PAXGUSDT",
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "GRAMUSDT",
]

# نام‌های نمایشی
COIN_NAMES = {
    "PAXG": "PAXG",
    "BTC": "BTC",
    "ETH": "ETH",
    "SOL": "SOL",
    "XRP": "XRP",
    "GRAM": "GRAM",
}


def get_closes(symbol, resolution, limit=200):
    """دریافت قیمت‌های پایانی از API نوبیتکس"""
    url = f"{NOBITEX_BASE}/market/udf/history"
    now = int(time.time())

    if resolution == "360":
        seconds_per_candle = 6 * 3600
    elif resolution == "720":
        seconds_per_candle = 12 * 3600
    else:
        seconds_per_candle = 86400

    aligned_now = (now // seconds_per_candle) * seconds_per_candle
    start = aligned_now - (limit - 1) * seconds_per_candle

    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": start,
        "to": now,
        "countback": limit,
    }

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
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
    """محاسبه RSI با روش Wilder (سازگار با TradingView)"""
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


def check_cross(symbol, resolution, level):
    """
    بررسی اینکه آیا RSI از پایین به بالای سطح مشخصی عبور کرده است
    بازگشت: (crossed: bool, current_rsi: float, prev_rsi: float)
    """
    closes = get_closes(symbol, resolution)
    if closes is None:
        return None, None, None

    rsi_series = calculate_rsi(closes, RSI_PERIOD).dropna()
    if len(rsi_series) < 2:
        return None, None, None

    prev_rsi = float(rsi_series.iloc[-2])
    curr_rsi = float(rsi_series.iloc[-1])

    # کراس به سمت بالا: RSI قبلی زیر سطح بوده و RSI فعلی بالای سطح است
    crossed = prev_rsi < level <= curr_rsi

    return crossed, curr_rsi, prev_rsi


def run_scan():
    """تابع ورودی برای Prefect، خروجی چاپ می‌شود و در داشبورد Prefect قابل مشاهده است"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{'=' * 60}")
    print(f"RSI Cross Scanner | {now_str}")
    print(f"{'=' * 60}\n")

    # بلوک ۱: 12H RSI کراس 47
    print("[Block 1] 12H RSI crossed above 47")
    print("-" * 50)
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        name = COIN_NAMES.get(base, base)
        crossed, curr, prev = check_cross(sym, "720", RSI_LEVEL)
        if crossed is None:
            print(f"  {name:<6} | data unavailable")
        elif crossed:
            print(f"  {name:<6} | RSI: {curr:6.2f} (prev: {prev:5.2f}) | ✅ CONFIRMED")
        else:
            print(f"  {name:<6} | RSI: {curr:6.2f} | ❌ not crossed")
    print()

    # بلوک ۲: 6H RSI کراس 47
    print("[Block 2] 6H RSI crossed above 47")
    print("-" * 50)
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        name = COIN_NAMES.get(base, base)
        crossed, curr, prev = check_cross(sym, "360", RSI_LEVEL)
        if crossed is None:
            print(f"  {name:<6} | data unavailable")
        elif crossed:
            print(f"  {name:<6} | RSI: {curr:6.2f} (prev: {prev:5.2f}) | ✅ CONFIRMED")
        else:
            print(f"  {name:<6} | RSI: {curr:6.2f} | ❌ not crossed")
    print()

    # بلوک ۳: 6H RSI کراس 30
    print("[Block 3] 6H RSI crossed above 30")
    print("-" * 50)
    for sym in SYMBOLS:
        base = sym.replace("USDT", "")
        name = COIN_NAMES.get(base, base)
        crossed, curr, prev = check_cross(sym, "360", RSI_LEVEL_30)
        if crossed is None:
            print(f"  {name:<6} | data unavailable")
        elif crossed:
            print(f"  {name:<6} | RSI: {curr:6.2f} (prev: {prev:5.2f}) | ✅ CONFIRMED")
        else:
            print(f"  {name:<6} | RSI: {curr:6.2f} | ❌ not crossed")
    print()

    print("=" * 60)
    print("Scan completed.")


if __name__ == "__main__":
    run_scan()