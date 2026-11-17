import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
from prefect import flow, get_run_logger

RSI_PERIOD = 14
NOBITEX_BASE = "https://apiv2.nobitex.ir"
HEADERS = {"User-Agent": "TraderBot/RSI-Scanner-2.1"}

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))

GOLD_SYMBOL = "PAXGUSDT"
OTHER_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# (level, resolution, tf_label)
GOLD_LEVELS = [(32, "720", "12h"), (32, "360", "6h"),
               (68, "720", "12h"), (68, "360", "6h")]

OTHER_LEVELS = [(25, "720", "12h"), (25, "360", "6h"),
                (75, "720", "12h"), (75, "360", "6h"),
                (47, "720", "12h"), (47, "360", "6h")]


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

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=8)
            if resp.status_code == 429:
                time.sleep(2)
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


def find_crosses_last_12h(rsi_series, timestamps, level, seconds_per_candle):
    """
    بررسی کراس صعودی (prev < level <= curr) در ۱۲ ساعت گذشته.
    خروجی:
      - last_closed_cross_time: زمان آخرین کراس در کندل بسته‌شده (معتبر برای ورود)
      - pending_cross: آیا کراس فقط در کندل ناقص جاری رخ داده؟
      - curr_rsi / prev_rsi
      - cross_count: تعداد کراس‌ها در ۱۲ ساعت گذشته (فقط کندل‌های بسته‌شده)
    """
    if rsi_series is None or len(rsi_series) < 2:
        return None, False, None, None, 0

    cutoff = time.time() - 12 * 3600
    latest_closed_cross_time = None
    cross_count = 0
    pending_cross = False

    # آخرین کندل ناقص است (چون get_closes شامل کندل جاری است)
    last_idx = len(rsi_series) - 1

    for i in range(1, len(rsi_series)):
        ts = timestamps[i]
        if ts < cutoff:
            continue
        prev_rsi = float(rsi_series.iloc[i - 1])
        curr_rsi = float(rsi_series.iloc[i])
        if prev_rsi < level <= curr_rsi:
            if i == last_idx:
                pending_cross = True  # کراس فقط در کندل ناقص جاری
            else:
                cross_count += 1
                latest_closed_cross_time = datetime.fromtimestamp(ts, tz=IRAN_TZ).strftime("%Y-%m-%d %H:%M")

    curr_rsi = float(rsi_series.iloc[-1])
    prev_rsi = float(rsi_series.iloc[-2])
    return latest_closed_cross_time, pending_cross, curr_rsi, prev_rsi, cross_count


def format_check(level, tf_label, last_closed_cross, pending_cross, curr_rsi, cross_count):
    """قالب‌بندی یک ردیف بررسی کراس با وضعیت ورود"""
    if curr_rsi is None:
        return f"    cross {level} ({tf_label}): data unavailable\n"
    if last_closed_cross:
        return (f"    cross {level} ({tf_label}): ✅ آماده ورود (RSI {curr_rsi:.2f}) "
                f"| last closed cross: {last_closed_cross} | count(12h): {cross_count}\n")
    if pending_cross:
        return (f"    cross {level} ({tf_label}): ⏳ در انتظار بسته شدن کندل جاری "
                f"(RSI {curr_rsi:.2f}) | هنوز سیگنال قطعی نیست\n")
    return f"    cross {level} ({tf_label}): ❌ بدون کراس در ۱۲ ساعت گذشته (RSI {curr_rsi:.2f})\n"


def fetch_tf_data(symbols, resolution):
    """داده همه نمادها برای یک تایم‌فریم رو یک بار می‌گیره و RSI حساب می‌کنه"""
    data = {}
    for sym in symbols:
        closes, timestamps = get_closes(sym, resolution)
        if closes is None:
            data[sym] = (None, None)
            continue
        rsi = calculate_rsi(closes).dropna()
        valid_ts = timestamps[-len(rsi):]
        data[sym] = (rsi, valid_ts)
    return data


def emit_asset_block(logger, title, symbol, levels, data_12h, data_6h):
    """چاپ یک بلوک کامل برای یک دارایی"""
    logger.info(f"\n{title}")
    logger.info("-" * 70)

    # RSI لحظه‌ای از سری 12h (آخرین مقدار) برای نمایش کلی
    rsi_12h, _ = data_12h.get(symbol, (None, None))
    if rsi_12h is not None:
        logger.info(f"  RSI لحظه‌ای (سری 12h): {float(rsi_12h.iloc[-1]):.2f}")
    else:
        logger.info("  RSI لحظه‌ای: data unavailable")
    logger.info("")

    for level, resolution, tf_label in levels:
        source = data_12h if resolution == "720" else data_6h
        rsi, valid_ts = source.get(symbol, (None, None))
        if rsi is None:
            logger.info(f"    cross {level} ({tf_label}): data unavailable")
            continue
        seconds_per_candle = 720 if resolution == "720" else 360
        last_closed_cross, pending_cross, curr_rsi, prev_rsi, cross_count = find_crosses_last_12h(
            rsi, valid_ts, level, seconds_per_candle
        )
        logger.info(format_check(level, tf_label, last_closed_cross, pending_cross, curr_rsi, cross_count))


@flow(log_prints=True)
def run_scan():
    logger = get_run_logger()
    now_str = datetime.now(IRAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 70)
    logger.info(f"RSI Cross Scanner v2.1 | {now_str} (Iran Time)")
    logger.info("=" * 70)

    all_symbols = [GOLD_SYMBOL] + OTHER_SYMBOLS

    # داده ۱۲ ساعته و ۶ ساعته رو یک بار برای همه نمادها بگیر (۸ درخواست)
    data_12h = fetch_tf_data(all_symbols, "720")
    data_6h = fetch_tf_data(all_symbols, "360")

    # بلوک طلا
    emit_asset_block(logger, "[Block 1] PAXG (Gold)", GOLD_SYMBOL, GOLD_LEVELS, data_12h, data_6h)

    # بلوک‌های بیت‌کوین، اتریوم، سولانا
    for idx, sym in enumerate(OTHER_SYMBOLS, start=2):
        emit_asset_block(
            logger, f"[Block {idx}] {sym.replace('USDT', '')}", sym, OTHER_LEVELS, data_12h, data_6h
        )

    logger.info("\nScan completed.")


if __name__ == "__main__":
    run_scan()
