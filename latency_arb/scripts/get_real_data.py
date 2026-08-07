#!/usr/bin/env python3
"""
get_real_data.py — download real historical klines from Binance's public API
and format them exactly as the C++ tick_replay --convert-csv expects:

    timestamp,bid,ask,volume

Binance does not expose public tick-level bid/ask history via the free REST
API, so the script derives the quote from the kline close with a simulated
tight spread:

    bid = close - spread_half
    ask = close + spread_half

The kline timestamp (open time, epoch ms) is used directly; volume is the
base-asset traded volume of that kline.

Endpoints tried, in order:
  1. https://data-api.binance.vision  (public market-data endpoint, no key)
  2. https://api.binance.com          (production REST, no key for klines)

Note on 1s data retention: Binance REST only serves recent 1s klines (roughly
the last 30 days for small intervals). For older 1s history you must pull the
monthly zips from https://data.binance.vision (see Usage below).

Usage:
    python3 get_real_data.py --symbol BTCUSDT --interval 1s \
        --start-date 2025-01-01 --end-date 2025-01-02 \
        --out real_lead.csv

Optional flags:
    --spread-half 0.5      half-spread in price units applied to the close
    --lag-shift-bars N     also write <out>.lag.csv shifted N bars later
                           (simulates a retail feed lagging the lead feed)
    --limit-bars M         stop after M klines (quick smoke test)
    --sleep-ms 150         pause between paginated requests (rate limiting)

Older 1s data from data.binance.vision (monthly/daily zips):
    binance-klines-download --symbol BTCUSDT --interval 1s --date 2024-03
    (or fetch https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1s/
     BTCUSDT-1s-2024-03.zip and repoint --csv-path below)

The klines API pagination stops when it reaches --end-date or Binance returns
no more rows, so requesting a range longer than the retention window simply
returns the newest available segment.

Requires: Python 3.8+ (standard library only).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_BASE_URLS = (
    "https://data-api.binance.vision",
    "https://api.binance.com",
)
API_KLINES = "/api/v3/klines"
MAX_LIMIT = 1000

# (seconds since epoch) -> ISO-ish date helpers ---------------------------------
def parse_date(s: str) -> int:
    """'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' (UTC) -> epoch ms start of period."""
    fmt = "%Y-%m-%d %H:%M:%S" if " " in s else "%Y-%m-%d"
    t = dt.datetime.strptime(s, fmt).replace(tzinfo=dt.timezone.utc)
    return int(t.timestamp() * 1000)


def http_get_json(url: str, timeout: int = 30):
    req = urllib.request.Request(url, headers={"User-Agent": "tick-replay/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_klines(base_url: str, symbol: str, interval: str,
                 start_ms: int, end_ms: int, limit: int):
    """One paginated request; returns the raw kline rows (list of lists)."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": str(start_ms),
        "endTime": str(end_ms),
        "limit": str(limit),
    }
    url = base_url + API_KLINES + "?" + urllib.parse.urlencode(params)
    return http_get_json(url)


def interval_ms(interval: str) -> int:
    """'1s'/'1m'/'1h'/'1d' -> ms. Binance 1s is exactly 1000 ms."""
    unit = interval[-1]
    mult = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}.get(unit)
    if not mult:
        raise ValueError(f"unsupported interval unit in '{interval}'")
    try:
        num = int(interval[:-1])
    except ValueError:
        raise ValueError(f"bad interval '{interval}'") from None
    return num * mult


def download(symbol: str, interval: str, start_ms: int, end_ms: int,
             sleep_ms: int, limit_bars: int | None) -> list[dict]:
    """Paginate klines between start_ms..end_ms (UTC), oldest first.

    Returns a list of dicts with keys: ts_ms, open, high, low, close, volume.
    """
    step = interval_ms(interval)
    rows: list[dict] = []
    cursor = start_ms
    last_err = None

    while True:
        limit = limit_bars if limit_bars else MAX_LIMIT
        if limit_bars:
            remaining = limit_bars - len(rows)
            if remaining <= 0:
                break
            limit = min(MAX_LIMIT, remaining)

        data = None
        for base in DEFAULT_BASE_URLS:
            try:
                data = fetch_klines(base, symbol, interval, cursor, end_ms, limit)
                break
            except (urllib.error.URLError, urllib.error.HTTPError) as e:
                last_err = e
                continue
        if data is None:
            raise RuntimeError(
                f"all Binance endpoints failed (last: {last_err}); check network / "
                f"symbol '{symbol}' / interval '{interval}'")

        if not data:
            break  # past the retained window

        for k in data:
            # Binance kline row:
            # [open_time, open, high, low, close, volume, close_time,
            #  quote_volume, trades, taker_buy_base, taker_buy_quote, ignore]
            rows.append({
                "ts_ms": int(k[0]),
                "open":  float(k[1]),
                "high":  float(k[2]),
                "low":   float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            })

        last_ts = rows[-1]["ts_ms"]
        if last_ts + step > end_ms or (limit_bars and len(rows) >= limit_bars):
            break
        cursor = last_ts + step      # next bucket, no overlap
        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)

    return rows


def write_csv(path: Path, rows: list[dict], spread_half: float) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "bid", "ask", "volume"])
        for r in rows:
            w.writerow([
                r["ts_ms"],
                f"{r['close'] - spread_half:.6f}",
                f"{r['close'] + spread_half:.6f}",
                f"{r['volume']:.8f}",
            ])


def main(argv) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--interval", default="1s")
    p.add_argument("--start-date", required=True,
                   help="UTC 'YYYY-MM-DD' (or with 'HH:MM:SS')")
    p.add_argument("--end-date", default=None,
                   help="UTC 'YYYY-MM-DD'; defaults to now")
    p.add_argument("--out", default="real_lead.csv")
    p.add_argument("--spread-half", type=float, default=0.5)
    p.add_argument("--lag-shift-bars", type=int, default=0,
                   help=">0: also write <out>.lag.csv shifted N bars later")
    p.add_argument("--limit-bars", type=int, default=None)
    p.add_argument("--sleep-ms", type=int, default=150)
    a = p.parse_args(argv)

    end_ms = parse_date(a.end_date) if a.end_date else int(time.time() * 1000)
    start_ms = parse_date(a.start_date)
    if start_ms >= end_ms:
        print(f"error: start {a.start_date} is not before end {a.end_date}")
        return 2

    print(f"downloading {a.symbol} {a.interval} "
          f"[{a.start_date} .. {a.end_date or 'now'}] ...")
    rows = download(a.symbol, a.interval, start_ms, end_ms,
                    a.sleep_ms, a.limit_bars)
    if not rows:
        print("no klines returned for this range (possibly outside Binance's "
              "retention window); try a more recent start-date")
        return 1

    out = Path(a.out)
    write_csv(out, rows, a.spread_half)
    print(f"wrote {out} : {len(rows)} ticks "
          f"({rows[0]['ts_ms']}..{rows[-1]['ts_ms']} ms)")

    if a.lag_shift_bars > 0:
        lag_path = Path(str(out) + ".lag.csv")
        # Simulate a genuine retail feed that lags the lead: same timestamps,
        # but each bar's quote is the lead's value observed `shift` bars
        # earlier (stale prices), so the gap is real, not zero.
        sh = min(a.lag_shift_bars, len(rows) - 1)
        lag_rows = []
        for i in range(len(rows)):
            src = rows[i - sh]
            lag_rows.append({
                "ts_ms": rows[i]["ts_ms"],
                "close": src["close"],
                "volume": src["volume"],
            })
        write_csv(lag_path, lag_rows, a.spread_half)
        print(f"wrote {lag_path} : {len(lag_rows)} ticks "
              f"(lead shifted {sh} bars later, real lag)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
