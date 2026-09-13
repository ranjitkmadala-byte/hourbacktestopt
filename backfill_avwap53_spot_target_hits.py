from __future__ import annotations

import os
import time
import uuid
from datetime import date, datetime, time as dtime
from urllib.parse import quote
from zoneinfo import ZoneInfo

import psycopg
import requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

IST = ZoneInfo("Asia/Kolkata")

DB = (os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
TOKEN = (os.getenv("UPSTOX_ACCESS_TOKEN") or os.getenv("UPSTOX_TOKEN") or "").strip()

START = date.fromisoformat(os.getenv("STUDY_START", "2026-08-26"))
END = date.fromisoformat(os.getenv("STUDY_END", "2026-09-11"))
EXPECTED = int(os.getenv("EXPECTED_ENTRIES", "53"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "0.50"))
RUN_ID = str(uuid.uuid4())

HIST = "https://api.upstox.com/v3/historical-candle"

def log(msg):
    print(f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} IST | {msg}", flush=True)

def db():
    return psycopg.connect(DB, row_factory=dict_row, connect_timeout=20)

def headers():
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }

DDL = """
CREATE TABLE IF NOT EXISTS public.avwap53_spot_target_hits (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,

    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_instrument_key TEXT NOT NULL,

    entry_time TIMESTAMPTZ NOT NULL,
    entry_price NUMERIC NOT NULL,
    target_pct NUMERIC NOT NULL,
    target_price NUMERIC NOT NULL,

    target_hit BOOLEAN NOT NULL,
    target_hit_time TIMESTAMPTZ,
    target_hit_bar_start TIMESTAMPTZ,
    target_hit_bar_end TIMESTAMPTZ,
    target_hit_bar_open NUMERIC,
    target_hit_bar_high NUMERIC,
    target_hit_bar_low NUMERIC,
    target_hit_bar_close NUMERIC,

    minutes_to_target INTEGER,

    final_price_1530 NUMERIC,
    eod_return_pct NUMERIC,

    data_status TEXT NOT NULL,
    error_message TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (study_start, study_end, trading_date, symbol)
);

CREATE TABLE IF NOT EXISTS public.avwap53_spot_target_hits_summary (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,
    generated_at TIMESTAMPTZ DEFAULT NOW(),

    expected_entries INTEGER NOT NULL,
    processed_entries INTEGER NOT NULL,
    valid_entries INTEGER NOT NULL,
    target_hits INTEGER NOT NULL,
    target_misses INTEGER NOT NULL,
    failed_entries INTEGER NOT NULL,

    summary JSONB NOT NULL,

    PRIMARY KEY (study_start, study_end)
);
"""

UPSERT = """
INSERT INTO public.avwap53_spot_target_hits (
 study_start,study_end,run_id,trading_date,symbol,spot_instrument_key,
 entry_time,entry_price,target_pct,target_price,target_hit,target_hit_time,
 target_hit_bar_start,target_hit_bar_end,target_hit_bar_open,target_hit_bar_high,
 target_hit_bar_low,target_hit_bar_close,minutes_to_target,
 final_price_1530,eod_return_pct,data_status,error_message
) VALUES (
 %(study_start)s,%(study_end)s,%(run_id)s,%(trading_date)s,%(symbol)s,%(spot_instrument_key)s,
 %(entry_time)s,%(entry_price)s,%(target_pct)s,%(target_price)s,%(target_hit)s,%(target_hit_time)s,
 %(target_hit_bar_start)s,%(target_hit_bar_end)s,%(target_hit_bar_open)s,%(target_hit_bar_high)s,
 %(target_hit_bar_low)s,%(target_hit_bar_close)s,%(minutes_to_target)s,
 %(final_price_1530)s,%(eod_return_pct)s,%(data_status)s,%(error_message)s
)
ON CONFLICT(study_start,study_end,trading_date,symbol) DO UPDATE SET
 run_id=EXCLUDED.run_id,
 spot_instrument_key=EXCLUDED.spot_instrument_key,
 entry_time=EXCLUDED.entry_time,
 entry_price=EXCLUDED.entry_price,
 target_pct=EXCLUDED.target_pct,
 target_price=EXCLUDED.target_price,
 target_hit=EXCLUDED.target_hit,
 target_hit_time=EXCLUDED.target_hit_time,
 target_hit_bar_start=EXCLUDED.target_hit_bar_start,
 target_hit_bar_end=EXCLUDED.target_hit_bar_end,
 target_hit_bar_open=EXCLUDED.target_hit_bar_open,
 target_hit_bar_high=EXCLUDED.target_hit_bar_high,
 target_hit_bar_low=EXCLUDED.target_hit_bar_low,
 target_hit_bar_close=EXCLUDED.target_hit_bar_close,
 minutes_to_target=EXCLUDED.minutes_to_target,
 final_price_1530=EXCLUDED.final_price_1530,
 eod_return_pct=EXCLUDED.eod_return_pct,
 data_status=EXCLUDED.data_status,
 error_message=EXCLUDED.error_message,
 updated_at=NOW();
"""

def load_entries():
    q = """
    SELECT
        trading_date,
        symbol,
        spot_instrument_key,
        entry_time,
        entry_price
    FROM public.spot_supply_1015_avwap_retrace_entry_backtest
    WHERE data_status='OK'
      AND setup_status='ENTRY_TAKEN'
      AND study_start=%s
      AND study_end=%s
      AND (entry_time AT TIME ZONE 'Asia/Kolkata')::time <= TIME '12:15'
    ORDER BY trading_date,entry_time,symbol
    """
    with db() as c:
        with c.cursor() as cur:
            cur.execute(q, (START, END))
            return [dict(r) for r in cur.fetchall()]

def fetch_1m(key, trade_day):
    enc = quote(key, safe="")
    url = f"{HIST}/{enc}/minutes/1/{trade_day.isoformat()}/{trade_day.isoformat()}"
    last = None
    for n in range(5):
        try:
            r = requests.get(url, headers=headers(), timeout=60)
            if r.status_code == 429:
                time.sleep(2 * (n + 1))
                continue
            r.raise_for_status()
            return (r.json().get("data") or {}).get("candles") or []
        except Exception as exc:
            last = exc
            if n < 4:
                time.sleep(1.5 * (n + 1))
    raise RuntimeError(f"spot history fetch failed: {last}")

def parse(rows):
    out = []
    for c in rows:
        if len(c) < 6:
            continue
        try:
            ts = datetime.fromisoformat(str(c[0]).replace("Z", "+00:00")).astimezone(IST)
            out.append({
                "ts": ts,
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": int(float(c[5] or 0)),
            })
        except Exception:
            continue
    return sorted(out, key=lambda x: x["ts"])

def pct(x, base):
    return (x / base - 1.0) * 100.0 if base else None

def analyze(e):
    rows = parse(fetch_1m(e["spot_instrument_key"], e["trading_date"]))
    entry_time = e["entry_time"].astimezone(IST)
    entry_price = float(e["entry_price"])
    target_price = entry_price * (1.0 + TARGET_PCT / 100.0)

    post = [
        r for r in rows
        if r["ts"] >= entry_time
        and r["ts"].time().replace(tzinfo=None) < dtime(15, 30)
    ]
    if not post:
        raise ValueError("No 1-minute spot candles from entry to EOD")

    hit = next((r for r in post if r["high"] >= target_price), None)
    final_price = float(post[-1]["close"])

    return {
        "study_start": START,
        "study_end": END,
        "run_id": RUN_ID,
        "trading_date": e["trading_date"],
        "symbol": e["symbol"],
        "spot_instrument_key": e["spot_instrument_key"],
        "entry_time": e["entry_time"],
        "entry_price": entry_price,
        "target_pct": TARGET_PCT,
        "target_price": target_price,
        "target_hit": hit is not None,
        # Historical 1m candle timestamp is candle start; target can occur any time within that minute.
        # We store both bar start and bar end and use bar_end as target_hit_time for conservative matching.
        "target_hit_time": hit["ts"] + __import__("datetime").timedelta(minutes=1) if hit else None,
        "target_hit_bar_start": hit["ts"] if hit else None,
        "target_hit_bar_end": hit["ts"] + __import__("datetime").timedelta(minutes=1) if hit else None,
        "target_hit_bar_open": hit["open"] if hit else None,
        "target_hit_bar_high": hit["high"] if hit else None,
        "target_hit_bar_low": hit["low"] if hit else None,
        "target_hit_bar_close": hit["close"] if hit else None,
        "minutes_to_target": (
            int(((hit["ts"] + __import__("datetime").timedelta(minutes=1)) - entry_time).total_seconds() // 60)
            if hit else None
        ),
        "final_price_1530": final_price,
        "eod_return_pct": pct(final_price, entry_price),
        "data_status": "OK",
        "error_message": None,
    }

def error_row(e, exc):
    entry_price = float(e["entry_price"])
    return {
        "study_start": START,
        "study_end": END,
        "run_id": RUN_ID,
        "trading_date": e["trading_date"],
        "symbol": e["symbol"],
        "spot_instrument_key": e["spot_instrument_key"],
        "entry_time": e["entry_time"],
        "entry_price": entry_price,
        "target_pct": TARGET_PCT,
        "target_price": entry_price * (1.0 + TARGET_PCT / 100.0),
        "target_hit": False,
        "target_hit_time": None,
        "target_hit_bar_start": None,
        "target_hit_bar_end": None,
        "target_hit_bar_open": None,
        "target_hit_bar_high": None,
        "target_hit_bar_low": None,
        "target_hit_bar_close": None,
        "minutes_to_target": None,
        "final_price_1530": None,
        "eod_return_pct": None,
        "data_status": "ERROR",
        "error_message": str(exc)[:1000],
    }

def main():
    if not DB:
        raise RuntimeError("NEON_DATABASE_URL required")
    if not TOKEN:
        raise RuntimeError("UPSTOX_TOKEN or UPSTOX_ACCESS_TOKEN required")

    with db() as c:
        with c.cursor() as cur:
            cur.execute(DDL)
        c.commit()

    entries = load_entries()
    log(f"Loaded {len(entries)} entries | expected={EXPECTED}")
    if len(entries) != EXPECTED:
        raise RuntimeError(f"Expected exactly {EXPECTED}, got {len(entries)}")

    out = []
    for i, e in enumerate(entries, 1):
        try:
            out.append(analyze(e))
        except Exception as exc:
            out.append(error_row(e, exc))

        if i % 10 == 0 or i == len(entries):
            log(f"Processed {i}/{len(entries)}")
        time.sleep(0.12)

    with db() as c:
        with c.cursor() as cur:
            cur.executemany(UPSERT, out)
        c.commit()

    valid = [r for r in out if r["data_status"] == "OK"]
    hits = [r for r in valid if r["target_hit"]]
    misses = [r for r in valid if not r["target_hit"]]
    failed = len(out) - len(valid)

    avg_minutes = None
    if hits:
        avg_minutes = round(
            sum(r["minutes_to_target"] for r in hits if r["minutes_to_target"] is not None) / len(hits),
            2,
        )

    summary = {
        "run_id": RUN_ID,
        "expected_entries": EXPECTED,
        "processed_entries": len(out),
        "valid_entries": len(valid),
        "target_hits": len(hits),
        "target_misses": len(misses),
        "failed_entries": failed,
        "target_pct": TARGET_PCT,
        "avg_minutes_to_target": avg_minutes,
    }

    q = """
    INSERT INTO public.avwap53_spot_target_hits_summary (
      study_start,study_end,run_id,expected_entries,processed_entries,
      valid_entries,target_hits,target_misses,failed_entries,summary
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(study_start,study_end) DO UPDATE SET
      run_id=EXCLUDED.run_id,
      generated_at=NOW(),
      expected_entries=EXCLUDED.expected_entries,
      processed_entries=EXCLUDED.processed_entries,
      valid_entries=EXCLUDED.valid_entries,
      target_hits=EXCLUDED.target_hits,
      target_misses=EXCLUDED.target_misses,
      failed_entries=EXCLUDED.failed_entries,
      summary=EXCLUDED.summary;
    """

    with db() as c:
        with c.cursor() as cur:
            cur.execute(q, (
                START, END, RUN_ID, EXPECTED, len(out),
                len(valid), len(hits), len(misses), failed, Jsonb(summary)
            ))
        c.commit()

    log("COMPLETE")
    log(str(summary))
    log("Detail : public.avwap53_spot_target_hits")
    log("Summary: public.avwap53_spot_target_hits_summary")

if __name__ == "__main__":
    main()
