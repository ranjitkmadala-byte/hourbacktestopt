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
ENTRY_CUTOFF = dtime(12, 15)
EOD = dtime(15, 30)
RUN_ID = str(uuid.uuid4())

BASE_V2 = "https://api.upstox.com/v2"
BASE_V3 = "https://api.upstox.com/v3/historical-candle"

def log(x):
    print(f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} IST | {x}", flush=True)

def conn():
    return psycopg.connect(DB, row_factory=dict_row, connect_timeout=20)

def headers():
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }

def get_json(url, params=None, tries=5):
    last = None
    for n in range(tries):
        try:
            r = requests.get(url, params=params, headers=headers(), timeout=60)
            if r.status_code == 429:
                time.sleep(2 * (n + 1))
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:600]}")
            return r.json()
        except Exception as exc:
            last = exc
            if n < tries - 1:
                time.sleep(1.5 * (n + 1))
    raise RuntimeError(str(last))

DDL = """
CREATE TABLE IF NOT EXISTS public.avwap53_upstox_atm_contracts (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,

    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_instrument_key TEXT NOT NULL,
    spot_entry_time TIMESTAMPTZ NOT NULL,
    spot_entry_price NUMERIC NOT NULL,

    selected_expiry DATE,
    option_type TEXT NOT NULL,
    strike NUMERIC,
    option_instrument_key TEXT,
    trading_symbol TEXT,
    source_api TEXT,

    strike_distance NUMERIC,
    data_status TEXT NOT NULL,
    message TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY(study_start, study_end, trading_date, symbol, option_type)
);

CREATE TABLE IF NOT EXISTS public.avwap53_upstox_atm_option_1m (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,

    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_entry_time TIMESTAMPTZ NOT NULL,
    spot_entry_price NUMERIC NOT NULL,

    expiry DATE NOT NULL,
    option_type TEXT NOT NULL,
    strike NUMERIC NOT NULL,
    option_instrument_key TEXT NOT NULL,

    ts TIMESTAMPTZ NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    oi BIGINT,

    volume_from_entry BIGINT,
    oi_change_from_entry BIGINT,
    oi_change_from_entry_pct NUMERIC,
    premium_change_from_entry_pct NUMERIC,

    signed_volume_proxy BIGINT,
    cumulative_signed_volume_proxy BIGINT,

    flow_proxy_state TEXT,

    true_cvd NUMERIC,
    true_cvd_available BOOLEAN NOT NULL DEFAULT FALSE,
    true_cvd_reason TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY(study_start, study_end, trading_date, symbol, option_type, ts)
);

CREATE TABLE IF NOT EXISTS public.avwap53_upstox_atm_summary (
    study_start DATE NOT NULL,
    study_end DATE NOT NULL,
    run_id UUID NOT NULL,
    generated_at TIMESTAMPTZ DEFAULT NOW(),

    expected_entries INTEGER NOT NULL,
    entries_found INTEGER NOT NULL,
    ce_contracts_found INTEGER NOT NULL,
    pe_contracts_found INTEGER NOT NULL,
    complete_pairs INTEGER NOT NULL,
    option_1m_rows INTEGER NOT NULL,

    summary JSONB NOT NULL,
    PRIMARY KEY(study_start, study_end)
);
"""

UPS_CONTRACT = """
INSERT INTO public.avwap53_upstox_atm_contracts (
 study_start,study_end,run_id,trading_date,symbol,spot_instrument_key,spot_entry_time,spot_entry_price,
 selected_expiry,option_type,strike,option_instrument_key,trading_symbol,source_api,
 strike_distance,data_status,message
) VALUES (
 %(study_start)s,%(study_end)s,%(run_id)s,%(trading_date)s,%(symbol)s,%(spot_instrument_key)s,
 %(spot_entry_time)s,%(spot_entry_price)s,%(selected_expiry)s,%(option_type)s,%(strike)s,
 %(option_instrument_key)s,%(trading_symbol)s,%(source_api)s,%(strike_distance)s,%(data_status)s,%(message)s
)
ON CONFLICT(study_start,study_end,trading_date,symbol,option_type) DO UPDATE SET
 run_id=EXCLUDED.run_id,spot_instrument_key=EXCLUDED.spot_instrument_key,
 spot_entry_time=EXCLUDED.spot_entry_time,spot_entry_price=EXCLUDED.spot_entry_price,
 selected_expiry=EXCLUDED.selected_expiry,strike=EXCLUDED.strike,
 option_instrument_key=EXCLUDED.option_instrument_key,trading_symbol=EXCLUDED.trading_symbol,
 source_api=EXCLUDED.source_api,strike_distance=EXCLUDED.strike_distance,
 data_status=EXCLUDED.data_status,message=EXCLUDED.message,updated_at=NOW();
"""

UPS_1M = """
INSERT INTO public.avwap53_upstox_atm_option_1m (
 study_start,study_end,run_id,trading_date,symbol,spot_entry_time,spot_entry_price,
 expiry,option_type,strike,option_instrument_key,ts,open,high,low,close,volume,oi,
 volume_from_entry,oi_change_from_entry,oi_change_from_entry_pct,premium_change_from_entry_pct,
 signed_volume_proxy,cumulative_signed_volume_proxy,flow_proxy_state,
 true_cvd,true_cvd_available,true_cvd_reason
) VALUES (
 %(study_start)s,%(study_end)s,%(run_id)s,%(trading_date)s,%(symbol)s,%(spot_entry_time)s,%(spot_entry_price)s,
 %(expiry)s,%(option_type)s,%(strike)s,%(option_instrument_key)s,%(ts)s,%(open)s,%(high)s,%(low)s,%(close)s,%(volume)s,%(oi)s,
 %(volume_from_entry)s,%(oi_change_from_entry)s,%(oi_change_from_entry_pct)s,%(premium_change_from_entry_pct)s,
 %(signed_volume_proxy)s,%(cumulative_signed_volume_proxy)s,%(flow_proxy_state)s,
 %(true_cvd)s,%(true_cvd_available)s,%(true_cvd_reason)s
)
ON CONFLICT(study_start,study_end,trading_date,symbol,option_type,ts) DO UPDATE SET
 open=EXCLUDED.open,high=EXCLUDED.high,low=EXCLUDED.low,close=EXCLUDED.close,
 volume=EXCLUDED.volume,oi=EXCLUDED.oi,volume_from_entry=EXCLUDED.volume_from_entry,
 oi_change_from_entry=EXCLUDED.oi_change_from_entry,
 oi_change_from_entry_pct=EXCLUDED.oi_change_from_entry_pct,
 premium_change_from_entry_pct=EXCLUDED.premium_change_from_entry_pct,
 signed_volume_proxy=EXCLUDED.signed_volume_proxy,
 cumulative_signed_volume_proxy=EXCLUDED.cumulative_signed_volume_proxy,
 flow_proxy_state=EXCLUDED.flow_proxy_state;
"""

def load_entries():
    q = """
    SELECT trading_date,symbol,spot_instrument_key,entry_time,entry_price
    FROM public.spot_supply_1015_avwap_retrace_entry_backtest
    WHERE data_status='OK'
      AND setup_status='ENTRY_TAKEN'
      AND study_start=%s AND study_end=%s
      AND (entry_time AT TIME ZONE 'Asia/Kolkata')::time <= TIME '12:15'
    ORDER BY trading_date,entry_time,symbol
    """
    with conn() as c:
        with c.cursor() as x:
            x.execute(q, (START, END))
            return [dict(r) for r in x.fetchall()]

def expired_expiries(spot_key):
    url = f"{BASE_V2}/expired-instruments/expiries"
    j = get_json(url, params={"instrument_key": spot_key})
    data = j.get("data") or []
    out = []
    for x in data:
        try:
            if isinstance(x, str):
                out.append(date.fromisoformat(x[:10]))
            elif isinstance(x, dict):
                val = x.get("expiry") or x.get("expiry_date") or x.get("date")
                if val:
                    out.append(date.fromisoformat(str(val)[:10]))
        except Exception:
            pass
    return sorted(set(out))

def current_contracts(spot_key, expiry=None):
    params = {"instrument_key": spot_key}
    if expiry:
        params["expiry_date"] = expiry.isoformat()
    j = get_json(f"{BASE_V2}/option/contract", params=params)
    return j.get("data") or []

def expired_contracts(spot_key, expiry):
    j = get_json(
        f"{BASE_V2}/expired-instruments/option/contract",
        params={"instrument_key": spot_key, "expiry_date": expiry.isoformat()},
    )
    return j.get("data") or []

def pick_expiry_and_contracts(entry):
    """
    Prefer nearest expiry >= trade date.
    Combine:
      - expired expiries from expired API
      - current option contracts' expiries
    """
    spot_key = entry["spot_instrument_key"]
    trade_day = entry["trading_date"]

    exps = set()
    try:
        exps.update(expired_expiries(spot_key))
    except Exception as exc:
        log(f"{entry['symbol']} expired expiries warning: {exc}")

    curr = []
    try:
        curr = current_contracts(spot_key)
        for r in curr:
            val = r.get("expiry")
            if val:
                try:
                    exps.add(date.fromisoformat(str(val)[:10]))
                except Exception:
                    pass
    except Exception as exc:
        log(f"{entry['symbol']} current contracts warning: {exc}")

    candidates = sorted(x for x in exps if x >= trade_day)
    if not candidates:
        raise RuntimeError("No expiry >= trading date found")

    expiry = candidates[0]
    today = datetime.now(IST).date()

    if expiry < today:
        rows = expired_contracts(spot_key, expiry)
        source = "EXPIRED"
    else:
        # Could be current or future expiry.
        rows = current_contracts(spot_key, expiry)
        source = "CURRENT"

    if not rows:
        raise RuntimeError(f"No option contracts for expiry {expiry}")

    return expiry, source, rows

def pick_atm_pair(entry, expiry, source, contracts):
    spot = float(entry["entry_price"])
    result = {}
    for side in ("CE", "PE"):
        cands = []
        for r in contracts:
            typ = str(r.get("instrument_type") or r.get("option_type") or "").upper()
            if typ != side:
                continue
            strike = r.get("strike_price")
            if strike is None:
                strike = r.get("strike")
            key = r.get("instrument_key")
            if strike is None or not key:
                continue
            try:
                strike = float(strike)
            except Exception:
                continue
            cands.append((abs(strike - spot), strike, r))
        if not cands:
            result[side] = None
            continue
        cands.sort(key=lambda x: (x[0], x[1]))
        dist, strike, r = cands[0]
        result[side] = {
            "expiry": expiry,
            "source": source,
            "strike": strike,
            "key": str(r.get("instrument_key")),
            "trading_symbol": str(r.get("trading_symbol") or ""),
            "distance": dist,
        }
    return result

def historical_1m(contract, trade_day):
    key = quote(contract["key"], safe="")
    if contract["source"] == "EXPIRED":
        url = (
            f"{BASE_V2}/expired-instruments/historical-candle/"
            f"{key}/1minute/{trade_day.isoformat()}/{trade_day.isoformat()}"
        )
    else:
        url = (
            f"{BASE_V3}/{key}/minutes/1/"
            f"{trade_day.isoformat()}/{trade_day.isoformat()}"
        )
    j = get_json(url)
    return (j.get("data") or {}).get("candles") or []

def parse_candles(candles):
    out = []
    for c in candles:
        if len(c) < 7:
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
                "oi": int(float(c[6] or 0)),
            })
        except Exception:
            pass
    return sorted(out, key=lambda x: x["ts"])

def flow_state(prev_close, close, prev_oi, oi):
    if prev_close is None or prev_oi is None:
        return "BASELINE"
    dp = close - prev_close
    doi = oi - prev_oi
    if dp > 0 and doi > 0:
        return "PRICE_UP_OI_UP"
    if dp > 0 and doi < 0:
        return "PRICE_UP_OI_DOWN"
    if dp < 0 and doi > 0:
        return "PRICE_DOWN_OI_UP"
    if dp < 0 and doi < 0:
        return "PRICE_DOWN_OI_DOWN"
    return "MIXED_FLAT"

def process_contract(entry, side, contract):
    base_contract = {
        "study_start": START, "study_end": END, "run_id": RUN_ID,
        "trading_date": entry["trading_date"], "symbol": entry["symbol"],
        "spot_instrument_key": entry["spot_instrument_key"],
        "spot_entry_time": entry["entry_time"], "spot_entry_price": entry["entry_price"],
        "selected_expiry": contract["expiry"] if contract else None,
        "option_type": side,
        "strike": contract["strike"] if contract else None,
        "option_instrument_key": contract["key"] if contract else None,
        "trading_symbol": contract["trading_symbol"] if contract else None,
        "source_api": contract["source"] if contract else None,
        "strike_distance": contract["distance"] if contract else None,
        "data_status": "OK" if contract else "MISSING",
        "message": None if contract else "ATM contract not found",
    }
    if not contract:
        return base_contract, []

    raw = parse_candles(historical_1m(contract, entry["trading_date"]))
    entry_ts = entry["entry_time"]
    path = [
        r for r in raw
        if r["ts"] >= entry_ts
        and r["ts"].time().replace(tzinfo=None) < EOD
    ]
    if not path:
        base_contract["data_status"] = "MISSING"
        base_contract["message"] = "No 1-minute candles from entry to EOD"
        return base_contract, []

    entry_close = path[0]["close"]
    entry_oi = path[0]["oi"]
    cumulative_proxy = 0
    cum_volume = 0
    prev_close = None
    prev_oi = None
    rows = []
    reason = (
        "Upstox historical candles provide total volume and OI, not aggressor-side "
        "buy/sell volume. signed_volume_proxy is candle-direction volume, NOT true CVD."
    )

    for r in path:
        # Proxy only: up candle = +volume; down candle = -volume.
        if r["close"] > r["open"]:
            signed = r["volume"]
        elif r["close"] < r["open"]:
            signed = -r["volume"]
        else:
            signed = 0

        cumulative_proxy += signed
        cum_volume += r["volume"]

        rows.append({
            "study_start": START, "study_end": END, "run_id": RUN_ID,
            "trading_date": entry["trading_date"], "symbol": entry["symbol"],
            "spot_entry_time": entry["entry_time"], "spot_entry_price": entry["entry_price"],
            "expiry": contract["expiry"], "option_type": side, "strike": contract["strike"],
            "option_instrument_key": contract["key"], "ts": r["ts"],
            "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"],
            "volume": r["volume"], "oi": r["oi"],
            "volume_from_entry": cum_volume,
            "oi_change_from_entry": r["oi"] - entry_oi,
            "oi_change_from_entry_pct": ((r["oi"] / entry_oi) - 1) * 100 if entry_oi else None,
            "premium_change_from_entry_pct": ((r["close"] / entry_close) - 1) * 100 if entry_close else None,
            "signed_volume_proxy": signed,
            "cumulative_signed_volume_proxy": cumulative_proxy,
            "flow_proxy_state": flow_state(prev_close, r["close"], prev_oi, r["oi"]),
            "true_cvd": None,
            "true_cvd_available": False,
            "true_cvd_reason": reason,
        })
        prev_close = r["close"]
        prev_oi = r["oi"]

    return base_contract, rows

def main():
    if not DB:
        raise RuntimeError("NEON_DATABASE_URL required")
    if not TOKEN:
        raise RuntimeError("UPSTOX_TOKEN or UPSTOX_ACCESS_TOKEN required")

    with conn() as c:
        with c.cursor() as x:
            x.execute(DDL)
        c.commit()

    entries = load_entries()
    log(f"Loaded {len(entries)} AVWAP entries; expected {EXPECTED}")
    if len(entries) != EXPECTED:
        raise RuntimeError(f"Expected {EXPECTED}, got {len(entries)}")

    contract_rows = []
    candle_rows = []

    for i, entry in enumerate(entries, 1):
        try:
            expiry, source, contracts = pick_expiry_and_contracts(entry)
            pair = pick_atm_pair(entry, expiry, source, contracts)
        except Exception as exc:
            log(f"{entry['trading_date']} {entry['symbol']} contract discovery failed: {exc}")
            pair = {"CE": None, "PE": None}

        for side in ("CE", "PE"):
            c = pair.get(side)
            try:
                cr, rows = process_contract(entry, side, c)
            except Exception as exc:
                cr = {
                    "study_start": START, "study_end": END, "run_id": RUN_ID,
                    "trading_date": entry["trading_date"], "symbol": entry["symbol"],
                    "spot_instrument_key": entry["spot_instrument_key"],
                    "spot_entry_time": entry["entry_time"], "spot_entry_price": entry["entry_price"],
                    "selected_expiry": c["expiry"] if c else None, "option_type": side,
                    "strike": c["strike"] if c else None,
                    "option_instrument_key": c["key"] if c else None,
                    "trading_symbol": c["trading_symbol"] if c else None,
                    "source_api": c["source"] if c else None,
                    "strike_distance": c["distance"] if c else None,
                    "data_status": "ERROR", "message": str(exc)[:1000],
                }
                rows = []
            contract_rows.append(cr)
            candle_rows.extend(rows)

        if i % 5 == 0 or i == len(entries):
            log(f"Processed {i}/{len(entries)} entries | 1m rows={len(candle_rows)}")
        time.sleep(0.15)

    with conn() as c:
        with c.cursor() as x:
            x.executemany(UPS_CONTRACT, contract_rows)
            if candle_rows:
                x.executemany(UPS_1M, candle_rows)
        c.commit()

    ce = sum(r["option_type"] == "CE" and r["data_status"] == "OK" for r in contract_rows)
    pe = sum(r["option_type"] == "PE" and r["data_status"] == "OK" for r in contract_rows)
    pairs_map = {}
    for r in contract_rows:
        k = (r["trading_date"], r["symbol"])
        pairs_map.setdefault(k, set())
        if r["data_status"] == "OK":
            pairs_map[k].add(r["option_type"])
    pairs = sum(v == {"CE", "PE"} for v in pairs_map.values())

    summary = {
        "run_id": RUN_ID,
        "entries": len(entries),
        "ce_contracts_found": ce,
        "pe_contracts_found": pe,
        "complete_pairs": pairs,
        "option_1m_rows": len(candle_rows),
        "true_cvd_available": False,
        "signed_volume_proxy_available": True,
        "signed_volume_proxy_definition": (
            "+volume on green option candle, -volume on red option candle; "
            "research proxy only, not aggressor-side CVD"
        ),
    }

    q = """
    INSERT INTO public.avwap53_upstox_atm_summary (
      study_start,study_end,run_id,expected_entries,entries_found,
      ce_contracts_found,pe_contracts_found,complete_pairs,option_1m_rows,summary
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(study_start,study_end) DO UPDATE SET
      run_id=EXCLUDED.run_id,generated_at=NOW(),
      expected_entries=EXCLUDED.expected_entries,entries_found=EXCLUDED.entries_found,
      ce_contracts_found=EXCLUDED.ce_contracts_found,pe_contracts_found=EXCLUDED.pe_contracts_found,
      complete_pairs=EXCLUDED.complete_pairs,option_1m_rows=EXCLUDED.option_1m_rows,
      summary=EXCLUDED.summary;
    """
    with conn() as c:
        with c.cursor() as x:
            x.execute(q, (
                START, END, RUN_ID, EXPECTED, len(entries),
                ce, pe, pairs, len(candle_rows), Jsonb(summary)
            ))
        c.commit()

    log("BACKFILL COMPLETE")
    log(str(summary))
    log("Contracts: public.avwap53_upstox_atm_contracts")
    log("1m path : public.avwap53_upstox_atm_option_1m")
    log("Summary : public.avwap53_upstox_atm_summary")

if __name__ == "__main__":
    main()
