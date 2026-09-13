from __future__ import annotations
import os, time, uuid
from datetime import date, datetime, timedelta, time as dtime
from urllib.parse import quote
from zoneinfo import ZoneInfo
import psycopg, requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

IST=ZoneInfo("Asia/Kolkata")
DB=(os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
TOKEN=(os.getenv("UPSTOX_ACCESS_TOKEN") or os.getenv("UPSTOX_TOKEN") or "").strip()
START=date.fromisoformat(os.getenv("STUDY_START","2026-08-26"))
END=date.fromisoformat(os.getenv("STUDY_END","2026-09-11"))
EXPECTED=int(os.getenv("EXPECTED_ENTRIES","53"))
TARGET_PCT=float(os.getenv("TARGET_PCT","0.50"))
STOP_PCT=float(os.getenv("STOP_PCT","0.50"))
RUN_ID=str(uuid.uuid4())
HIST="https://api.upstox.com/v3/historical-candle"

def log(x): print(f"{datetime.now(IST):%Y-%m-%d %H:%M:%S} IST | {x}",flush=True)
def db(): return psycopg.connect(DB,row_factory=dict_row,connect_timeout=20)
def hdr(): return {"Accept":"application/json","Authorization":f"Bearer {TOKEN}"}

DDL="""
CREATE TABLE IF NOT EXISTS public.avwap53_spot_target_stop (
 study_start date NOT NULL, study_end date NOT NULL, run_id uuid NOT NULL,
 trading_date date NOT NULL, symbol text NOT NULL, spot_instrument_key text NOT NULL,
 entry_time timestamptz NOT NULL, entry_price numeric NOT NULL,
 target_price numeric NOT NULL, stop_price numeric NOT NULL,
 first_outcome text NOT NULL,
 outcome_time timestamptz, outcome_bar_start timestamptz, outcome_bar_end timestamptz,
 outcome_bar_open numeric,outcome_bar_high numeric,outcome_bar_low numeric,outcome_bar_close numeric,
 minutes_to_outcome integer,
 target_hit_time timestamptz, stop_hit_time timestamptz,
 ambiguous_same_bar boolean NOT NULL DEFAULT false,
 final_price_1530 numeric,eod_return_pct numeric,
 data_status text NOT NULL,error_message text,updated_at timestamptz default now(),
 PRIMARY KEY(study_start,study_end,trading_date,symbol)
);
CREATE TABLE IF NOT EXISTS public.avwap53_spot_target_stop_summary(
 study_start date NOT NULL,study_end date NOT NULL,run_id uuid NOT NULL,
 generated_at timestamptz default now(),expected_entries integer,processed_entries integer,
 valid_entries integer,target_first integer,stop_first integer,neither integer,
 ambiguous_same_bar integer,failed_entries integer,summary jsonb,
 PRIMARY KEY(study_start,study_end)
);
"""
UPS="""
INSERT INTO public.avwap53_spot_target_stop(
 study_start,study_end,run_id,trading_date,symbol,spot_instrument_key,entry_time,entry_price,
 target_price,stop_price,first_outcome,outcome_time,outcome_bar_start,outcome_bar_end,
 outcome_bar_open,outcome_bar_high,outcome_bar_low,outcome_bar_close,minutes_to_outcome,
 target_hit_time,stop_hit_time,ambiguous_same_bar,final_price_1530,eod_return_pct,data_status,error_message
) VALUES(
 %(study_start)s,%(study_end)s,%(run_id)s,%(trading_date)s,%(symbol)s,%(spot_instrument_key)s,
 %(entry_time)s,%(entry_price)s,%(target_price)s,%(stop_price)s,%(first_outcome)s,%(outcome_time)s,
 %(outcome_bar_start)s,%(outcome_bar_end)s,%(outcome_bar_open)s,%(outcome_bar_high)s,
 %(outcome_bar_low)s,%(outcome_bar_close)s,%(minutes_to_outcome)s,%(target_hit_time)s,
 %(stop_hit_time)s,%(ambiguous_same_bar)s,%(final_price_1530)s,%(eod_return_pct)s,%(data_status)s,%(error_message)s
)
ON CONFLICT(study_start,study_end,trading_date,symbol) DO UPDATE SET
 run_id=EXCLUDED.run_id,target_price=EXCLUDED.target_price,stop_price=EXCLUDED.stop_price,
 first_outcome=EXCLUDED.first_outcome,outcome_time=EXCLUDED.outcome_time,
 outcome_bar_start=EXCLUDED.outcome_bar_start,outcome_bar_end=EXCLUDED.outcome_bar_end,
 outcome_bar_open=EXCLUDED.outcome_bar_open,outcome_bar_high=EXCLUDED.outcome_bar_high,
 outcome_bar_low=EXCLUDED.outcome_bar_low,outcome_bar_close=EXCLUDED.outcome_bar_close,
 minutes_to_outcome=EXCLUDED.minutes_to_outcome,target_hit_time=EXCLUDED.target_hit_time,
 stop_hit_time=EXCLUDED.stop_hit_time,ambiguous_same_bar=EXCLUDED.ambiguous_same_bar,
 final_price_1530=EXCLUDED.final_price_1530,eod_return_pct=EXCLUDED.eod_return_pct,
 data_status=EXCLUDED.data_status,error_message=EXCLUDED.error_message,updated_at=NOW();
"""

def entries():
    q="""SELECT trading_date,symbol,spot_instrument_key,entry_time,entry_price
    FROM public.spot_supply_1015_avwap_retrace_entry_backtest
    WHERE data_status='OK' AND setup_status='ENTRY_TAKEN'
      AND study_start=%s AND study_end=%s
      AND (entry_time AT TIME ZONE 'Asia/Kolkata')::time<=TIME '12:15'
    ORDER BY trading_date,entry_time,symbol"""
    with db() as c:
        with c.cursor() as x:x.execute(q,(START,END));return [dict(r) for r in x.fetchall()]

def candles(key,day):
    u=f"{HIST}/{quote(key,safe='')}/minutes/1/{day.isoformat()}/{day.isoformat()}"
    last=None
    for n in range(5):
        try:
            r=requests.get(u,headers=hdr(),timeout=60)
            if r.status_code==429: time.sleep(2*(n+1));continue
            r.raise_for_status()
            out=[]
            for a in (r.json().get("data") or {}).get("candles") or []:
                if len(a)<6: continue
                ts=datetime.fromisoformat(str(a[0]).replace("Z","+00:00")).astimezone(IST)
                out.append(dict(ts=ts,open=float(a[1]),high=float(a[2]),low=float(a[3]),close=float(a[4])))
            return sorted(out,key=lambda z:z["ts"])
        except Exception as exc:
            last=exc;time.sleep(1+n)
    raise RuntimeError(last)

def analyze(e):
    ep=float(e["entry_price"]); target=ep*(1+TARGET_PCT/100); stop=ep*(1-STOP_PCT/100)
    et=e["entry_time"].astimezone(IST)
    bars=[r for r in candles(e["spot_instrument_key"],e["trading_date"])
          if r["ts"]>=et and r["ts"].time().replace(tzinfo=None)<dtime(15,30)]
    if not bars: raise ValueError("No post-entry 1m spot candles")
    th=sh=None; outcome=None; ob=None; ambiguous=False
    for r in bars:
        t=r["high"]>=target; s=r["low"]<=stop
        if t and th is None: th=r["ts"]+timedelta(minutes=1)
        if s and sh is None: sh=r["ts"]+timedelta(minutes=1)
        if t or s:
            ob=r
            if t and s:
                outcome="AMBIGUOUS_SAME_1M_BAR";ambiguous=True
            elif t: outcome="TARGET_FIRST"
            else: outcome="STOP_FIRST"
            break
    if outcome is None: outcome="NEITHER"
    ot=ob["ts"]+timedelta(minutes=1) if ob else None
    final=bars[-1]["close"]
    return dict(study_start=START,study_end=END,run_id=RUN_ID,trading_date=e["trading_date"],
      symbol=e["symbol"],spot_instrument_key=e["spot_instrument_key"],entry_time=e["entry_time"],
      entry_price=ep,target_price=target,stop_price=stop,first_outcome=outcome,outcome_time=ot,
      outcome_bar_start=ob["ts"] if ob else None,outcome_bar_end=ot,
      outcome_bar_open=ob["open"] if ob else None,outcome_bar_high=ob["high"] if ob else None,
      outcome_bar_low=ob["low"] if ob else None,outcome_bar_close=ob["close"] if ob else None,
      minutes_to_outcome=int((ot-et).total_seconds()//60) if ot else None,
      target_hit_time=th,stop_hit_time=sh,ambiguous_same_bar=ambiguous,
      final_price_1530=final,eod_return_pct=(final/ep-1)*100,data_status="OK",error_message=None)

def main():
    if not DB or not TOKEN: raise RuntimeError("NEON_DATABASE_URL and UPSTOX_TOKEN required")
    with db() as c:
        with c.cursor() as x:x.execute(DDL)
        c.commit()
    es=entries();log(f"entries={len(es)} expected={EXPECTED}")
    if len(es)!=EXPECTED: raise RuntimeError(f"Expected {EXPECTED}, got {len(es)}")
    rows=[]
    for i,e in enumerate(es,1):
        try: rows.append(analyze(e))
        except Exception as exc:
            ep=float(e["entry_price"])
            rows.append(dict(study_start=START,study_end=END,run_id=RUN_ID,trading_date=e["trading_date"],
              symbol=e["symbol"],spot_instrument_key=e["spot_instrument_key"],entry_time=e["entry_time"],
              entry_price=ep,target_price=ep*1.005,stop_price=ep*.995,first_outcome="ERROR",
              outcome_time=None,outcome_bar_start=None,outcome_bar_end=None,outcome_bar_open=None,
              outcome_bar_high=None,outcome_bar_low=None,outcome_bar_close=None,minutes_to_outcome=None,
              target_hit_time=None,stop_hit_time=None,ambiguous_same_bar=False,final_price_1530=None,
              eod_return_pct=None,data_status="ERROR",error_message=str(exc)[:1000]))
        if i%10==0 or i==len(es):log(f"processed {i}/{len(es)}")
        time.sleep(.1)
    with db() as c:
        with c.cursor() as x:x.executemany(UPS,rows)
        c.commit()
    valid=[r for r in rows if r["data_status"]=="OK"]
    counts={k:sum(r["first_outcome"]==k for r in valid) for k in
            ("TARGET_FIRST","STOP_FIRST","NEITHER","AMBIGUOUS_SAME_1M_BAR")}
    summary={"target_pct":TARGET_PCT,"stop_pct":STOP_PCT,**counts}
    q="""INSERT INTO public.avwap53_spot_target_stop
    SELECT * FROM public.avwap53_spot_target_stop WHERE false""" # no-op sanity
    qs="""INSERT INTO public.avwap53_spot_target_stop_summary
    (study_start,study_end,run_id,expected_entries,processed_entries,valid_entries,target_first,stop_first,neither,ambiguous_same_bar,failed_entries,summary)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(study_start,study_end) DO UPDATE SET run_id=EXCLUDED.run_id,generated_at=NOW(),
    expected_entries=EXCLUDED.expected_entries,processed_entries=EXCLUDED.processed_entries,
    valid_entries=EXCLUDED.valid_entries,target_first=EXCLUDED.target_first,stop_first=EXCLUDED.stop_first,
    neither=EXCLUDED.neither,ambiguous_same_bar=EXCLUDED.ambiguous_same_bar,
    failed_entries=EXCLUDED.failed_entries,summary=EXCLUDED.summary"""
    with db() as c:
        with c.cursor() as x:x.execute(qs,(START,END,RUN_ID,EXPECTED,len(rows),len(valid),
          counts["TARGET_FIRST"],counts["STOP_FIRST"],counts["NEITHER"],counts["AMBIGUOUS_SAME_1M_BAR"],
          len(rows)-len(valid),Jsonb(summary)))
        c.commit()
    log("COMPLETE "+str(summary))

if __name__=="__main__": main()
