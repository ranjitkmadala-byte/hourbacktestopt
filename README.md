# AVWAP 53 — Spot +0.5% target-hit backfill

For the 53 historical AVWAP entries:
- fetches 1-minute SPOT candles from Upstox
- calculates target = entry * 1.005
- finds first 1-minute candle whose HIGH reaches target
- stores target-hit bar start/end and conservative target_hit_time = bar end
- stores minutes to target
- writes results to Neon

Output:
- public.avwap53_spot_target_hits
- public.avwap53_spot_target_hits_summary

Required Railway variables:
- NEON_DATABASE_URL
- UPSTOX_TOKEN
