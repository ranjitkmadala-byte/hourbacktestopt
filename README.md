# AVWAP 53 — Upstox ATM option backfill

For the 53 spot AVWAP retrace/recovery entries confirmed by 12:15:

1. Reads the 53 spot entry events from Neon.
2. Gets available expiries from Upstox.
3. Selects the nearest expiry >= the event's trading date.
4. Identifies nearest ATM CE + PE from the exact spot entry price.
5. Uses expired option APIs for already-expired contracts.
6. Uses current option-contract + V3 historical candle APIs for non-expired contracts.
7. Downloads 1-minute option OHLC, volume, and OI from spot entry time through EOD.
8. Writes the full CE/PE path to Neon.

Output tables:
- public.avwap53_upstox_atm_contracts
- public.avwap53_upstox_atm_option_1m
- public.avwap53_upstox_atm_summary

Required Railway variables:
- NEON_DATABASE_URL
- UPSTOX_TOKEN

Important:
Historical Upstox candles contain total volume and OI. They do NOT expose buyer-initiated
vs seller-initiated volume, so true CVD cannot be reconstructed.

The worker stores a research-only signed-volume proxy:
- green 1m option candle => +volume
- red 1m option candle => -volume
- flat candle => 0

This is explicitly labelled as a proxy and must not be called true CVD.
