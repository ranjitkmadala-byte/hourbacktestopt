Backfills spot target-before-stop for all 53 AVWAP entries.
Target: +0.5% from spot entry.
Stop: -0.5% from spot entry.
Uses Upstox 1-minute spot candles from entry to 15:30.

Important: if both target and stop occur inside the same 1-minute candle, the row is marked
AMBIGUOUS_SAME_1M_BAR rather than assuming an order.

Writes:
- public.avwap53_spot_target_stop
- public.avwap53_spot_target_stop_summary
