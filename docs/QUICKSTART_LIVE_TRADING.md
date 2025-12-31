# Quick Start: Live Trading

## Pre-Flight Checklist

```bash
# 1. Verify model bundles referenced in config
python - <<'PY'
import json
from pathlib import Path
routes = json.load(open('config/model_routes.json')).get('routes', [])
missing = [r.get('model_path') for r in routes if r.get('model_path') and not Path(r['model_path']).exists()]
if missing:
    print('Missing model files:')
    for p in missing:
        print('-', p)
else:
    print('✅ All model files present')
PY

# 2. Check configuration files exist
cat config/stream_config.json
cat config/model_routes.json

# 3. Verify OANDA credentials
python -c "from api.oanda_api import OandaApi; api = OandaApi(); print('✅ API connected' if api.get_account_summary() else '⚠️ API not ready')"
```

## Minimal Configuration

### `config/stream_config.json`
```json
{
  "global_settings": {
    "granularity": "M1",
    "default_brick_size": 10,
    "risk": {"type": "percent", "value": 0.002},
    "max_concurrent_pairs": 5,
    "signal_stale_minutes": 3,
    "target_bricks": 4,
    "stop_bricks": 2,
    "pivot_lookback": 10,
    "renko_mode": "candle_only"
  },
  "pair_settings": {
    "EUR_USD": {"brick_size": 10, "pip_location": -4}
  }
}
```

### `config/model_routes.json`
```json
{
  "routes": [
    {
      "pair": "EUR_USD",
      "name": "EUR_USD_dual",
      "model_path": "generalizers/EUR_USD_M1_dual_models.pkl",
      "threshold": 0.55,
      "ev_threshold": 0.0
    }
  ]
}
```

## Run

```bash
python run_renko_trading.py
```

## What You'll See

**Startup** (should see all ✅):
```
✅ Loaded 28 instruments from file
✅ API ready
✅ CandleManager ready
✅ Configured 1 pairs
✅ Loaded dual model for EUR_USD
✅ Trade Manager ready: 4 bricks for / 2 bricks against (brick exits)
✅ Renko Engine started in 'candle_only' mode with lookback=10
```

**Pivot Detection**:
```
🎯 NEW PIVOT DETECTED: EUR_USD
   Type: LOW → BUY
   ML Score: 0.632 (threshold: 0.550)
   ⚡ HIGH CONFIDENCE SIGNAL - Attempting entry
   ✅ Trade entered successfully
```

**Brick Updates**:
```
EUR_USD: Brick update - For: 1, Against: 0
EUR_USD: Brick update - For: 2, Against: 0
...
```

**Exit**:
```
🛑 CLOSING POSITION: EUR_USD
   Reason: target_4_bricks_for
   Bricks For: 4
   ✅ Position closed successfully
```

## Stop

`Ctrl+C` will gracefully shutdown all components.

## Paper Trading First

⚠️ **CRITICAL**: Use OANDA practice account initially
⚠️ Start with 0.1-0.2% risk
⚠️ Monitor for 24-48 hours before scaling

## Troubleshooting

**No pivots detected?**
- Check M1 candles arriving (should see M1 updates in logs)
- Verify brick size matches market volatility

**No trades executing?**
- Check ML scores (are they above threshold?)
- Verify signal age (must be < 3 min)
- Check max pairs limit not hit

**Positions not closing?**
- Verify Renko engine running and brick events logging
- Check brick updates in logs
- Monitor brick counts (bricks_for/against)

## Key Metrics to Watch

- **Signal age**: Should be < 60 seconds typically
- **ML scores**: Should vary 0.4-0.8 for good signals
- **Slippage**: Should be < 0.3 bricks
- **Fill rate**: Should be > 95%

## Emergency Stop

```python
# In Python REPL
from api.oanda_api import OandaApi
api = OandaApi()

# Close all open trades
trades = api.get_open_trades()
for t in trades:
    api.close_trade(t.id)
    print(f"Closed {t.id}")
```

## Next Steps

After successful paper trading:
1. Increase risk to 0.5%
2. Add 1-2 more pairs
3. Monitor for 1 week
4. Scale gradually

See `docs/LIVE_TRADING_SYSTEM.md` for complete documentation.
