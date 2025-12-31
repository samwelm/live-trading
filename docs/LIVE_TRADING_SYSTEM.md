# Live Renko ML Trading System

## Overview

Complete production-ready trading system integrating:
- M1 candle detection → Renko building → Pivot detection → Feature engineering
- ML model scoring with train/serve parity
- OANDA trade execution with NAV-based risk management
- Brick-count based exits (2 bricks against / 4 bricks for)

## Architecture

```
M1 Candles → Renko Builder → Pivot Detector → Feature Engineering
                                    ↓
                            ML Model Scoring
                                    ↓
                     (if score > threshold)
                                    ↓
                         Trade Execution
                                    ↓
                    Position + Brick Tracking
                                    ↓
                   Exit on 2 against / 4 for
```

## Components

### 1. **RenkoTradeManager** (`bot/renko_trade_manager.py`)

**Purpose**: Handles ML signal processing and trade execution

**Features**:
- Signal staleness validation (3 min max age)
- Pivot deduplication by (pair, timestamp, type)
- NAV-based position sizing
- Broker-attached TP/SL (no brick-count exits)
- Broker-truth position tracking
- Max concurrent pairs limit

**Key Methods**:
```python
# Process new pivot signal
process_pivot_signal(pivot_data, ml_score, threshold) -> bool

# Close position
close_position(pair, reason) -> bool

# Get position summary
get_position_summary() -> dict
```

**Entry Logic**:
1. Check ML score >= threshold
2. Validate signal not stale (< 3 min)
3. Check pivot not already traded (dedup)
4. Check no existing position for pair
5. Check max concurrent pairs limit
6. Calculate risk amount from NAV
7. Place market order without broker-attached TP/SL
8. Brick-count exits managed locally (RenkoTradeLedger + TradeManager)

---


### 2. **RenkoEngine / HybridRenkoEngine** (`prob_signals/renko_engine.py`, `prob_signals/hybrid_renko_engine.py`)

**Purpose**: Candle → Renko → Pivots → Features pipeline

**Integration**: Emits brick callbacks for ledger-driven exits

---

### 3. **Runner** (`run_renko_trading.py`)

**Purpose**: Main production entry point

**Startup Sequence**:
1. Load configuration (pairs, risk, models)
2. Load instruments (file → API priority)
3. Initialize OANDA API (singleton)
4. Setup CandleManager (M1 detection)
5. Load ML models per pair
6. Create RenkoTradeManager (risk config)
7. Create RenkoEngine (candle_only or hybrid)
8. Start main processing loop

**Main Loop**:
```python
while True:
    if pivot_queue.empty(): continue

    pivot = pivot_queue.get()

    # Score with ML model
    ml_score = model.predict_proba(features)[0, 1]

    # Attempt entry if high confidence
    if ml_score >= threshold:
        trade_manager.process_pivot_signal(pivot, ml_score, threshold)
```

---

## Configuration

### Stream Config (`config/stream_config.json`)

```json
{
  "global_settings": {
    "default_brick_size": 10,
    "risk": {
      "type": "percent",
      "value": 0.005
    },
    "max_concurrent_pairs": 10,
    "signal_stale_minutes": 3,
    "target_bricks": 4,
    "stop_bricks": 2,
    "renko_mode": "m1_only"
  },
  "pair_settings": {
    "EUR_USD": {
      "brick_size": 10,
      "pip_location": -4
    },
    "GBP_USD": {
      "brick_size": 0.0012,
      "pip_location": -4
    }
  }
}
```

**Key Settings**:
- `risk.type`: "percent" (NAV-based) or "fixed" (dollar amount)
- `risk.value`: 0.005 = 0.5% NAV per trade
- `max_concurrent_pairs`: Maximum open positions
- `signal_stale_minutes`: Max pivot age for entry (default: 3)
- `target_bricks`: Bricks for target exit (default: 4)
- `stop_bricks`: Bricks against stop exit (default: 2)
- `renko_mode`: "m1_only" (simple, 0-60s lag) or "hybrid" (tick monitoring, near-zero lag)

---

### Model Config (`config/model_routes.json`)

```json
{
  "routes": [
    {
      "name": "EUR_USD_v2",
      "model_path": "generalizers/eur_usd_10pip_model_v2.pkl",
      "threshold": 0.55,
      "trained_with_ffill_returns": false,
      "feature_parity_verified": true,
      "trained_date": "2025-12-04",
      "status": "ACTIVE"
    }
  ]
}
```

**Requirements**:
- `name`: Must start with pair symbol (e.g., "EUR_USD_*")
- `model_path`: Path to pickled model file
- `threshold`: Minimum ML score for entry (0.0-1.0)
- `feature_parity_verified`: Must be true (test passed)
- `status`: "ACTIVE" to use model

---

## Data Flow

### Pivot Signal → Trade Entry

1. **Pivot Detection**:
   ```
   M1 candles → Renko build → Pivot detect → Features → Queue
   ```

2. **ML Scoring**:
   ```python
   features = pivot['features']
   ml_score = model.predict_proba(features)[0, 1]
   trade_ok = ml_score >= threshold
   ```

3. **Trade Entry** (if `trade_ok`):
   ```python
   # Validate
   - Signal age < 3 min?
   - Pivot not already traded?
   - No existing position?
   - Under max pairs limit?

   # Calculate
   risk_amount = NAV × risk_percent
   entry_price = current ask/bid
   tp_price = entry ± (4 × brick_size)
   sl_price = entry ∓ (2 × brick_size)
   units = calculate_units(risk, entry, sl)

   # Execute
   trade_id = api.place_trade(pair, units, direction, tp, sl)

   # Track
   positions[pair] = RenkoPosition(...)
   traded_pivots.add((pair, time, type))
   ```

---

### Brick Update → Exit Check

1. **New Brick Forms**:
   ```
   M1 update → Renko rebuild → New brick → Notify monitor
   ```

2. **Brick Count Update**:
   ```python
   # For open position on this pair
   brick_move = (new_close - entry_close) / brick_size

   if direction == BUY:
       if brick_move > 0: bricks_for = abs(brick_move)
       else: bricks_against = abs(brick_move)
   else: # SELL
       if brick_move < 0: bricks_for = abs(brick_move)
       else: bricks_against = abs(brick_move)
   ```

3. **Exit Decision**:
   ```python
   if bricks_against >= 2:
       close_position(pair, "stop_2_bricks_against")

   if bricks_for >= 4:
       close_position(pair, "target_4_bricks_for")
   ```

---

## Risk Management

### Position Sizing

**Formula**:
```python
# 1. Get risk amount
risk_amount = NAV × risk_percent  # e.g., $10,000 × 0.005 = $50

# 2. Calculate price risk
price_risk = abs(entry_price - sl_price)

# 3. Get conversion factor (for non-USD pairs)
conv = get_home_conversion(pair, direction)

# 4. Risk per unit
risk_per_unit = price_risk × conv

# 5. Units
units = risk_amount / risk_per_unit

# 6. Margin check
if margin_required > available_margin × 0.3:
    scale_down units
```

**Example** (EUR_USD):
```
NAV = $10,000
Risk = 0.5% = $50
Entry = 1.10000 (BUY at ask)
SL = 1.09980 (2 bricks = 0.0020 below)
Price risk = 0.00020
Conv = 1.0 (EUR_USD quote is USD)
Risk/unit = 0.00020 × 1.0 = 0.00020
Units = $50 / 0.00020 = 250,000
```

---

### Limits

- **Per-trade risk**: 0.5% NAV (configurable)
- **Max concurrent pairs**: 10 (configurable)
- **Margin safety**: Uses max 30% of available margin per trade
- **Signal age**: 3 minutes max (pivot staleness)

---

## Execution Guarantees

### Train/Serve Parity

✅ **Feature Engineering**: Same `pct_change(fill_method=None)` in both repos
✅ **Test**: `tests/test_feature_parity.py` validates exact equality
✅ **Renko Building**: Identical `build_renko()` function
✅ **Pivot Detection**: Identical `detect_pivots()` function

---

### Signal Validation

✅ **Staleness**: Reject signals older than 3 min
✅ **Deduplication**: Track by (pair, timestamp, pivot_type) tuple
✅ **Position check**: One position per pair max
✅ **Concurrent limit**: Max 10 pairs (configurable)

---

### Exit Reliability

✅ **Broker TP/SL**: Exits handled by OANDA on entry  
✅ **Health checks**: Periodic OANDA reconciliation  
✅ **Trade logging**: JSONL audit trail for entries/brick events/closes

---

## Running the System

### Prerequisites

1. **Feature Parity Test**:
   ```bash
   python tests/test_feature_parity.py
   # Must see: "🎉 FEATURE PARITY TEST PASSED!"
   ```

2. **Models Trained**: With aligned features (`fill_method=None`)

3. **Configuration**: `config/stream_config.json` and `config/model_routes.json`

---

### Startup

```bash
python run_renko_trading.py
```

**Expected Output**:
```
======================================================================
M1 Renko ML Trading Engine - PRODUCTION
======================================================================

📋 Loading configuration...
✅ Monitoring 2 pairs: EUR_USD, GBP_USD

📊 Loading instruments...
✅ Loaded 28 instruments from file

🔌 Getting OANDA API instance...
✅ API ready

🕐 Setting up CandleManager...
✅ CandleManager ready

⚙️  Preparing Renko configuration...
✅ Configured 2 pairs

🤖 Loading ML models...
✅ Loaded model for EUR_USD: eur_usd_10pip_model_v2.pkl
✅ Loaded model for GBP_USD: gbp_usd_10pip_model_v2.pkl

💼 Setting up Trade Manager...
✅ Trade Manager ready: 4 bricks for / 2 bricks against

👁️  Setting up Brick Monitor...
✅ Brick Monitor started

🚀 Starting Renko Engine...
✅ Renko Engine started in 'm1_only' mode

======================================================================
🟢 SYSTEM RUNNING - Full ML Trading Pipeline Active
======================================================================

Monitoring:
  • New M1 candles
  • Renko brick formation
  • Pivot detection
  • ML signal scoring
  • Trade execution
  • Brick-count exits

Press Ctrl+C to stop
======================================================================
```

---

### Monitoring

**Pivot Detection**:
```
🎯 NEW PIVOT DETECTED: EUR_USD
   Type: LOW → BUY
   ML Score: 0.632 (threshold: 0.550)
   Price: 1.10125
   Brick Size: 0.00100
   Age: 42s
   ⚡ HIGH CONFIDENCE SIGNAL - Attempting entry
```

**Trade Entry**:
```
✅ EUR_USD: Position opened - Trade ID: 12345
   Entry: 1.10130
   Units: 250000
   Brick tracking: 4 for / 2 against
```

**Brick Updates**:
```
EUR_USD: Brick update - For: 1, Against: 0
EUR_USD: Brick update - For: 2, Against: 0
```

**Exit**:
```
🛑 CLOSING POSITION: EUR_USD
   Reason: target_4_bricks_for
   Trade ID: 12345
   Bricks For: 4
   Bricks Against: 0
   Duration: 1847s
✅ EUR_USD: Position closed successfully
```

---

## Operational Notes

### What's Different from Backtest

| Aspect | Backtest | Live |
|--------|----------|------|
| Entry price | Pivot brick Close (historical) | Current market ask/bid |
| Entry timing | Instant at pivot brick | Lag: detect → score → execute |
| Exit tracking | Look ahead 50 bricks | Count forward from entry |
| Fills | Perfect, instant | Slippage, partial fills possible |
| Costs | Fixed cost_bricks | Variable spreads |

**Accepted**: 0.1-0.3 brick slippage is normal
**Alert**: > 0.5 brick slippage = execution issues

---

### Safety Features

1. **Signal Staleness**: 3-min max age prevents trading on old pivots
2. **Pivot Deduplication**: Prevents duplicate trades on same pivot
3. **Position Limits**: Max 1 per pair, max 10 total
4. **Margin Safety**: Uses max 30% of available margin
5. **Broker TP/SL**: Exits managed by OANDA on entry
6. **Health Checks**: Periodic position reconciliation

---

### Troubleshooting

**No trades executing?**
- Check ML scores reaching threshold
- Verify model files loaded correctly
- Check signal staleness (< 3 min)
- Verify max pairs limit not hit

**Positions not closing?**
- Confirm TP/SL were attached on entry
- Check broker trade status in OANDA
- Check logs for trade placement errors

**Execution errors?**
- Verify OANDA API credentials
- Check margin availability
- Validate instrument precision

---

## Critical Files

**Core**:
- `run_renko_trading.py` - Main entry point
- `bot/renko_trade_manager.py` - Trade execution + position tracking
- `prob_signals/renko_engine.py` - Candle → Renko → Pivots pipeline
- `prob_signals/hybrid_renko_engine.py` - Candle + tick hybrid Renko engine

**Supporting**:
- `bot/trade_manager.py` - OANDA order placement
- `bot/trade_risk_calculator.py` - NAV-based position sizing
- `prob_signals/renko_ml_features.py` - Feature engineering (parity critical!)

**Config**:
- `config/stream_config.json` - Pairs, risk, brick thresholds
- `config/model_routes.json` - Model paths and thresholds

**Tests**:
- `tests/test_feature_parity.py` - Train/serve parity validation (RUN BEFORE DEPLOY!)

---

## Next Steps

### Before First Live Trade

1. ✅ Run feature parity test
2. ✅ Verify all models trained with `fill_method=None`
3. ⚠️ **Paper trade first** - monitor for 24-48 hours
4. ⚠️ **Start with 1-2 pairs** - validate before scaling
5. ⚠️ **Low risk** - use 0.1-0.2% NAV initially
6. ⚠️ **Monitor closely** - watch logs for errors

### Ongoing

- Run parity test after any `feature_builder.py` changes
- Review execution quality monthly (slippage, fill rates)
- Update models quarterly with fresh data
- Clean up pivot memory weekly (`cleanup_stale_pivot_memory()`)

---

## Status

**Current**: ✅ Production-ready with full integration
**Tested**: Feature parity validated, components unit tested
**Missing**: Live trading history, production hardening

**Recommendation**: Paper trade for 1 week before going live with real capital.
