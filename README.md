# Live Renko ML Trading

Live trading engine for Renko-based ML signals on OANDA. This repo is trimmed to runtime-only code (no backtests or analysis scripts).

## Quick Start

1. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configure environment

Copy `.env.example` to `.env` and set your credentials:
- `API_KEY`
- `ACCOUNT_ID`
- `OANDA_URL` (e.g. `https://api-fxpractice.oanda.com/v3`)

3. Update configs

- `config/stream_config.json` (pairs, brick sizes, risk)
- `config/model_routes.json` (model bundles + thresholds)

4. Run

```bash
python run_renko_trading.py
```

See `QUICKSTART_LIVE_TRADING.md` for pre-flight checks and expected logs.

## Runtime Directories

- `data/` stores candle caches and `instruments.json` (auto-generated if missing)
- `state/` stores the Renko brick ledger (SQLite)
- `logs/` stores runtime logs

## Notes

- Start on a practice account and monitor for at least 24–48 hours.
- Models must be compatible with the feature set produced by `prob_signals/renko_ml_features.py`.
