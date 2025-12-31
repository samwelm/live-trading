import json
from pathlib import Path

# Default fallbacks
DEFAULTS = {"brick_size": 0.001, "spread_pips": 1.8}
DEFAULTS_JPY = {"brick_size": 0.1, "spread_pips": 1.8}

def load_spread_config(path: Path = Path("spread.json")) -> dict:
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return {}

SPREAD_CONFIG = load_spread_config()

def get_pair_config(pair: str) -> dict:
    # start with JPY/non-JPY defaults
    cfg = DEFAULTS_JPY.copy() if "JPY" in pair else DEFAULTS.copy()
    # overlay spread.json if available
    override = SPREAD_CONFIG.get(pair, {})
    if override:
        cfg.update(
            {
                "brick_size": override.get("brick_size", cfg["brick_size"]),
                "spread_pips": override.get("spread_pips", cfg["spread_pips"]),
            }
        )
    return cfg
