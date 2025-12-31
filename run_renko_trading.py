#!/usr/bin/env python3
"""
Config-driven Renko ML Trading Engine - PRODUCTION VERSION

Full integration:
1. Candle detection via CandleManager (granularity from config)
2. Renko building with pivot detection
3. Feature engineering (train/serve parity)
4. ML model scoring
5. Trade execution via RenkoTradeManager
6. Brick-count exits (close_trade on +/- bricks; no broker TP/SL)
"""
import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from api.oanda_api import OandaApi
from bot.renko_trade_manager import RenkoTradeManager
from bot.renko_trade_ledger import RenkoTradeLedger
from runtime.config_loader import load_model_routes, load_stream_config
from runtime.logging_setup import configure_logging
from runtime.loop import run_trading_loop
from runtime.model_loader import check_feature_parity, load_models_for_pairs
from runtime.setup import (
    build_pairs_config,
    build_pip_location_map,
    create_candle_manager,
    load_instruments,
    refresh_candles,
    start_candle_maintenance,
    start_renko_engine,
)

logger = configure_logging()


def main() -> None:
    parser = argparse.ArgumentParser(description="Renko ML Trading Engine")
    parser.add_argument("--config", default="config/stream_config.json", help="Path to stream config")
    parser.add_argument("--routes", default="config/model_routes.json", help="Path to model routes")
    args = parser.parse_args()

    print("=" * 70)
    print("Renko ML Trading Engine - PRODUCTION")
    print("=" * 70)

    print("\nLoading configuration...")
    config = load_stream_config(args.config)
    global_settings = config.global_settings
    pair_settings = config.pair_settings

    model_config = load_model_routes(args.routes)

    pairs = list(pair_settings.keys())
    granularity = global_settings.get("granularity", "M1")
    poll_interval = global_settings.get("poll_interval_seconds", 3)
    feature_k_lags = global_settings.get("feature_k_lags", 12)
    include_speed = global_settings.get("include_speed", True)
    pivot_lookback = global_settings.get("pivot_lookback", 10)
    ev_threshold_default = global_settings.get("ev_threshold", 0.0)
    print(f"OK: Monitoring {len(pairs)} pairs: {', '.join(pairs)}")
    print(f"   Granularity: {granularity}")
    print(f"   Poll interval: {poll_interval}s (candle checks)")

    print("\nGetting OANDA API instance...")
    api = OandaApi()
    print("OK: API ready")

    print("\nLoading instruments...")
    load_instruments(data_dir="./data", api=api)

    print("\nUpdating candle data to current time...")
    data_dir = "./data"
    start_dates = {
        pair: pair_settings.get(pair, {}).get("start_date") for pair in pairs
    }
    all_successful = refresh_candles(
        api=api,
        pairs=pairs,
        granularities=[granularity],
        data_dir=data_dir,
        start_dates=start_dates,
    )

    if all_successful:
        print("\nOK: Data update complete for configured granularities")
    else:
        print("\nWARNING: Some updates failed - check warnings above")

    print("\nSetting up CandleManager...")
    candle_manager = create_candle_manager(
        api=api,
        pair_settings=pair_settings,
        granularity=granularity,
    )
    print("OK: CandleManager ready")

    print("\nPreparing Renko configuration...")
    pip_location_map = build_pip_location_map(pairs=pairs, api=api)
    pairs_config = build_pairs_config(
        pair_settings=pair_settings,
        global_settings=global_settings,
        pip_location_map=pip_location_map,
    )
    print(f"OK: Configured {len(pairs_config)} pairs")

    print("\nLoading ML models...")
    models = load_models_for_pairs(
        model_config=model_config,
        pairs=pairs,
        ev_threshold_default=ev_threshold_default,
    )
    if models:
        check_feature_parity(
            models=models,
            k_lags=feature_k_lags,
            pivot_lookback=pivot_lookback,
            include_speed=include_speed,
        )
    else:
        print("WARNING: No models loaded - will detect pivots but not score them")

    print("\nSetting up Trade Manager...")
    global_risk = global_settings.get("risk") or {"type": "percent", "value": 0.005}
    risk_config = {
        pair: pair_settings.get(pair, {}).get("risk", global_risk) for pair in pairs
    }
    configured_strategy_id = global_settings.get("strategy_id")
    pair_strategy_ids = {}
    for pair in pairs:
        explicit_pair_id = pair_settings.get(pair, {}).get("strategy_id")
        if explicit_pair_id:
            pair_strategy_ids[pair] = explicit_pair_id
        elif configured_strategy_id:
            pair_strategy_ids[pair] = configured_strategy_id
        else:
            pair_strategy_ids[pair] = f"renko_ml_{pair.lower()}"
    allowed_strategy_ids = set(pair_strategy_ids.values())
    if configured_strategy_id:
        allowed_strategy_ids.add(configured_strategy_id)
    ev_rollover_enabled = bool(global_settings.get("ev_rollover", True))
    pivot_signals_enabled = bool(global_settings.get("enable_pivot_signals", True))
    trade_ledger = RenkoTradeLedger(allowed_strategy_ids=allowed_strategy_ids)
    trade_manager = RenkoTradeManager(
        api=api,
        risk_config=risk_config,
        max_concurrent_pairs=global_settings.get("max_concurrent_pairs", 10),
        signal_stale_minutes=global_settings.get("signal_stale_minutes", 3),
        target_bricks=global_settings.get("target_bricks", 4),
        stop_bricks=global_settings.get("stop_bricks", 2),
        strategy_id=configured_strategy_id or "renko_ml_v1",
        pair_strategy_ids=pair_strategy_ids,
        trade_event_logger=trade_ledger,
        trading_schedule=global_settings.get("trading_schedule"),
    )
    print(
        "OK: Trade Manager ready: %s bricks for / %s bricks against (brick exits)"
        % (trade_manager.target_bricks, trade_manager.stop_bricks)
    )
    if ev_rollover_enabled:
        print("   - EV rollover enabled (same-direction continue, opposite reverses)")
    else:
        print("   - EV rollover disabled (always close on exit)")
    if pivot_signals_enabled:
        print("   - Pivot signals enabled")
    else:
        print("   - Pivot signals disabled (EV-only entries)")

    maintenance_interval = 300
    start_candle_maintenance(
        api,
        pairs,
        granularity,
        start_dates=start_dates,
        data_dir=data_dir,
        interval_seconds=maintenance_interval,
    )

    print("\nStarting Renko Engine...")
    mode = global_settings.get("renko_mode", "candle_only")
    lookback = pivot_lookback
    print(f"   Mode: {mode}")
    print(f"   Pivot lookback: {lookback} (from config)")

    renko_engine, pivot_queue, pending_exit_signals, pending_exit_lock = start_renko_engine(
        api=api,
        candle_manager=candle_manager,
        pairs_config=pairs_config,
        mode=mode,
        granularity=granularity,
        lookback=lookback,
        poll_interval=poll_interval,
        feature_k_lags=feature_k_lags,
        include_speed=include_speed,
        trade_ledger=trade_ledger,
        target_bricks=trade_manager.target_bricks,
        stop_bricks=trade_manager.stop_bricks,
    )
    print(f"OK: Renko Engine started in '{mode}' mode with lookback={lookback}")

    print("\n" + "=" * 70)
    print("SYSTEM RUNNING - Full ML Trading Pipeline Active")
    print("=" * 70)
    print("\nMonitoring:")
    print(f"  - New {granularity} candles")
    print("  - Renko brick formation")
    print("  - Pivot detection")
    print("  - ML signal scoring")
    print("  - Trade execution")
    print("  - Brick-count exits (close_trade)")
    print("\nPress Ctrl+C to stop")
    print("=" * 70)

    try:
        run_trading_loop(
            api=api,
            pairs=pairs,
            granularity=granularity,
            models=models,
            renko_engine=renko_engine,
            trade_manager=trade_manager,
            trade_ledger=trade_ledger,
            pivot_queue=pivot_queue,
            pending_exit_signals=pending_exit_signals,
            pending_exit_lock=pending_exit_lock,
            ev_threshold_default=ev_threshold_default,
            pivot_signals_enabled=pivot_signals_enabled,
            ev_rollover_enabled=ev_rollover_enabled,
            global_settings=global_settings,
        )
    except KeyboardInterrupt:
        print("\n\nShutting down...")

        renko_engine.stop()
        renko_engine.join(timeout=5)

        summary = trade_manager.get_position_summary()
        print("\nFinal Status:")
        print(f"   Open Positions: {summary['total_positions']}")
        if summary["total_positions"] > 0:
            print(f"   Pairs: {', '.join(summary['pairs'])}")

        print("\nOK: Engine stopped")
        print("Bye")


if __name__ == "__main__":
    main()
