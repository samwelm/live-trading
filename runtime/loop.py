import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from core.ledger_rows import EvUpdateRow
from core.signals import PivotPayload, PivotSignal, SignalData

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PendingSignal:
    signal_data: SignalData
    ml_score: float
    threshold: float


@dataclass(slots=True)
class PendingReverseSignal:
    signal_data: SignalData
    ml_score: float
    threshold: float
    pair: str
    attempts: int
    next_attempt_ts: float
    expires_ts: float


@dataclass(slots=True)
class EvSignalInfo:
    direction: str
    score: float
    threshold: float
    time: Optional[datetime]
    brick_size: float
    price: Optional[float]


def run_trading_loop(
    *,
    api,
    pairs: list[str],
    granularity: str,
    models,
    renko_engine,
    trade_manager,
    trade_ledger,
    pivot_queue,
    pending_exit_signals,
    pending_exit_lock,
    ev_threshold_default: float,
    pivot_signals_enabled: bool,
    ev_rollover_enabled: bool,
    global_settings: Any,
) -> None:
    last_trade_refresh_ts = 0.0
    trade_refresh_interval = 15
    pending_pivot_signals: dict[tuple[str, int], PendingSignal] = {}
    ev_startup_backfill = max(int(global_settings.get("ev_startup_backfill", 1)), 0)
    last_ev_index: dict[str, Optional[int]] = {}
    for pair in pairs:
        state = renko_engine.get_state(pair)
        df = state.feature_df_complete if state else None
        if df is None or df.empty:
            last_ev_index[pair] = None
            continue
        last_ev_index[pair] = max(len(df) - ev_startup_backfill - 1, -1)

    pending_reverse_signals: list[PendingReverseSignal] = []
    reverse_retry_seconds = 5
    reverse_max_attempts = 6
    ev_signal_cache: dict[tuple[str, int], EvSignalInfo] = {}
    ev_signal_cache_keep = 50
    rollover_cap_strategy = str(global_settings.get("rollover_cap_strategy", "") or "").lower()
    rollover_cap_limit = max(int(global_settings.get("rollover_cap_limit", 0) or 0), 0)
    cap_rollover_enabled = rollover_cap_strategy == "cap_at_consecutive" and rollover_cap_limit > 0

    while True:
        now_ts = time.time()
        if now_ts - last_trade_refresh_ts >= trade_refresh_interval:
            trade_ledger.refresh_open_trades(
                api,
                target_bricks=trade_manager.target_bricks,
                stop_bricks=trade_manager.stop_bricks,
            )
            last_trade_refresh_ts = now_ts

        while not pivot_queue.empty():
            pivot = pivot_queue.get()
            if not pivot_signals_enabled:
                continue

            logger.info("\n%s", "=" * 60)
            pivot_signal = pivot if isinstance(pivot, PivotSignal) else PivotSignal.from_dict(pivot)

            logger.info("NEW PIVOT DETECTED: %s", pivot_signal.pair)
            logger.info("   Type: %s", pivot_signal.pivot_type.upper())
            logger.info("   Price: %.5f", pivot_signal.price)
            logger.info("   Time: %s", pivot_signal.time)
            logger.info("   Brick Size: %.5f", pivot_signal.brick_size)

            pair = pivot_signal.pair

            if pivot_signal.pivot_type == "low":
                expected_direction = "BUY"
            else:
                expected_direction = "SELL"

            logger.info("   Direction: %s", expected_direction)

            model_info = models.get(pair)
            if not model_info or not model_info.pivot_model:
                logger.info("   No pivot model available for %s", pair)
                logger.info("%s\n", "=" * 60)
                continue

            try:
                feature_dict = pivot_signal.features
                feature_cols = model_info.pivot_features
                if not feature_cols:
                    logger.warning("%s: Pivot feature list empty; skipping", pair)
                    logger.info("%s\n", "=" * 60)
                    continue

                df_row = pd.DataFrame(
                    [{col: feature_dict.get(col, 0) for col in feature_cols}]
                ).fillna(0)
                pivot_model = model_info.pivot_model
                ml_score = float(pivot_model.predict_proba(df_row)[:, 1][0])
                threshold_val = model_info.pivot_threshold
                trade_ok = ml_score >= threshold_val

                renko_engine.last_scored_pivots[pair] = {
                    "score": ml_score,
                    "direction": expected_direction,
                    "time": pivot_signal.time,
                    "price": pivot_signal.price,
                }

                decision = "PASS" if trade_ok else "FAIL"
                logger.info(
                    "   Pivot Score: %.3f vs threshold: %.3f -> %s",
                    ml_score,
                    threshold_val,
                    decision,
                )
                print(
                    f"   Pivot Score: {ml_score:.3f} vs {threshold_val:.3f} -> {decision}"
                )

                if trade_ok:
                    brick_num = pivot_signal.brick_num
                    if brick_num is None and pivot_signal.brick_idx is not None:
                        brick_num = int(pivot_signal.brick_idx) + 1
                    signal_data = SignalData(
                        pair=pair,
                        time=pivot_signal.time,
                        direction=expected_direction,
                        price=pivot_signal.price,
                        brick_size=pivot_signal.brick_size,
                        signal_source="pivot",
                        pivot_type=pivot_signal.pivot_type,
                        brick_num=brick_num,
                        granularity=granularity,
                        pivot_data=PivotPayload(
                            type=pivot_signal.pivot_type,
                            time=str(pivot_signal.time),
                            price=float(pivot_signal.price),
                            brick_size=float(pivot_signal.brick_size),
                            brick_index=brick_num,
                        ),
                    )

                    if model_info.ev_models:
                        key = (pair, brick_num)
                        pending_pivot_signals[key] = PendingSignal(
                            signal_data=signal_data,
                            ml_score=ml_score,
                            threshold=threshold_val,
                        )
                    else:
                        logger.info("   Attempting pivot trade entry")
                        success = trade_manager.process_signal(
                            signal_data=signal_data,
                            ml_score=ml_score,
                            threshold=threshold_val,
                        )
                        if success:
                            logger.info("   Trade entered successfully")
                        else:
                            logger.info("   Trade entry failed or skipped")
                else:
                    logger.info("   Score below threshold - No entry")

            except Exception as exc:
                logger.error("   Error scoring pivot: %s", exc, exc_info=True)

            logger.info("%s\n", "=" * 60)

        for pair, model_info in models.items():
            ev_models = model_info.ev_models
            if not ev_models:
                continue

            state = renko_engine.get_state(pair)
            if not state or state.feature_df_complete is None or state.feature_df_complete.empty:
                continue

            df = state.feature_df_complete
            last_pos = last_ev_index.get(pair, -1)
            if last_pos is None:
                last_pos = max(len(df) - ev_startup_backfill - 1, -1)
                last_ev_index[pair] = last_pos
            if last_pos >= len(df) - 1:
                continue

            feature_cols = model_info.ev_features
            if not feature_cols:
                logger.warning("%s: EV feature list empty; skipping", pair)
                last_ev_index[pair] = len(df) - 1
                continue

            model_long = ev_models["long"]
            model_short = ev_models["short"]
            ev_threshold = model_info.ev_threshold or ev_threshold_default

            ev_updates: list[EvUpdateRow] = []
            for pos in range(last_pos + 1, len(df)):
                row = df.iloc[pos]
                X_ev = pd.DataFrame(
                    [{col: row.get(col, 0) for col in feature_cols}]
                ).fillna(0)
                pred_long = float(model_long.predict(X_ev)[0])
                pred_short = float(model_short.predict(X_ev)[0])

                brick_num = int(row.name) + 1
                ev_long = pred_long if pd.notna(pred_long) else None
                ev_short = pred_short if pd.notna(pred_short) else None
                ev_signal = None
                if ev_long is not None and ev_short is not None:
                    if ev_long > ev_short and ev_long > ev_threshold:
                        ev_direction = "BUY"
                        ev_score = ev_long
                        ev_signal = 1
                    elif ev_short > ev_long and ev_short > ev_threshold:
                        ev_direction = "SELL"
                        ev_score = ev_short
                        ev_signal = -1
                    else:
                        ev_signal = 0

                ev_updates.append(EvUpdateRow(pair, brick_num, ev_long, ev_short, ev_signal))
                if ev_signal not in (1, -1):
                    continue

                price_val = row.get("Close")
                if pd.isna(price_val):
                    price_val = row.get("mid_c")
                if pd.isna(price_val):
                    price_val = state.last_brick_close if state else None
                price_for_signal = float(price_val) if pd.notna(price_val) else None
                if price_for_signal is None and state and state.last_brick_close is not None:
                    price_for_signal = float(state.last_brick_close)
                ev_signal_cache[(pair, brick_num)] = EvSignalInfo(
                    direction=ev_direction,
                    score=float(ev_score),
                    threshold=float(ev_threshold),
                    time=row.get("time"),
                    brick_size=float(row.get("brick_size", state.brick_size)),
                    price=price_for_signal,
                )
                signal_data = SignalData(
                    pair=pair,
                    time=row.get("time"),
                    direction=ev_direction,
                    price=price_for_signal,
                    brick_size=row.get("brick_size", state.brick_size),
                    signal_source="ev",
                    brick_num=brick_num,
                    granularity=granularity,
                    pivot_data=PivotPayload(
                        signal_source="ev",
                        brick_index=brick_num,
                    ),
                )

                pivot_key = (pair, brick_num)
                pivot_signal = pending_pivot_signals.pop(pivot_key, None)
                if pivot_signal:
                    if pivot_signal.signal_data.direction == ev_direction:
                        logger.info(
                            "%s: Consensus signal on brick %s (%s)",
                            pair,
                            brick_num,
                            ev_direction,
                        )
                        pivot_payload = pivot_signal.signal_data.pivot_data or PivotPayload()
                        pivot_payload.pivot_score = float(pivot_signal.ml_score)
                        pivot_payload.pivot_threshold = float(pivot_signal.threshold)
                        pivot_payload.ev_score = float(ev_score)
                        pivot_payload.ev_threshold = float(ev_threshold)
                        pivot_payload.signal_source = "consensus"
                        consensus_data = SignalData(
                            pair=signal_data.pair,
                            time=signal_data.time,
                            direction=signal_data.direction,
                            price=signal_data.price,
                            brick_size=signal_data.brick_size,
                            signal_source="consensus",
                            brick_num=signal_data.brick_num,
                            granularity=signal_data.granularity,
                            pivot_data=pivot_payload,
                        )
                        trade_manager.process_signal(
                            signal_data=consensus_data,
                            ml_score=ev_score,
                            threshold=ev_threshold,
                        )
                    else:
                        logger.info(
                            "%s: EV/Pivot conflict on brick %s "
                            "(EV %s, Pivot %s) -> EV only",
                            pair,
                            brick_num,
                            ev_direction,
                            pivot_signal.signal_data.direction,
                        )
                        trade_manager.process_signal(
                            signal_data=signal_data,
                            ml_score=ev_score,
                            threshold=ev_threshold,
                        )
                else:
                    trade_manager.process_signal(
                        signal_data=signal_data,
                        ml_score=ev_score,
                        threshold=ev_threshold,
                    )

            if ev_updates:
                trade_ledger.update_ev_rows(ev_updates)

            last_ev_index[pair] = len(df) - 1

            if state and state.feature_df_full is not None:
                max_brick = len(state.feature_df_full)
                cutoff = max_brick - ev_signal_cache_keep
                if cutoff > 0:
                    for key in [
                        k for k in ev_signal_cache.keys() if k[0] == pair and k[1] < cutoff
                    ]:
                        ev_signal_cache.pop(key, None)

        for key, pivot_signal in list(pending_pivot_signals.items()):
            pair, brick_num = key
            last_pos = last_ev_index.get(pair, -1)
            if last_pos >= 0 and brick_num <= last_pos + 1:
                logger.info("%s: Pivot-only entry on brick %s", pair, brick_num)
                trade_manager.process_signal(
                    signal_data=pivot_signal.signal_data,
                    ml_score=pivot_signal.ml_score,
                    threshold=pivot_signal.threshold,
                )
                pending_pivot_signals.pop(key, None)

        exit_batch = []
        with pending_exit_lock:
            while pending_exit_signals:
                exit_batch.append(pending_exit_signals.popleft())

        for signal in exit_batch:
            pair = signal.get("pair")
            trade_id = signal.get("trade_id")
            exit_brick_index = signal.get("exit_brick_index")
            direction = str(signal.get("direction") or "").upper()
            reason = signal.get("reason", "exit")
            segment_outcome = "NEUTRAL"
            reason_lower = str(reason).lower()
            if "target" in reason_lower:
                segment_outcome = "WIN"
            elif "stop" in reason_lower:
                segment_outcome = "LOSS"

            logger.info(
                "%s: Exit trigger %s "
                "(net=%s for=%s against=%s)",
                pair,
                reason,
                signal.get("net_bricks"),
                signal.get("bricks_for"),
                signal.get("bricks_against"),
            )

            if not ev_rollover_enabled or exit_brick_index is None:
                success = trade_manager.close_position(pair, reason)
                if not success and trade_id:
                    trade_ledger.clear_pending_close(trade_id)
                continue

            brick_info = trade_ledger.get_brick_ev_info(pair, exit_brick_index)
            ev_signal_value = brick_info.get("ev_signal") if brick_info else None
            if ev_signal_value not in (1, -1):
                success = trade_manager.close_position(pair, reason)
                if not success and trade_id:
                    trade_ledger.clear_pending_close(trade_id)
                continue

            ev_direction = "BUY" if ev_signal_value == 1 else "SELL"
            if ev_direction == direction:
                if cap_rollover_enabled and trade_id:
                    should_cap, projected = trade_ledger.should_cap_rollover(
                        trade_id,
                        segment_outcome,
                        cap_limit=rollover_cap_limit,
                    )
                    if should_cap:
                        cap_reason = f"cap_{rollover_cap_limit}_consecutive_losses"
                        logger.info(
                            "%s: Cap triggered after %s consecutive losses (exit=%s) - closing",
                            pair,
                            projected,
                            reason,
                        )
                        success = trade_manager.close_position(pair, cap_reason)
                        if not success and trade_id:
                            trade_ledger.clear_pending_close(trade_id)
                        continue
                ev_signal = ev_signal_cache.get((pair, exit_brick_index))
                rollover_time = None
                if ev_signal:
                    rollover_time = ev_signal.time
                elif brick_info:
                    rollover_time = brick_info.get("brick_time")
                if trade_id:
                    trade_ledger.rollover_trade(
                        trade_id,
                        exit_brick_index,
                        rollover_time,
                        segment_outcome=segment_outcome,
                    )
                consecutive = (
                    trade_ledger.get_consecutive_negative_count(trade_id)
                    if trade_id
                    else None
                )
                logger.info(
                    "%s: EV rollover on brick %s (%s) -> reset +%s/-%s (segment=%s, losses=%s)",
                    pair,
                    exit_brick_index,
                    direction,
                    trade_manager.target_bricks,
                    trade_manager.stop_bricks,
                    segment_outcome,
                    consecutive if consecutive is not None else "n/a",
                )
                continue

            success = trade_manager.close_position(pair, reason)
            if not success:
                if trade_id:
                    trade_ledger.clear_pending_close(trade_id)
                continue

            ev_signal = ev_signal_cache.get((pair, exit_brick_index))
            reverse_time = ev_signal.time if ev_signal else None
            reverse_price = ev_signal.price if ev_signal else None
            reverse_brick_size = ev_signal.brick_size if ev_signal else None
            reverse_score = ev_signal.score if ev_signal else None
            reverse_threshold = ev_signal.threshold if ev_signal else None
            if reverse_time is None and brick_info:
                reverse_time = brick_info.get("brick_time")
            if reverse_price is None and brick_info:
                reverse_price = brick_info.get("brick_close") or brick_info.get("brick_open")
            if reverse_brick_size is None:
                state = renko_engine.get_state(pair)
                if state and state.brick_size is not None:
                    reverse_brick_size = float(state.brick_size)
            if reverse_score is None and brick_info:
                reverse_score = (
                    brick_info.get("ev_long") if ev_signal_value == 1 else brick_info.get("ev_short")
                )
            if reverse_threshold is None:
                model_info = models.get(pair)
                reverse_threshold = (
                    model_info.ev_threshold if model_info else ev_threshold_default
                )
            if (
                reverse_time is None
                or reverse_price is None
                or reverse_brick_size is None
                or reverse_score is None
            ):
                logger.warning(
                    "%s: Missing EV data for reverse on brick %s; skipping re-entry",
                    pair,
                    exit_brick_index,
                )
                continue
            reverse_data = SignalData(
                pair=pair,
                time=reverse_time,
                direction=ev_direction,
                price=reverse_price,
                brick_size=reverse_brick_size,
                signal_source="ev_rollover_reverse",
                brick_num=exit_brick_index,
                granularity=granularity,
                pivot_data=PivotPayload(
                    signal_source="ev",
                    brick_index=exit_brick_index,
                    rollover=True,
                ),
            )
            pending_reverse_signals.append(
                PendingReverseSignal(
                    signal_data=reverse_data,
                    ml_score=reverse_score,
                    threshold=reverse_threshold,
                    pair=pair,
                    attempts=0,
                    next_attempt_ts=time.time(),
                    expires_ts=time.time()
                    + (reverse_retry_seconds * reverse_max_attempts),
                )
            )

        if pending_reverse_signals:
            now_ts = time.time()
            still_pending = []
            for item in pending_reverse_signals:
                if now_ts < item.next_attempt_ts:
                    still_pending.append(item)
                    continue
                success = trade_manager.process_signal(
                    signal_data=item.signal_data,
                    ml_score=item.ml_score,
                    threshold=item.threshold,
                )
                if success:
                    logger.info(
                        "%s: Reverse entry placed on brick %s",
                        item.pair,
                        item.signal_data.brick_num,
                    )
                    continue
                item.attempts += 1
                if item.attempts < reverse_max_attempts and now_ts < item.expires_ts:
                    item.next_attempt_ts = now_ts + reverse_retry_seconds
                    still_pending.append(item)
                else:
                    logger.warning(
                        "%s: Reverse entry dropped after %s attempts",
                        item.pair,
                        item.attempts,
                    )
            pending_reverse_signals[:] = still_pending

        time.sleep(1)
