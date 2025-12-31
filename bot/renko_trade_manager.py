"""
RenkoTradeManager - Brick-count exits (no broker TP/SL)

This version places trades without broker-attached TP/SL and relies on
brick-count exits driven by the Renko engine. Local state only tracks
pivot de-duplication; position state is read directly from the broker each time.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set, Tuple
import threading

import pytz

from api.oanda_api import OandaApi, oanda_api
from bot.trade_event_logger import TradeEventLogger
from bot.trade_manager import place_trade, close_trade_by_pair, trade_is_open
from bot.trade_risk_calculator import calculate_risk_amount
from models.trade_decision import TradeDecision
import constants.defs as defs
from core.signals import SignalData, PivotPayload
from core.config_models import RiskConfig, TradingSchedule

logger = logging.getLogger(__name__)


class RenkoTradeManager:
    """
    Manages Renko pivot signal execution with brick-count exits.

    Key behaviors:
    - Deduplicates pivots
    - Places trades without broker-attached TP/SL
    - Relies on broker state for open-position checks (no local persistence)
    - Brick-count exits via close_trade
    """

    def __init__(
        self,
        api: OandaApi,
        risk_config: dict | RiskConfig | float | int,
        max_concurrent_pairs: int = 10,
        signal_stale_minutes: int = 3,
        target_bricks: int = 4,
        stop_bricks: int = 2,
        strategy_id: str = "renko_ml",
        pair_strategy_ids: Optional[Dict[str, str]] = None,
        state_path: str = "./state/renko_state.json",
        trade_event_logger: Optional[TradeEventLogger] = None,
        trading_schedule: Optional[dict | TradingSchedule] = None
    ):
        self.api = api
        self.risk_config = risk_config
        self.default_risk_config = {'type': 'percent', 'value': 0.005}
        self.max_concurrent_pairs = max_concurrent_pairs
        self.signal_stale_minutes = signal_stale_minutes
        # We still use brick multiples to size TP/SL distances, but they are kept in-memory.
        self.target_bricks = target_bricks
        self.stop_bricks = stop_bricks
        self.strategy_id = strategy_id
        self.pair_strategy_ids: Dict[str, str] = pair_strategy_ids or {}
        self.state_path = None  # State persistence disabled; rely on broker truth
        self.trade_event_logger = trade_event_logger
        self.trading_schedule = trading_schedule or {}

        # Tracking (signal de-duplication only; no local position state)
        self.traded_signals: Set[Tuple[str, datetime, str, int]] = set()  # (pair, time, source, brick_num)

        # Thread safety
        self.lock = threading.Lock()

        logger.info(
            f"RenkoTradeManager initialized: TP {target_bricks} bricks, SL {stop_bricks} bricks (brick-count exits)"
        )
        logger.info("Position persistence disabled; broker open trades are the source of truth")

    # ──────────────────────────
    # Helpers
    # ──────────────────────────
    def _pair_risk_config(self, pair: str) -> dict:
        """Return risk config for pair if mapping provided, else default."""
        if isinstance(self.risk_config, RiskConfig):
            return self.risk_config.to_dict()
        if isinstance(self.risk_config, dict) and 'type' in self.risk_config and 'value' in self.risk_config:
            return self.risk_config
        if isinstance(self.risk_config, dict):
            per_pair = self.risk_config.get(pair, self.default_risk_config)
            if isinstance(per_pair, RiskConfig):
                return per_pair.to_dict()
            return per_pair
        if isinstance(self.risk_config, (int, float)):
            return self.risk_config
        return self.default_risk_config

    def _strategy_id_for_pair(self, pair: str) -> str:
        """Return strategy_id for pair (configurable per pair, fallback to global)."""
        return self.pair_strategy_ids.get(pair, self.strategy_id)

    def _normalize_signal_time(self, signal_time: datetime | str) -> datetime:
        """Ensure timezone-aware signal time."""
        if isinstance(signal_time, str):
            signal_time = datetime.fromisoformat(signal_time.replace('Z', '+00:00'))
        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)
        return signal_time

    def _signal_key(self, pair: str, signal_time: datetime, signal_source: str, brick_num: Optional[int]) -> Tuple[str, datetime, str, int]:
        return (pair, signal_time, signal_source, int(brick_num or -1))

    def _normalize_direction(self, direction: int | str) -> Tuple[int, str]:
        if isinstance(direction, str):
            if direction.upper() == "BUY":
                return defs.BUY, "BUY"
            if direction.upper() == "SELL":
                return defs.SELL, "SELL"
        return (direction, "BUY" if direction == defs.BUY else "SELL")

    def _schedule_allows_entry(self, pivot_time: datetime) -> Tuple[bool, str]:
        schedule = self.trading_schedule or {}
        if isinstance(schedule, TradingSchedule):
            schedule = schedule.to_dict()
        disabled_weekdays = schedule.get("disabled_weekdays") or []
        if not disabled_weekdays:
            return True, ""

        tz_name = schedule.get("timezone") or "America/Chicago"
        try:
            tz = pytz.timezone(tz_name)
        except Exception:
            tz = pytz.timezone("America/Chicago")
            tz_name = "America/Chicago"

        if pivot_time.tzinfo is None:
            pivot_time = pivot_time.replace(tzinfo=timezone.utc)

        local_weekday = pivot_time.astimezone(tz).strftime("%A")
        disabled = {str(d).strip().lower() for d in disabled_weekdays}
        if local_weekday.lower() in disabled:
            return False, f"Trading disabled on {local_weekday} ({tz_name})"

        return True, ""

    # ──────────────────────────
    # Trading
    # ──────────────────────────
    def process_signal(
        self,
        signal_data: dict | SignalData,
        ml_score: float,
        threshold: float,
    ) -> bool:
        """
        Process a new model signal (pivot/ev/consensus) and potentially enter trade.

        signal_data keys: pair, time, direction, price, brick_size, signal_source
        """
        if isinstance(signal_data, SignalData):
            signal_data = signal_data.to_dict()

        pair = signal_data["pair"]
        signal_time = self._normalize_signal_time(signal_data["time"])
        signal_source = signal_data.get("signal_source", "pivot")
        brick_size = signal_data["brick_size"]
        pivot_type = signal_data.get("pivot_type")
        brick_num = signal_data.get("brick_num")
        granularity = signal_data.get("granularity", "S5")
        strategy_id = self._strategy_id_for_pair(pair)

        with self.lock:
            if ml_score < threshold:
                logger.info(f"{pair}: Score {ml_score:.3f} below threshold {threshold:.3f}, skipping")
                return False

            signal_age = datetime.now(timezone.utc) - signal_time
            if signal_age > timedelta(minutes=self.signal_stale_minutes):
                logger.warning(f"{pair}: Signal stale ({signal_age.seconds}s old), skipping")
                return False

            allowed, reason = self._schedule_allows_entry(signal_time)
            if not allowed:
                logger.info(f"{pair}: {reason}; skipping entry")
                return False

            signal_key = self._signal_key(pair, signal_time, signal_source, brick_num)
            if signal_key in self.traded_signals:
                logger.info(f"{pair}: Signal already traded at {signal_time} ({signal_source}), skipping")
                return False

            # Skip if broker already has an open trade for this pair (one trade per pair)
            broker_trade, lookup_ok = trade_is_open(pair, return_status=True)
            if not lookup_ok:
                logger.warning(f"{pair}: Could not verify open trades at broker; skipping to avoid duplicates")
                return False
            if broker_trade:
                logger.info(f"{pair}: Broker already has open trade {broker_trade.id}, skipping")
                return False

            # Enforce max concurrent pairs based on broker state
            open_trades = oanda_api.get_open_trades()
            if open_trades is None:
                logger.warning(f"{pair}: Could not fetch open trades; skipping entry to avoid duplicates")
                return False
            if len(open_trades) >= self.max_concurrent_pairs:
                logger.warning(f"Max concurrent pairs ({self.max_concurrent_pairs}) reached (broker has {len(open_trades)}), skipping")
                return False

            direction, signal_name = self._normalize_direction(signal_data["direction"])

            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 RENKO {signal_source.upper()} SIGNAL: {pair}")
            if pivot_type:
                logger.info(f"   Type: {pivot_type.upper()} → {signal_name}")
            else:
                logger.info(f"   Direction: {signal_name}")
            logger.info(f"   ML Score: {ml_score:.3f} (threshold: {threshold:.3f})")
            logger.info(f"   Price: {signal_data['price']:.5f}")
            logger.info(f"   Brick Size: {brick_size:.5f}")
            logger.info(f"   Age: {signal_age.seconds}s")
            logger.info(f"{'='*60}\n")

            # Current prices
            try:
                prices = self.api.get_prices([pair])
                if not prices or len(prices) == 0:
                    logger.error(f"{pair}: Could not get current prices")
                    return False

                price_obj = prices[0]
                current_bid = price_obj.bid
                current_ask = price_obj.ask
                spread = current_ask - current_bid

                entry_price = current_ask if direction == defs.BUY else current_bid
                logger.info(f"{pair}: Current Bid={current_bid:.5f} Ask={current_ask:.5f} Entry={entry_price:.5f}")

            except Exception as e:
                logger.error(f"{pair}: Error getting prices: {e}")
                return False

            # TP/SL based on brick multiples (in-memory for brick exits)
            if direction == defs.BUY:
                tp_price = entry_price + (self.target_bricks * brick_size)
                sl_price = entry_price - (self.stop_bricks * brick_size)
            else:
                tp_price = entry_price - (self.target_bricks * brick_size)
                sl_price = entry_price + (self.stop_bricks * brick_size)

            # Risk amount
            try:
                trade_risk = calculate_risk_amount(
                    self._pair_risk_config(pair),
                    log_message=lambda msg: logger.info(f"{pair}: {msg}")
                )
                logger.info(f"{pair}: Risk amount: ${trade_risk:.2f}")
            except Exception as e:
                logger.error(f"{pair}: Error calculating risk: {e}")
                return False

            # Build TradeDecision payload
            class Row:
                pass

            row = Row()
            row.PAIR = pair
            row.SIGNAL = direction
            row.strategy_id = strategy_id
            row.time = signal_time

            # Percentage gains/losses for reporting
            if direction == defs.BUY:
                row.GAIN = (tp_price - entry_price) / entry_price
                row.LOSS = (entry_price - sl_price) / entry_price
            else:
                row.GAIN = (entry_price - tp_price) / entry_price
                row.LOSS = (sl_price - entry_price) / entry_price

            row.SL = sl_price
            row.TP = tp_price
            row.ENTRY = entry_price
            row.diff_z = 0.0  # Not used for Renko
            row.SPREAD = spread
            row.mid_c = signal_data['price']
            row.granularity = granularity
            row.valid_until = datetime.now(timezone.utc) + timedelta(minutes=self.signal_stale_minutes)

            trade_decision = TradeDecision.from_row(row)

            # Place trade without broker TP/SL (brick-count exits)
            try:
                trade_id = place_trade(
                    trade_decision=trade_decision,
                    log_message=lambda msg, p=None: logger.info(f"{p or pair}: {msg}"),
                    log_error=lambda msg: logger.error(f"{pair}: {msg}"),
                    trade_risk=trade_risk,
                    return_id=True,
                    attach_broker_stops=False
                )

                if not trade_id:
                    logger.error(f"{pair}: Trade placement failed")
                    return False

                open_trade = oanda_api.get_open_trade(trade_id)
                if not open_trade:
                    open_trade, lookup_ok = trade_is_open(pair, return_status=True)
                    if not lookup_ok:
                        logger.warning(
                            f"{pair}: Trade placed (ID {trade_id}) but openTrades lookup failed; tracking anyway"
                        )
                    else:
                        logger.warning(f"{pair}: Trade placed (ID {trade_id}) but not yet visible in open trades")

                self.traded_signals.add(signal_key)

                trade_id_to_log = open_trade.id if open_trade else trade_id
                entry_to_log = float(open_trade.price) if open_trade else entry_price

                logger.info(f"✅ {pair}: Position opened - Trade ID: {trade_id_to_log}")
                logger.info(f"   Entry: {entry_to_log:.5f}")
                units_text = f"{abs(open_trade.currentUnits):.0f}" if open_trade else "unknown"
                logger.info(f"   Units: {units_text}")
                logger.info(f"   TP/SL (in-memory): TP {tp_price:.5f}, SL {sl_price:.5f}")

                if self.trade_event_logger:
                    entry_brick_index = brick_num
                    pivot_payload = signal_data.get("pivot_data") or {}
                    pivot_payload.update({
                        "signal_source": signal_source,
                    })
                    self.trade_event_logger.record_entry(
                        trade_id=trade_id_to_log,
                        pair=pair,
                        strategy_id=strategy_id,
                        direction=signal_name,
                        entry_price=float(entry_to_log),
                        tp_price=float(tp_price),
                        sl_price=float(sl_price),
                        units=int(abs(open_trade.currentUnits)) if open_trade else None,
                        entry_brick_index=entry_brick_index,
                        entry_brick_time=signal_time,
                        ml_score=float(ml_score),
                        threshold=float(threshold),
                        trade_risk=float(trade_risk),
                        risk_config=self._pair_risk_config(pair),
                        spread=float(spread),
                        pivot_data=pivot_payload or None,
                        target_bricks=self.target_bricks,
                        stop_bricks=self.stop_bricks,
                    )

                return True

            except Exception as e:
                logger.error(f"{pair}: Exception placing trade: {e}", exc_info=True)
                return False

    def process_pivot_signal(
        self,
        pivot_data: dict,
        ml_score: float,
        threshold: float
    ) -> bool:
        """
        Process a new pivot signal and potentially enter trade.

        Args:
            pivot_data: Dict with keys: pair, time, pivot_type, price, brick_size, features
            ml_score: ML model probability score
            threshold: Minimum score for entry

        Returns:
            True if trade was placed, False otherwise
        """
        pivot_type = pivot_data['pivot_type']
        if pivot_type.lower() == 'low':
            direction = defs.BUY
        elif pivot_type.lower() == 'high':
            direction = defs.SELL
        else:
            logger.error(f"{pivot_data['pair']}: Unknown pivot type '{pivot_type}'")
            return False

        signal_data = SignalData(
            pair=pivot_data["pair"],
            time=pivot_data["time"],
            direction=direction,
            price=pivot_data["price"],
            brick_size=pivot_data["brick_size"],
            signal_source="pivot",
            pivot_type=pivot_type,
            brick_num=pivot_data.get("brick_num"),
            granularity=pivot_data.get("granularity", "S5"),
            pivot_data=PivotPayload(
                type=str(pivot_type),
                time=str(pivot_data["time"]),
                price=float(pivot_data["price"]),
                brick_size=float(pivot_data["brick_size"]),
                brick_index=pivot_data.get("brick_num"),
            ),
        ).to_dict()

        return self.process_signal(signal_data, ml_score, threshold)

    # ──────────────────────────
    # Position management
    # ──────────────────────────
    def close_position(self, pair: str, reason: str) -> bool:
        """Close a broker position for the given pair."""
        with self.lock:
            strategy_id = self._strategy_id_for_pair(pair)
            existing_trade, lookup_ok = trade_is_open(pair, return_status=True)

            if not lookup_ok:
                logger.warning(f"{pair}: Could not verify broker position; aborting close")
                return False

            if existing_trade is None:
                logger.warning(f"{pair}: No broker position to close")
                return False

            logger.info(f"\n{'='*60}")
            logger.info(f"🛑 CLOSING POSITION: {pair}")
            logger.info(f"   Reason: {reason}")
            logger.info(f"   Trade ID: {existing_trade.id}")
            logger.info(f"{'='*60}\n")

            try:
                success = oanda_api.close_trade(existing_trade.id)
                if not success:
                    success = close_trade_by_pair(
                        pair,
                        log_message=lambda msg, p=None: logger.info(f"{p or pair}: {msg}"),
                        log_error=lambda msg: logger.error(f"{pair}: {msg}")
                    )

                if success:
                    logger.info(f"✅ {pair}: Position closed successfully")
                    if self.trade_event_logger:
                        direction = 'BUY' if existing_trade.currentUnits > 0 else 'SELL'
                        self.trade_event_logger.record_close(
                            trade_id=existing_trade.id,
                            pair=pair,
                            strategy_id=strategy_id,
                            direction=direction,
                            units=int(abs(existing_trade.currentUnits)),
                            entry_price=float(existing_trade.price),
                            reason=reason,
                        )
                else:
                    logger.error(f"{pair}: Failed to close position")
                return success

            except Exception as e:
                logger.error(f"{pair}: Exception closing position: {e}", exc_info=True)
                return False

    def get_position_summary(self) -> dict:
        """Get summary of open positions directly from the broker."""
        with self.lock:
            open_trades = oanda_api.get_open_trades() or []
            summary = {
                'total_positions': len(open_trades),
                'pairs': [t.instrument for t in open_trades],
                'positions': []
            }

            for ot in open_trades:
                units = int(getattr(ot, "currentUnits", 0))
                direction = 'BUY' if units > 0 else 'SELL'
                entry_price = float(getattr(ot, "price", 0.0))

                summary['positions'].append({
                    'pair': ot.instrument,
                    'direction': direction,
                    'entry_price': entry_price,
                    'tp_price': float(getattr(ot, "takeProfit", {}).get("price", 0.0)) if hasattr(ot, "takeProfit") else None,
                    'sl_price': float(getattr(ot, "stopLoss", {}).get("price", 0.0)) if hasattr(ot, "stopLoss") else None,
                    'trade_id': ot.id
                })

            return summary

    def cleanup_stale_pivot_memory(self, hours: int = 24):
        """Remove old signals from traded_signals set to prevent memory bloat."""
        with self.lock:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

            def _normalize(ts: datetime) -> datetime:
                if ts.tzinfo is None:
                    return ts.replace(tzinfo=timezone.utc)
                return ts.astimezone(timezone.utc)

            before_count = len(self.traded_signals)
            self.traded_signals = {
                (pair, time, source, brick_num)
                for pair, time, source, brick_num in self.traded_signals
                if _normalize(time) > cutoff
            }

            removed = before_count - len(self.traded_signals)
            if removed > 0:
                logger.info(f"Cleaned up {removed} old signal records (older than {hours}h)")

    def reconcile_broker_positions(self):
        """
        Compatibility hook for callers expecting periodic reconciliation.
        No local state to update because broker open trades are the source of truth.
        """
        logger.debug("reconcile_broker_positions: broker state is authoritative; nothing to reconcile")
