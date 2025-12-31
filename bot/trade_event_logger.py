"""
Trade event logging for Renko live trading.

Writes append-only JSONL events and maintains a compact index for restart safety.
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

from dateutil import parser as date_parser

logger = logging.getLogger(__name__)


def _to_datetime(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif hasattr(value, "to_pydatetime"):
        dt = value.to_pydatetime()
    else:
        dt = date_parser.parse(str(value))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_dt(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


@dataclass(slots=True)
class TradeState:
    trade_id: str
    pair: str
    strategy_id: Optional[str]
    direction: str
    entry_time: Optional[datetime]
    entry_brick_index: Optional[int]
    last_seen_brick_index: Optional[int]
    last_seen_brick_time: Optional[datetime]
    bricks_for: int = 0
    bricks_against: int = 0

    def to_dict(self) -> dict:
        return {
            "pair": self.pair,
            "strategy_id": self.strategy_id,
            "direction": self.direction,
            "entry_time": _format_dt(self.entry_time),
            "entry_brick_index": self.entry_brick_index,
            "last_seen_brick_index": self.last_seen_brick_index,
            "last_seen_brick_time": _format_dt(self.last_seen_brick_time),
            "bricks_for": self.bricks_for,
            "bricks_against": self.bricks_against,
        }

    @classmethod
    def from_dict(cls, trade_id: str, payload: dict) -> "TradeState":
        return cls(
            trade_id=trade_id,
            pair=payload.get("pair", ""),
            strategy_id=payload.get("strategy_id"),
            direction=payload.get("direction", "BUY"),
            entry_time=_to_datetime(payload.get("entry_time")),
            entry_brick_index=payload.get("entry_brick_index"),
            last_seen_brick_index=payload.get("last_seen_brick_index"),
            last_seen_brick_time=_to_datetime(payload.get("last_seen_brick_time")),
            bricks_for=int(payload.get("bricks_for", 0)),
            bricks_against=int(payload.get("bricks_against", 0)),
        )


class TradeEventLogger:
    def __init__(
        self,
        events_path: str = "./state/trade_events.jsonl",
        index_path: str = "./state/trade_events.index.json",
        allowed_strategy_ids: Optional[Set[str]] = None,
    ):
        self.events_path = Path(events_path)
        self.index_path = Path(index_path)
        self.allowed_strategy_ids = set(allowed_strategy_ids or [])
        self.lock = threading.Lock()
        self.seen_event_ids: Set[str] = set()
        self.trades: Dict[str, TradeState] = {}
        self.trades_by_pair: Dict[str, Set[str]] = {}
        self.pending_closes: Set[str] = set()
        self.renko_engine = None
        self._load_index()

    def attach_engine(self, renko_engine) -> None:
        self.renko_engine = renko_engine

    def _append_event(self, payload: dict) -> None:
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True))
            handle.write("\n")

    def _load_index(self) -> None:
        if not self.index_path.exists():
            return
        try:
            with self.index_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            self.seen_event_ids = set(data.get("seen_event_ids", []))
            self.trades = {
                trade_id: TradeState.from_dict(trade_id, payload)
                for trade_id, payload in data.get("trades", {}).items()
            }
            self.trades_by_pair = {}
            for trade_id, state in self.trades.items():
                self.trades_by_pair.setdefault(state.pair, set()).add(trade_id)
        except Exception as exc:
            logger.warning(f"Failed to load trade event index: {exc}")

    def _write_index(self) -> None:
        data = {
            "seen_event_ids": sorted(self.seen_event_ids),
            "trades": {trade_id: state.to_dict() for trade_id, state in self.trades.items()},
        }
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.index_path.with_suffix(self.index_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        tmp_path.replace(self.index_path)

    def _track_trade(self, state: TradeState) -> None:
        self.trades[state.trade_id] = state
        self.trades_by_pair.setdefault(state.pair, set()).add(state.trade_id)

    def _untrack_trade(self, trade_id: str) -> None:
        state = self.trades.pop(trade_id, None)
        if not state:
            return
        pair_set = self.trades_by_pair.get(state.pair)
        if pair_set:
            pair_set.discard(trade_id)
            if not pair_set:
                self.trades_by_pair.pop(state.pair, None)
        self.pending_closes.discard(trade_id)

    def clear_pending_close(self, trade_id: str) -> None:
        with self.lock:
            self.pending_closes.discard(trade_id)

    def _current_brick_index(self, pair: str) -> Optional[int]:
        if not self.renko_engine:
            return None
        state = self.renko_engine.get_state(pair)
        if not state:
            return None
        if state.last_brick_count:
            return int(state.last_brick_count)
        if state.feature_df_complete is not None and not state.feature_df_complete.empty:
            return len(state.feature_df_complete)
        if state.feature_df_full is not None and not state.feature_df_full.empty:
            return len(state.feature_df_full)
        return None

    def record_entry(
        self,
        trade_id: str,
        pair: str,
        strategy_id: Optional[str],
        direction: str,
        entry_price: Optional[float],
        tp_price: Optional[float],
        sl_price: Optional[float],
        units: Optional[int],
        entry_brick_index: Optional[int],
        entry_brick_time: Optional[datetime],
        ml_score: Optional[float],
        threshold: Optional[float],
        trade_risk: Optional[float],
        risk_config: Optional[dict],
        spread: Optional[float],
        pivot_data: Optional[dict],
        recovered: bool = False,
        entry_time: Optional[datetime] = None,
    ) -> None:
        event_id = f"{trade_id}:entry"

        now = entry_time or datetime.now(timezone.utc)
        brick_index = entry_brick_index or self._current_brick_index(pair)

        payload = {
            "event_id": event_id,
            "event": "entry",
            "timestamp": _format_dt(now),
            "trade_id": trade_id,
            "pair": pair,
            "strategy_id": strategy_id,
            "direction": direction,
            "entry_price": float(entry_price) if entry_price is not None else None,
            "tp_price": float(tp_price) if tp_price is not None else None,
            "sl_price": float(sl_price) if sl_price is not None else None,
            "units": int(units) if units is not None else None,
            "entry_brick_index": int(brick_index) if brick_index is not None else None,
            "entry_brick_time": _format_dt(_to_datetime(entry_brick_time)),
            "ml_score": float(ml_score) if ml_score is not None else None,
            "threshold": float(threshold) if threshold is not None else None,
            "risk": {
                "amount": float(trade_risk) if trade_risk is not None else None,
                "config": risk_config or None,
            },
            "spread": float(spread) if spread is not None else None,
            "pivot": pivot_data,
            "recovered": recovered,
        }

        with self.lock:
            if event_id in self.seen_event_ids:
                if trade_id not in self.trades:
                    state = TradeState(
                        trade_id=trade_id,
                        pair=pair,
                        strategy_id=strategy_id,
                        direction=direction,
                        entry_time=now,
                        entry_brick_index=brick_index,
                        last_seen_brick_index=brick_index,
                        last_seen_brick_time=_to_datetime(entry_brick_time),
                        bricks_for=0,
                        bricks_against=0,
                    )
                    self._track_trade(state)
                    self._write_index()
                return
            self._append_event(payload)
            self.seen_event_ids.add(event_id)
            state = TradeState(
                trade_id=trade_id,
                pair=pair,
                strategy_id=strategy_id,
                direction=direction,
                entry_time=now,
                entry_brick_index=brick_index,
                last_seen_brick_index=brick_index,
                last_seen_brick_time=_to_datetime(entry_brick_time),
                bricks_for=0,
                bricks_against=0,
            )
            self._track_trade(state)
            self._write_index()

    def record_close(
        self,
        trade_id: str,
        pair: str,
        strategy_id: Optional[str],
        direction: Optional[str],
        units: Optional[int],
        entry_price: Optional[float],
        reason: str,
        close_time: Optional[datetime] = None,
    ) -> None:
        event_id = f"{trade_id}:close"

        now = close_time or datetime.now(timezone.utc)
        payload = {
            "event_id": event_id,
            "event": "close",
            "timestamp": _format_dt(now),
            "trade_id": trade_id,
            "pair": pair,
            "strategy_id": strategy_id,
            "direction": direction,
            "units": int(units) if units is not None else None,
            "entry_price": float(entry_price) if entry_price is not None else None,
            "reason": reason,
        }

        with self.lock:
            if event_id in self.seen_event_ids:
                self._untrack_trade(trade_id)
                self._write_index()
                return
            self._append_event(payload)
            self.seen_event_ids.add(event_id)
            self._untrack_trade(trade_id)
            self._write_index()

    def handle_brick_events(
        self,
        pair: str,
        brick_events: List[dict],
        *,
        target_bricks: Optional[int] = None,
        stop_bricks: Optional[int] = None,
    ) -> List[dict]:
        if not brick_events:
            return []
        with self.lock:
            trade_ids = list(self.trades_by_pair.get(pair, set()))
            if not trade_ids:
                return []

            updated = False
            exit_signals: List[dict] = []
            for trade_id in trade_ids:
                trade_state = self.trades.get(trade_id)
                if not trade_state:
                    continue
                if trade_id in self.pending_closes:
                    continue
                for brick in brick_events:
                    if self._record_brick_locked(trade_state, brick):
                        updated = True
                        if target_bricks is not None and stop_bricks is not None:
                            net_bricks = trade_state.bricks_for - trade_state.bricks_against
                            if net_bricks >= target_bricks:
                                reason = f"target_{target_bricks}_bricks_for"
                            elif net_bricks <= -stop_bricks:
                                reason = f"stop_{stop_bricks}_bricks_against"
                            else:
                                continue
                            self.pending_closes.add(trade_id)
                            exit_signals.append({
                                "trade_id": trade_id,
                                "pair": trade_state.pair,
                                "strategy_id": trade_state.strategy_id,
                                "direction": trade_state.direction,
                                "bricks_for": trade_state.bricks_for,
                                "bricks_against": trade_state.bricks_against,
                                "net_bricks": net_bricks,
                                "reason": reason,
                                "exit_brick_index": brick.get("brick_index"),
                                "exit_brick_time": brick.get("brick_time"),
                            })
                            break

            if updated:
                self._write_index()
            return exit_signals

    def _record_brick_locked(self, trade_state: TradeState, brick: dict) -> bool:
        brick_index = brick.get("brick_index")
        brick_time = _to_datetime(brick.get("brick_time"))
        brick_open = brick.get("brick_open")
        brick_close = brick.get("brick_close")

        if brick_index is None:
            return False
        if trade_state.last_seen_brick_index is not None and brick_index <= trade_state.last_seen_brick_index:
            return False

        if brick_open is None or brick_close is None:
            return False

        if brick_close > brick_open:
            brick_direction = "UP"
        elif brick_close < brick_open:
            brick_direction = "DOWN"
        else:
            brick_direction = "FLAT"

        if brick_direction != "FLAT":
            if trade_state.direction == "BUY":
                if brick_direction == "UP":
                    trade_state.bricks_for += 1
                else:
                    trade_state.bricks_against += 1
            else:
                if brick_direction == "DOWN":
                    trade_state.bricks_for += 1
                else:
                    trade_state.bricks_against += 1

        trade_state.last_seen_brick_index = int(brick_index)
        trade_state.last_seen_brick_time = brick_time

        event_id = f"{trade_state.trade_id}:brick:{brick_index}:{_format_dt(brick_time)}"
        payload = {
            "event_id": event_id,
            "event": "brick",
            "timestamp": _format_dt(datetime.now(timezone.utc)),
            "trade_id": trade_state.trade_id,
            "pair": trade_state.pair,
            "brick_index": int(brick_index),
            "brick_time": _format_dt(brick_time),
            "brick_open": float(brick_open),
            "brick_close": float(brick_close),
            "brick_direction": brick_direction,
            "bricks_for": trade_state.bricks_for,
            "bricks_against": trade_state.bricks_against,
            "net_bricks": trade_state.bricks_for - trade_state.bricks_against,
        }
        self._append_event(payload)
        return True

    def reconcile_with_broker(self, api) -> None:
        open_trades = api.get_open_trades()
        if open_trades is None:
            logger.warning("TradeEventLogger: broker open trades unavailable, skipping reconcile")
            return

        open_by_id = {trade.id: trade for trade in open_trades}

        with self.lock:
            updated = False
            missing_ids = [trade_id for trade_id in self.trades if trade_id not in open_by_id]
            for trade_id in missing_ids:
                state = self.trades.get(trade_id)
                if not state:
                    continue
                close_id = f"{trade_id}:close"
                if close_id not in self.seen_event_ids:
                    self._append_event({
                        "event_id": close_id,
                        "event": "close",
                        "timestamp": _format_dt(datetime.now(timezone.utc)),
                        "trade_id": trade_id,
                        "pair": state.pair,
                        "strategy_id": state.strategy_id,
                        "direction": state.direction,
                        "units": None,
                        "entry_price": None,
                        "reason": "reconcile_missing",
                    })
                    self.seen_event_ids.add(close_id)
                    updated = True
                self._untrack_trade(trade_id)
                updated = True

            for trade_id, trade in open_by_id.items():
                if self.allowed_strategy_ids and trade.strategy_id not in self.allowed_strategy_ids:
                    continue
                if trade_id not in self.trades:
                    direction = "BUY" if trade.currentUnits > 0 else "SELL"
                    entry_time = _to_datetime(getattr(trade, "open_time", None)) or datetime.now(timezone.utc)
                    entry_brick_index = self._current_brick_index(trade.instrument)
                    entry_id = f"{trade_id}:entry"
                    if entry_id not in self.seen_event_ids:
                        self._append_event({
                            "event_id": entry_id,
                            "event": "entry",
                            "timestamp": _format_dt(entry_time),
                            "trade_id": trade_id,
                            "pair": trade.instrument,
                            "strategy_id": trade.strategy_id,
                            "direction": direction,
                            "entry_price": float(trade.price),
                            "tp_price": None,
                            "sl_price": None,
                            "units": int(abs(trade.currentUnits)),
                            "entry_brick_index": int(entry_brick_index) if entry_brick_index is not None else None,
                            "entry_brick_time": None,
                            "ml_score": None,
                            "threshold": None,
                            "risk": {"amount": None, "config": None},
                            "spread": None,
                            "pivot": None,
                            "recovered": True,
                        })
                        self.seen_event_ids.add(entry_id)
                        updated = True
                    state = TradeState(
                        trade_id=trade_id,
                        pair=trade.instrument,
                        strategy_id=trade.strategy_id,
                        direction=direction,
                        entry_time=entry_time,
                        entry_brick_index=entry_brick_index,
                        last_seen_brick_index=entry_brick_index,
                        last_seen_brick_time=None,
                        bricks_for=0,
                        bricks_against=0,
                    )
                    self._track_trade(state)
                    updated = True

            if self._backfill_missing_bricks_locked():
                updated = True
            if updated:
                self._write_index()

    def refresh_open_trades(self, api) -> None:
        open_trades = api.get_open_trades()
        if open_trades is None:
            return

        open_by_id = {trade.id: trade for trade in open_trades}
        with self.lock:
            updated = False
            tracked_ids = set(self.trades.keys())
            missing_ids = tracked_ids - set(open_by_id.keys())
            for trade_id in missing_ids:
                state = self.trades.get(trade_id)
                if not state:
                    continue
                close_id = f"{trade_id}:close"
                if close_id not in self.seen_event_ids:
                    self._append_event({
                        "event_id": close_id,
                        "event": "close",
                        "timestamp": _format_dt(datetime.now(timezone.utc)),
                        "trade_id": trade_id,
                        "pair": state.pair,
                        "strategy_id": state.strategy_id,
                        "direction": state.direction,
                        "units": None,
                        "entry_price": None,
                        "reason": "broker_closed",
                    })
                    self.seen_event_ids.add(close_id)
                    updated = True
                self._untrack_trade(trade_id)
                updated = True

            for trade_id, trade in open_by_id.items():
                if self.allowed_strategy_ids and trade.strategy_id not in self.allowed_strategy_ids:
                    continue
                if trade_id in self.trades:
                    continue
                direction = "BUY" if trade.currentUnits > 0 else "SELL"
                entry_time = _to_datetime(getattr(trade, "open_time", None)) or datetime.now(timezone.utc)
                entry_brick_index = self._current_brick_index(trade.instrument)
                entry_id = f"{trade_id}:entry"
                if entry_id not in self.seen_event_ids:
                    self._append_event({
                        "event_id": entry_id,
                        "event": "entry",
                        "timestamp": _format_dt(entry_time),
                        "trade_id": trade_id,
                        "pair": trade.instrument,
                        "strategy_id": trade.strategy_id,
                        "direction": direction,
                        "entry_price": float(trade.price),
                        "tp_price": None,
                        "sl_price": None,
                        "units": int(abs(trade.currentUnits)),
                        "entry_brick_index": int(entry_brick_index) if entry_brick_index is not None else None,
                        "entry_brick_time": None,
                        "ml_score": None,
                        "threshold": None,
                        "risk": {"amount": None, "config": None},
                        "spread": None,
                        "pivot": None,
                        "recovered": True,
                    })
                    self.seen_event_ids.add(entry_id)
                    updated = True
                state = TradeState(
                    trade_id=trade_id,
                    pair=trade.instrument,
                    strategy_id=trade.strategy_id,
                    direction=direction,
                    entry_time=entry_time,
                    entry_brick_index=entry_brick_index,
                    last_seen_brick_index=entry_brick_index,
                    last_seen_brick_time=None,
                    bricks_for=0,
                    bricks_against=0,
                )
                self._track_trade(state)
                updated = True

            if updated:
                self._write_index()

    def _backfill_missing_bricks_locked(self) -> bool:
        if not self.renko_engine:
            return False
        updated = False
        for trade_state in self.trades.values():
            state = self.renko_engine.get_state(trade_state.pair)
            if not state:
                continue
            df = state.feature_df_complete
            if df is None or df.empty:
                df = state.feature_df_full
            if df is None or df.empty:
                continue
            current_count = len(df)
            start_index = trade_state.last_seen_brick_index or trade_state.entry_brick_index or current_count
            if start_index >= current_count:
                continue

            for row_index in range(start_index, current_count):
                brick = df.iloc[row_index]
                brick_time = _to_datetime(brick.get("time"))
                if trade_state.entry_time and brick_time and brick_time < trade_state.entry_time:
                    continue

                brick_event = {
                    "brick_index": row_index + 1,
                    "brick_time": brick_time,
                    "brick_open": brick.get("Open"),
                    "brick_close": brick.get("Close"),
                }
                if self._record_brick_locked(trade_state, brick_event):
                    updated = True
        return updated
