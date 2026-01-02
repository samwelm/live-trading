"""
Renko trade ledger backed by SQLite.

Persists Renko bricks and open trades so brick exits are derived from the
full brick series instead of per-brick JSONL events.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set

import pandas as pd
from dateutil import parser as date_parser

from core.ledger_rows import EvUpdateRow

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


def _to_epoch(value) -> Optional[float]:
    dt = _to_datetime(value)
    if dt is None:
        return None
    return dt.timestamp()


def _direction_from_prices(open_price: Optional[float], close_price: Optional[float]) -> int:
    if open_price is None or close_price is None:
        return 0
    if close_price > open_price:
        return 1
    if close_price < open_price:
        return -1
    return 0


class RenkoTradeLedger:
    def __init__(
        self,
        db_path: str = "./state/renko_bricks.db",
        allowed_strategy_ids: Optional[Set[str]] = None,
    ):
        self.db_path = Path(db_path)
        self.allowed_strategy_ids = set(allowed_strategy_ids or [])
        self.lock = threading.Lock()
        self.pending_closes: Set[str] = set()
        self.renko_engine = None
        self._last_brick_index_cache = {}
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.OperationalError:
            pass
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS renko_bricks (
                    pair TEXT NOT NULL,
                    brick_index INTEGER NOT NULL,
                    brick_time TEXT,
                    brick_time_ts REAL,
                    brick_open REAL,
                    brick_close REAL,
                    direction INTEGER,
                    ev_long REAL,
                    ev_short REAL,
                    ev_signal INTEGER,
                    PRIMARY KEY (pair, brick_index)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_renko_bricks_pair_idx ON renko_bricks(pair, brick_index)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_renko_bricks_pair_dir_idx ON renko_bricks(pair, direction, brick_index)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_renko_bricks_pair_time ON renko_bricks(pair, brick_time_ts)"
            )
            self._ensure_brick_columns(conn)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS renko_open_trades (
                    trade_id TEXT PRIMARY KEY,
                    pair TEXT NOT NULL,
                    strategy_id TEXT,
                    direction TEXT NOT NULL,
                    entry_time TEXT,
                    entry_time_ts REAL,
                    entry_brick_index INTEGER,
                    target_bricks INTEGER,
                    stop_bricks INTEGER,
                    tp_price REAL,
                    sl_price REAL,
                    chain_start_brick_index INTEGER,
                    last_reset_brick_index INTEGER,
                    last_seen_brick_index INTEGER,
                    last_seen_brick_time TEXT,
                    current_step_count INTEGER,
                    chain_step_count INTEGER,
                    current_cum_reward INTEGER,
                    chain_cum_reward INTEGER,
                    reset_count INTEGER,
                    consecutive_negative_resets INTEGER DEFAULT 0,
                    last_segment_outcome TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_renko_open_trades_pair ON renko_open_trades(pair)"
            )
            self._ensure_open_trade_columns(conn)

    def _ensure_brick_columns(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(renko_bricks)").fetchall()
        existing = {row["name"] for row in rows}
        for name, col_type in (
            ("ev_long", "REAL"),
            ("ev_short", "REAL"),
            ("ev_signal", "INTEGER"),
        ):
            if name not in existing:
                conn.execute(f"ALTER TABLE renko_bricks ADD COLUMN {name} {col_type}")

    def _ensure_open_trade_columns(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(renko_open_trades)").fetchall()
        existing = {row["name"] for row in rows}
        for name, col_type in (
            ("tp_price", "REAL"),
            ("sl_price", "REAL"),
            ("chain_start_brick_index", "INTEGER"),
            ("last_reset_brick_index", "INTEGER"),
            ("last_seen_brick_index", "INTEGER"),
            ("last_seen_brick_time", "TEXT"),
            ("current_step_count", "INTEGER"),
            ("chain_step_count", "INTEGER"),
            ("current_cum_reward", "INTEGER"),
            ("chain_cum_reward", "INTEGER"),
            ("reset_count", "INTEGER"),
            ("consecutive_negative_resets", "INTEGER DEFAULT 0"),
            ("last_segment_outcome", "TEXT"),
        ):
            if name not in existing:
                conn.execute(f"ALTER TABLE renko_open_trades ADD COLUMN {name} {col_type}")

    def attach_engine(self, renko_engine) -> None:
        self.renko_engine = renko_engine

    def sync_all_pairs_from_engine(self) -> None:
        if not self.renko_engine:
            return
        for pair in list(getattr(self.renko_engine, "states", {}).keys()):
            self.sync_from_engine(pair)

    def sync_from_engine(self, pair: str) -> None:
        if not self.renko_engine:
            return
        state = self.renko_engine.get_state(pair)
        if not state:
            return
        df = state.feature_df_full
        if df is None or df.empty:
            df = state.renko_df
        if df is None or df.empty:
            return

        df = df.copy()
        if "Open" not in df.columns and "mid_o" in df.columns:
            df["Open"] = df["mid_o"]
        if "Close" not in df.columns and "mid_c" in df.columns:
            df["Close"] = df["mid_c"]
        if "time" not in df.columns and "Time" in df.columns:
            df["time"] = df["Time"]
        if "time" not in df.columns or "Open" not in df.columns or "Close" not in df.columns:
            logger.warning(f"{pair}: Missing columns for brick sync; skipping")
            return

        df = df.reset_index(drop=True)
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")

        last_idx = self.get_last_brick_index(pair) or 0
        if last_idx > len(df):
            logger.warning(
                f"{pair}: Brick ledger ahead of engine ({last_idx} > {len(df)}). "
                "Consider clearing the ledger if this persists."
            )
            return
        if last_idx == len(df):
            return

        new_df = df.iloc[last_idx:]
        rows = []
        for row_idx, row in new_df.iterrows():
            brick_index = int(row_idx) + 1
            brick_time = _to_datetime(row.get("time"))
            brick_open = row.get("Open")
            brick_close = row.get("Close")
            ev_long = row.get("ev_long")
            if ev_long is None:
                ev_long = row.get("pred_long")
            ev_short = row.get("ev_short")
            if ev_short is None:
                ev_short = row.get("pred_short")
            ev_signal = row.get("ev_signal")
            rows.append(
                (
                    pair,
                    brick_index,
                    _format_dt(brick_time),
                    _to_epoch(brick_time),
                    float(brick_open) if brick_open is not None else None,
                    float(brick_close) if brick_close is not None else None,
                    _direction_from_prices(brick_open, brick_close),
                    float(ev_long) if ev_long is not None and pd.notna(ev_long) else None,
                    float(ev_short) if ev_short is not None and pd.notna(ev_short) else None,
                    int(ev_signal) if ev_signal is not None and pd.notna(ev_signal) else None,
                )
            )
        self._insert_brick_rows(rows)
        if rows:
            self._last_brick_index_cache[pair] = max(row[1] for row in rows)

    def _insert_brick_rows(self, rows: List[tuple]) -> None:
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO renko_bricks
                (pair, brick_index, brick_time, brick_time_ts, brick_open, brick_close, direction,
                 ev_long, ev_short, ev_signal)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pair, brick_index) DO UPDATE SET
                    brick_time = excluded.brick_time,
                    brick_time_ts = excluded.brick_time_ts,
                    brick_open = excluded.brick_open,
                    brick_close = excluded.brick_close,
                    direction = excluded.direction,
                    ev_long = COALESCE(excluded.ev_long, renko_bricks.ev_long),
                    ev_short = COALESCE(excluded.ev_short, renko_bricks.ev_short),
                    ev_signal = COALESCE(excluded.ev_signal, renko_bricks.ev_signal)
                """,
                rows,
            )

    def update_ev_rows(self, rows: List[tuple] | List[EvUpdateRow]) -> None:
        if not rows:
            return
        if rows and isinstance(rows[0], EvUpdateRow):
            rows = [row.to_tuple() for row in rows]
        with self.lock:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO renko_bricks (pair, brick_index, ev_long, ev_short, ev_signal)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(pair, brick_index) DO UPDATE SET
                        ev_long = excluded.ev_long,
                        ev_short = excluded.ev_short,
                        ev_signal = excluded.ev_signal
                    """,
                    rows,
                )

    def get_brick_ev_info(self, pair: str, brick_index: int) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT brick_time, brick_open, brick_close, ev_signal, ev_long, ev_short
                FROM renko_bricks
                WHERE pair = ? AND brick_index = ?
                """,
                (pair, int(brick_index)),
            ).fetchone()
        if not row:
            return None
        return {
            "brick_time": row["brick_time"],
            "brick_open": float(row["brick_open"]) if row["brick_open"] is not None else None,
            "brick_close": float(row["brick_close"]) if row["brick_close"] is not None else None,
            "ev_signal": int(row["ev_signal"]) if row["ev_signal"] is not None else None,
            "ev_long": float(row["ev_long"]) if row["ev_long"] is not None else None,
            "ev_short": float(row["ev_short"]) if row["ev_short"] is not None else None,
        }

    def get_last_brick_index(self, pair: str) -> Optional[int]:
        if pair in self._last_brick_index_cache:
            return self._last_brick_index_cache[pair]
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(brick_index) AS max_idx FROM renko_bricks WHERE pair = ?",
                (pair,),
            ).fetchone()
        max_idx = int(row["max_idx"]) if row and row["max_idx"] is not None else None
        self._last_brick_index_cache[pair] = max_idx
        return max_idx

    def _current_brick_index(self, pair: str) -> Optional[int]:
        last_idx = self.get_last_brick_index(pair)
        if last_idx:
            return last_idx
        if not self.renko_engine:
            return None
        state = self.renko_engine.get_state(pair)
        if not state:
            return None
        df = state.feature_df_full
        if df is None or df.empty:
            df = state.renko_df
        if df is not None and not df.empty:
            return len(df)
        return None

    def _get_brick_size(self, pair: str) -> Optional[float]:
        if not self.renko_engine:
            return None
        state = self.renko_engine.get_state(pair)
        if not state or state.brick_size is None:
            return None
        return float(state.brick_size)

    def _compute_tp_sl(
        self,
        entry_price: Optional[float],
        direction: str,
        brick_size: Optional[float],
        target_bricks: Optional[int],
        stop_bricks: Optional[int],
    ) -> tuple[Optional[float], Optional[float]]:
        # Informational only; exits are driven by brick counts, not these prices.
        if entry_price is None or brick_size is None:
            return None, None
        if target_bricks is None or stop_bricks is None:
            return None, None
        if direction.upper() == "BUY":
            tp_price = entry_price + (target_bricks * brick_size)
            sl_price = entry_price - (stop_bricks * brick_size)
        else:
            tp_price = entry_price - (target_bricks * brick_size)
            sl_price = entry_price + (stop_bricks * brick_size)
        return tp_price, sl_price

    def _lookup_brick_index_for_time(self, pair: str, entry_time: Optional[datetime]) -> Optional[int]:
        if entry_time is None:
            return None
        entry_ts = _to_epoch(entry_time)
        if entry_ts is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT brick_index FROM renko_bricks
                WHERE pair = ? AND brick_time_ts <= ?
                ORDER BY brick_time_ts DESC
                LIMIT 1
                """,
                (pair, entry_ts),
            ).fetchone()
            if row and row["brick_index"] is not None:
                return int(row["brick_index"])
            row = conn.execute(
                """
                SELECT brick_index FROM renko_bricks
                WHERE pair = ? AND brick_time_ts >= ?
                ORDER BY brick_time_ts ASC
                LIMIT 1
                """,
                (pair, entry_ts),
            ).fetchone()
        if row and row["brick_index"] is not None:
            return int(row["brick_index"])
        return None

    def _fetch_open_trades(self, pair: Optional[str] = None) -> List[dict]:
        query = (
            "SELECT trade_id, pair, strategy_id, direction, entry_time, entry_time_ts, entry_brick_index, "
            "target_bricks, stop_bricks, tp_price, sl_price, "
            "chain_start_brick_index, last_reset_brick_index, last_seen_brick_index, last_seen_brick_time, "
            "current_step_count, chain_step_count, current_cum_reward, chain_cum_reward, reset_count, "
            "consecutive_negative_resets, last_segment_outcome "
            "FROM renko_open_trades"
        )
        params = ()
        if pair:
            query += " WHERE pair = ?"
            params = (pair,)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def _upsert_open_trade(
        self,
        trade_id: str,
        pair: str,
        strategy_id: Optional[str],
        direction: str,
        entry_time: Optional[datetime],
        entry_brick_index: Optional[int],
        target_bricks: Optional[int] = None,
        stop_bricks: Optional[int] = None,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
    ) -> None:
        entry_dt = _to_datetime(entry_time) if entry_time is not None else None
        entry_time_str = _format_dt(entry_dt)
        entry_time_ts = _to_epoch(entry_dt)
        chain_start_index = int(entry_brick_index) if entry_brick_index is not None else None
        last_reset_index = int(entry_brick_index) if entry_brick_index is not None else None
        last_seen_index = int(entry_brick_index) if entry_brick_index is not None else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO renko_open_trades
                (trade_id, pair, strategy_id, direction, entry_time, entry_time_ts, entry_brick_index,
                 target_bricks, stop_bricks, tp_price, sl_price,
                 chain_start_brick_index, last_reset_brick_index, last_seen_brick_index, last_seen_brick_time,
                 current_step_count, chain_step_count, current_cum_reward, chain_cum_reward, reset_count,
                 consecutive_negative_resets, last_segment_outcome)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    pair = excluded.pair,
                    strategy_id = excluded.strategy_id,
                    direction = excluded.direction,
                    entry_time = excluded.entry_time,
                    entry_time_ts = excluded.entry_time_ts,
                    entry_brick_index = excluded.entry_brick_index,
                    target_bricks = COALESCE(excluded.target_bricks, renko_open_trades.target_bricks),
                    stop_bricks = COALESCE(excluded.stop_bricks, renko_open_trades.stop_bricks),
                    tp_price = COALESCE(excluded.tp_price, renko_open_trades.tp_price),
                    sl_price = COALESCE(excluded.sl_price, renko_open_trades.sl_price),
                    chain_start_brick_index = COALESCE(renko_open_trades.chain_start_brick_index, excluded.chain_start_brick_index),
                    last_reset_brick_index = COALESCE(renko_open_trades.last_reset_brick_index, excluded.last_reset_brick_index),
                    last_seen_brick_index = COALESCE(renko_open_trades.last_seen_brick_index, excluded.last_seen_brick_index),
                    last_seen_brick_time = COALESCE(renko_open_trades.last_seen_brick_time, excluded.last_seen_brick_time),
                    current_step_count = COALESCE(renko_open_trades.current_step_count, excluded.current_step_count),
                    chain_step_count = COALESCE(renko_open_trades.chain_step_count, excluded.chain_step_count),
                    current_cum_reward = COALESCE(renko_open_trades.current_cum_reward, excluded.current_cum_reward),
                    chain_cum_reward = COALESCE(renko_open_trades.chain_cum_reward, excluded.chain_cum_reward),
                    reset_count = COALESCE(renko_open_trades.reset_count, excluded.reset_count),
                    consecutive_negative_resets = COALESCE(
                        renko_open_trades.consecutive_negative_resets,
                        excluded.consecutive_negative_resets
                    ),
                    last_segment_outcome = COALESCE(
                        renko_open_trades.last_segment_outcome,
                        excluded.last_segment_outcome
                    )
                """,
                (
                    trade_id,
                    pair,
                    strategy_id,
                    direction,
                    entry_time_str,
                    entry_time_ts,
                    int(entry_brick_index) if entry_brick_index is not None else None,
                    int(target_bricks) if target_bricks is not None else None,
                    int(stop_bricks) if stop_bricks is not None else None,
                    float(tp_price) if tp_price is not None else None,
                    float(sl_price) if sl_price is not None else None,
                    chain_start_index,
                    last_reset_index,
                    last_seen_index,
                    entry_time_str,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    "NEUTRAL",
                ),
            )

    def _delete_open_trade(self, trade_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM renko_open_trades WHERE trade_id = ?", (trade_id,))

    def _update_trade_metrics(
        self,
        trade_id: str,
        last_seen_brick_index: int,
        last_seen_brick_time: Optional[datetime],
        current_step_count: int,
        chain_step_count: int,
        current_cum_reward: int,
        chain_cum_reward: int,
        *,
        chain_start_brick_index: Optional[int] = None,
        last_reset_brick_index: Optional[int] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE renko_open_trades
                SET last_seen_brick_index = ?,
                    last_seen_brick_time = COALESCE(?, last_seen_brick_time),
                    current_step_count = ?,
                    chain_step_count = ?,
                    current_cum_reward = ?,
                    chain_cum_reward = ?,
                    chain_start_brick_index = COALESCE(?, chain_start_brick_index),
                    last_reset_brick_index = COALESCE(?, last_reset_brick_index)
                WHERE trade_id = ?
                """,
                (
                    int(last_seen_brick_index),
                    _format_dt(_to_datetime(last_seen_brick_time)) if last_seen_brick_time is not None else None,
                    int(current_step_count),
                    int(chain_step_count),
                    int(current_cum_reward),
                    int(chain_cum_reward),
                    int(chain_start_brick_index) if chain_start_brick_index is not None else None,
                    int(last_reset_brick_index) if last_reset_brick_index is not None else None,
                    trade_id,
                ),
            )

    def _update_entry_brick_index(
        self,
        trade_id: str,
        entry_brick_index: int,
        entry_time: Optional[datetime] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE renko_open_trades
                SET entry_brick_index = COALESCE(entry_brick_index, ?),
                    chain_start_brick_index = COALESCE(chain_start_brick_index, ?),
                    last_reset_brick_index = COALESCE(last_reset_brick_index, ?),
                    last_seen_brick_index = COALESCE(last_seen_brick_index, ?),
                    last_seen_brick_time = COALESCE(last_seen_brick_time, ?)
                WHERE trade_id = ?
                """,
                (
                    int(entry_brick_index),
                    int(entry_brick_index),
                    int(entry_brick_index),
                    int(entry_brick_index),
                    _format_dt(_to_datetime(entry_time)) if entry_time is not None else None,
                    trade_id,
                ),
            )

    def _backfill_open_trade_targets(
        self,
        target_bricks: Optional[int],
        stop_bricks: Optional[int],
    ) -> None:
        if target_bricks is None and stop_bricks is None:
            return
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE renko_open_trades
                SET target_bricks = CASE
                        WHEN target_bricks IS NULL OR target_bricks != target_bricks THEN ?
                        ELSE target_bricks
                    END,
                    stop_bricks = CASE
                        WHEN stop_bricks IS NULL OR stop_bricks != stop_bricks THEN ?
                        ELSE stop_bricks
                    END
                WHERE target_bricks IS NULL OR target_bricks != target_bricks
                   OR stop_bricks IS NULL OR stop_bricks != stop_bricks
                """,
                (
                    int(target_bricks) if target_bricks is not None else None,
                    int(stop_bricks) if stop_bricks is not None else None,
                ),
            )

    def _backfill_open_trade_prices(
        self,
        open_by_id: dict,
        target_bricks: Optional[int],
        stop_bricks: Optional[int],
    ) -> None:
        if target_bricks is None or stop_bricks is None:
            return
        if not open_by_id:
            return
        with self._connect() as conn:
            for trade_id, trade in open_by_id.items():
                entry_price = getattr(trade, "price", None)
                pair = getattr(trade, "instrument", None)
                if entry_price is None or not pair:
                    continue
                brick_size = self._get_brick_size(pair)
                if brick_size is None:
                    continue
                direction = "BUY" if getattr(trade, "currentUnits", 0) > 0 else "SELL"
                tp_price, sl_price = self._compute_tp_sl(
                    float(entry_price),
                    direction,
                    brick_size,
                    target_bricks,
                    stop_bricks,
                )
                if tp_price is None and sl_price is None:
                    continue
                conn.execute(
                    """
                    UPDATE renko_open_trades
                    SET tp_price = CASE
                            WHEN tp_price IS NULL OR tp_price != tp_price THEN ?
                            ELSE tp_price
                        END,
                        sl_price = CASE
                            WHEN sl_price IS NULL OR sl_price != sl_price THEN ?
                            ELSE sl_price
                        END
                    WHERE trade_id = ?
                    """,
                    (
                        float(tp_price) if tp_price is not None else None,
                        float(sl_price) if sl_price is not None else None,
                        trade_id,
                    ),
                )

    def clear_pending_close(self, trade_id: str) -> None:
        with self.lock:
            self.pending_closes.discard(trade_id)

    def rollover_trade(
        self,
        trade_id: str,
        new_entry_brick_index: int,
        entry_time: Optional[datetime] = None,
        segment_outcome: str = "NEUTRAL",
    ) -> bool:
        entry_dt = _to_datetime(entry_time) if entry_time is not None else None
        outcome = str(segment_outcome or "NEUTRAL").upper()
        if outcome not in ("WIN", "LOSS", "NEUTRAL"):
            outcome = "NEUTRAL"
        with self.lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT consecutive_negative_resets
                    FROM renko_open_trades
                    WHERE trade_id = ?
                    """,
                    (trade_id,),
                ).fetchone()
                if not row:
                    return False
                current_consecutive = (
                    int(row["consecutive_negative_resets"])
                    if row["consecutive_negative_resets"] is not None
                    else 0
                )
                if outcome == "LOSS":
                    new_consecutive = current_consecutive + 1
                elif outcome == "WIN":
                    new_consecutive = 0
                else:
                    new_consecutive = current_consecutive
                cur = conn.execute(
                    """
                    UPDATE renko_open_trades
                    SET last_reset_brick_index = ?,
                        last_seen_brick_index = ?,
                        last_seen_brick_time = COALESCE(?, last_seen_brick_time),
                        current_step_count = 0,
                        current_cum_reward = 0,
                        reset_count = COALESCE(reset_count, 0) + 1,
                        consecutive_negative_resets = ?,
                        last_segment_outcome = ?
                    WHERE trade_id = ?
                    """,
                    (
                        int(new_entry_brick_index),
                        int(new_entry_brick_index),
                        _format_dt(entry_dt) if entry_dt is not None else None,
                        int(new_consecutive),
                        outcome,
                        trade_id,
                    ),
                )
            self.pending_closes.discard(trade_id)
        return bool(cur.rowcount and cur.rowcount > 0)

    def get_consecutive_negative_count(self, trade_id: str) -> int:
        with self.lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT consecutive_negative_resets
                    FROM renko_open_trades
                    WHERE trade_id = ?
                    """,
                    (trade_id,),
                ).fetchone()
        if row and row["consecutive_negative_resets"] is not None:
            return int(row["consecutive_negative_resets"])
        return 0

    def projected_consecutive_negative_count(self, trade_id: str, segment_outcome: str) -> int:
        outcome = str(segment_outcome or "NEUTRAL").upper()
        if outcome not in ("WIN", "LOSS", "NEUTRAL"):
            outcome = "NEUTRAL"
        current = self.get_consecutive_negative_count(trade_id)
        if outcome == "LOSS":
            return current + 1
        if outcome == "WIN":
            return 0
        return current

    def should_cap_rollover(
        self,
        trade_id: str,
        segment_outcome: str,
        cap_limit: int = 3,
    ) -> tuple[bool, int]:
        outcome = str(segment_outcome or "NEUTRAL").upper()
        if outcome not in ("WIN", "LOSS", "NEUTRAL"):
            outcome = "NEUTRAL"
        projected = self.projected_consecutive_negative_count(trade_id, outcome)
        if cap_limit > 0 and outcome == "LOSS":
            if projected >= cap_limit:
                return True, projected
        return False, projected

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
        target_bricks: Optional[int] = None,
        stop_bricks: Optional[int] = None,
        recovered: bool = False,
        entry_time: Optional[datetime] = None,
    ) -> None:
        entry_dt = _to_datetime(entry_time) or _to_datetime(entry_brick_time) or datetime.now(timezone.utc)
        brick_index = entry_brick_index
        if brick_index is None:
            brick_index = self._lookup_brick_index_for_time(pair, entry_dt)
        if brick_index is None:
            brick_index = self._current_brick_index(pair)

        with self.lock:
            self._upsert_open_trade(
                trade_id=trade_id,
                pair=pair,
                strategy_id=strategy_id,
                direction=str(direction).upper(),
                entry_time=entry_dt,
                entry_brick_index=brick_index,
                target_bricks=target_bricks,
                stop_bricks=stop_bricks,
                tp_price=tp_price,
                sl_price=sl_price,
            )

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
        with self.lock:
            self._delete_open_trade(trade_id)
            self.pending_closes.discard(trade_id)

    def _count_bricks_range(self, pair: str, start_index: int, end_index: int) -> tuple[int, int, int]:
        if end_index <= start_index:
            return 0, 0, 0
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN direction = 1 THEN 1 ELSE 0 END) AS up_count,
                    SUM(CASE WHEN direction = -1 THEN 1 ELSE 0 END) AS down_count,
                    COUNT(*) AS total_count
                FROM renko_bricks
                WHERE pair = ? AND brick_index > ? AND brick_index <= ?
                """,
                (pair, int(start_index), int(end_index)),
            ).fetchone()
        up_count = int(row["up_count"]) if row and row["up_count"] is not None else 0
        down_count = int(row["down_count"]) if row and row["down_count"] is not None else 0
        total_count = int(row["total_count"]) if row and row["total_count"] is not None else 0
        return up_count, down_count, total_count

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
            rows = []
            max_event_index = None
            max_event_time = None
            for brick in brick_events:
                brick_index = brick.get("brick_index")
                brick_time = _to_datetime(brick.get("brick_time"))
                brick_open = brick.get("brick_open")
                brick_close = brick.get("brick_close")
                ev_long = brick.get("ev_long")
                ev_short = brick.get("ev_short")
                ev_signal = brick.get("ev_signal")
                if brick_index is None:
                    continue
                rows.append(
                    (
                        pair,
                        int(brick_index),
                        _format_dt(brick_time),
                        _to_epoch(brick_time),
                        float(brick_open) if brick_open is not None else None,
                        float(brick_close) if brick_close is not None else None,
                        _direction_from_prices(brick_open, brick_close),
                        float(ev_long) if ev_long is not None and pd.notna(ev_long) else None,
                        float(ev_short) if ev_short is not None and pd.notna(ev_short) else None,
                        int(ev_signal) if ev_signal is not None and pd.notna(ev_signal) else None,
                    )
                )
                if max_event_index is None or int(brick_index) > max_event_index:
                    max_event_index = int(brick_index)
                    max_event_time = brick_time

            self._insert_brick_rows(rows)
            if rows:
                self._last_brick_index_cache[pair] = max(row[1] for row in rows)

            trades = self._fetch_open_trades(pair)
            if not trades:
                return []

            last_brick_index = self.get_last_brick_index(pair) or max_event_index
            if last_brick_index is None:
                return []

            exit_signals: List[dict] = []
            for trade in trades:
                trade_id = trade.get("trade_id")
                if not trade_id:
                    continue
                entry_index = trade.get("entry_brick_index")
                entry_dt = _to_datetime(trade.get("entry_time"))
                if entry_index is None:
                    entry_index = self._lookup_brick_index_for_time(pair, entry_dt)
                    if entry_index is None:
                        entry_index = last_brick_index
                    if entry_index is not None:
                        self._update_entry_brick_index(trade_id, entry_index, entry_dt)
                    else:
                        continue

                chain_start_index = trade.get("chain_start_brick_index") or entry_index
                last_reset_index = trade.get("last_reset_brick_index") or entry_index
                if chain_start_index is None or last_reset_index is None:
                    continue

                curr_up, curr_down, curr_total = self._count_bricks_range(
                    pair, int(last_reset_index), int(last_brick_index)
                )
                chain_up, chain_down, chain_total = self._count_bricks_range(
                    pair, int(chain_start_index), int(last_brick_index)
                )

                direction = str(trade.get("direction") or "BUY").upper()
                if direction == "BUY":
                    bricks_for = curr_up
                    bricks_against = curr_down
                    current_cum = curr_up - curr_down
                    chain_cum = chain_up - chain_down
                else:
                    bricks_for = curr_down
                    bricks_against = curr_up
                    current_cum = curr_down - curr_up
                    chain_cum = chain_down - chain_up

                self._update_trade_metrics(
                    trade_id=trade_id,
                    last_seen_brick_index=int(last_brick_index),
                    last_seen_brick_time=max_event_time,
                    current_step_count=curr_total,
                    chain_step_count=chain_total,
                    current_cum_reward=current_cum,
                    chain_cum_reward=chain_cum,
                    chain_start_brick_index=chain_start_index
                    if trade.get("chain_start_brick_index") is None
                    else None,
                    last_reset_brick_index=last_reset_index
                    if trade.get("last_reset_brick_index") is None
                    else None,
                )

                if trade_id in self.pending_closes:
                    continue

                if target_bricks is None or stop_bricks is None:
                    continue
                if current_cum >= target_bricks:
                    reason = f"target_{target_bricks}_bricks_for"
                elif current_cum <= -stop_bricks:
                    reason = f"stop_{stop_bricks}_bricks_against"
                else:
                    continue

                self.pending_closes.add(trade_id)
                exit_signals.append({
                    "trade_id": trade_id,
                    "pair": trade.get("pair"),
                    "strategy_id": trade.get("strategy_id"),
                    "direction": direction,
                    "bricks_for": bricks_for,
                    "bricks_against": bricks_against,
                    "net_bricks": current_cum,
                    "reason": reason,
                    "exit_brick_index": last_brick_index,
                    "exit_brick_time": _format_dt(max_event_time),
                })

            return exit_signals

    def reconcile_with_broker(
        self,
        api,
        *,
        target_bricks: Optional[int] = None,
        stop_bricks: Optional[int] = None,
    ) -> None:
        if self.renko_engine:
            self.sync_all_pairs_from_engine()

        open_trades = api.get_open_trades()
        if open_trades is None:
            logger.warning("RenkoTradeLedger: broker open trades unavailable, skipping reconcile")
            return

        open_by_id = {trade.id: trade for trade in open_trades}

        with self.lock:
            tracked = {row["trade_id"] for row in self._fetch_open_trades()}
            missing_ids = tracked - set(open_by_id.keys())
            for trade_id in missing_ids:
                self._delete_open_trade(trade_id)
                self.pending_closes.discard(trade_id)

            for trade_id, trade in open_by_id.items():
                strategy_id = getattr(trade, "strategy_id", None)
                if self.allowed_strategy_ids and strategy_id not in self.allowed_strategy_ids:
                    continue
                if trade_id in tracked:
                    continue
                direction = "BUY" if trade.currentUnits > 0 else "SELL"
                entry_time = _to_datetime(getattr(trade, "open_time", None)) or datetime.now(timezone.utc)
                entry_brick_index = self._lookup_brick_index_for_time(trade.instrument, entry_time)
                if entry_brick_index is None:
                    entry_brick_index = self._current_brick_index(trade.instrument)
                self._upsert_open_trade(
                    trade_id=trade_id,
                    pair=trade.instrument,
                    strategy_id=strategy_id,
                    direction=direction,
                    entry_time=entry_time,
                    entry_brick_index=entry_brick_index,
                )
            self._backfill_open_trade_targets(target_bricks, stop_bricks)
            self._backfill_open_trade_prices(open_by_id, target_bricks, stop_bricks)

    def refresh_open_trades(
        self,
        api,
        *,
        target_bricks: Optional[int] = None,
        stop_bricks: Optional[int] = None,
    ) -> None:
        open_trades = api.get_open_trades()
        if open_trades is None:
            return

        open_by_id = {trade.id: trade for trade in open_trades}
        with self.lock:
            tracked_rows = self._fetch_open_trades()
            tracked_ids = {row["trade_id"] for row in tracked_rows}
            missing_ids = tracked_ids - set(open_by_id.keys())
            for trade_id in missing_ids:
                self._delete_open_trade(trade_id)
                self.pending_closes.discard(trade_id)

            for trade_id, trade in open_by_id.items():
                strategy_id = getattr(trade, "strategy_id", None)
                if self.allowed_strategy_ids and strategy_id not in self.allowed_strategy_ids:
                    continue
                if trade_id in tracked_ids:
                    continue
                direction = "BUY" if trade.currentUnits > 0 else "SELL"
                entry_time = _to_datetime(getattr(trade, "open_time", None)) or datetime.now(timezone.utc)
                entry_brick_index = self._lookup_brick_index_for_time(trade.instrument, entry_time)
                if entry_brick_index is None:
                    entry_brick_index = self._current_brick_index(trade.instrument)
                self._upsert_open_trade(
                    trade_id=trade_id,
                    pair=trade.instrument,
                    strategy_id=strategy_id,
                    direction=direction,
                    entry_time=entry_time,
                    entry_brick_index=entry_brick_index,
                )
            self._backfill_open_trade_targets(target_bricks, stop_bricks)
            self._backfill_open_trade_prices(open_by_id, target_bricks, stop_bricks)
