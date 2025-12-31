"""
Timeframe-agnostic Renko ML Engine using CandleManager for candle completion detection.

Architecture:
- Uses existing CandleManager to detect new candles (configurable granularity)
- Fetches candle data from API on each new candle
- Rebuilds Renko from candle data (EXACT same pipeline as training)
- Detects new pivots and scores them with ML model

Supports any granularity: S5, M1, M5, M15, H1, etc.
"""
import threading
import time
import logging
from dataclasses import dataclass
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List
from queue import Queue

from api.oanda_api import OandaApi
from .renko_builder import build_renko
from .renko_ml_features import build_enhanced_features
from .pairs_config import get_pair_config
from infrastructure.collect_data import INCREMENTS, granularity_to_timedelta
from core.signals import PivotSignal

logger = logging.getLogger(__name__)


def _parse_start_date(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


@dataclass(repr=False, slots=True)
class RenkoState:
    """State tracking for a single pair's Renko processing."""
    pair: str
    brick_size: float
    start_date: str
    spread_pips: float
    granularity: str = "S5"

    candle_data: Optional[pd.DataFrame] = None
    last_candle_close_time: Optional[datetime] = None
    initialized: bool = False
    candle_updates_since_save: int = 0

    renko_df: Optional[pd.DataFrame] = None
    last_brick_close: Optional[float] = None
    current_reference_price: Optional[float] = None
    last_rebuild_price: Optional[float] = None
    last_rebuild_time: Optional[datetime] = None
    feature_df_full: Optional[pd.DataFrame] = None
    feature_df_complete: Optional[pd.DataFrame] = None

    last_pivot_idx: int = -1
    last_brick_count: int = 0
    last_update: Optional[datetime] = None

    def update_brick_tracking(self, feature_df: pd.DataFrame):
        """Update brick-in-progress tracking from latest feature-complete dataset."""
        if feature_df is not None and not feature_df.empty:
            # Get last complete brick's close price
            self.last_brick_close = feature_df.iloc[-1]['Close']
            self.current_reference_price = self.last_brick_close
            logger.debug(f"{self.pair}: Brick tracking updated - reference: {self.current_reference_price:.5f}")

    def check_new_brick_from_price(self, current_price: float) -> bool:
        """
        Check if current price would trigger a new brick.

        Args:
            current_price: Current mid price

        Returns:
            True if new brick(s) should form
        """
        if self.current_reference_price is None:
            return False

        distance = abs(current_price - self.current_reference_price)
        num_bricks = int(distance / self.brick_size)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"{self.pair}: tick check price={current_price:.5f} ref={self.current_reference_price:.5f} "
                f"dist={distance:.5f} brick={self.brick_size:.5f} -> bricks={num_bricks}"
            )

        return num_bricks > 0

    def get_new_pivots(self, feature_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract pivots that are new since last update.

        Args:
            feature_df: Latest feature-complete DataFrame (full history)

        Returns:
            DataFrame of new pivots only
        """
        # Find pivots in new tail
        if "pivot_high" in feature_df.columns and "pivot_low" in feature_df.columns:
            pivot_mask = feature_df["pivot_high"].astype(bool) | feature_df["pivot_low"].astype(bool)
        else:
            pivot_mask = (feature_df['raw_pivot_high'] == 1) | (feature_df['raw_pivot_low'] == 1)
        pivot_indices = feature_df.index[pivot_mask].tolist()

        # Filter to only new pivots (after last processed index)
        new_pivot_indices = [idx for idx in pivot_indices if idx > self.last_pivot_idx]

        if not new_pivot_indices:
            return pd.DataFrame()

        # Update last processed index
        self.last_pivot_idx = max(new_pivot_indices)

        return feature_df.loc[new_pivot_indices].copy()


class RenkoEngine(threading.Thread):
    """
    Timeframe-agnostic Renko ML Engine.

    Uses CandleManager to detect new candle completion, then:
    1. Fetches candle data (configurable lookback for context)
    2. Builds Renko from candles (exact training pipeline)
    3. Detects pivots
    4. Engineers features
    5. Extracts new pivots since last update
    6. Pushes to scoring queue
    """

    def __init__(
        self,
        candle_manager,
        api: OandaApi,
        pairs_config: Dict[str, dict],
        pivot_queue: Queue[PivotSignal],
        granularity: Optional[str] = None,
        poll_interval: int = 1,
        lookback: Optional[int] = None,
        brick_event_callback=None,
        feature_k_lags: int = 12,
        include_speed: bool = True
    ):
        """
        Args:
            candle_manager: Existing CandleManager instance
            api: OandaApi instance for fetching candle data
            pairs_config: Dict of pair -> {brick_size, pip_location, start_date}
            pivot_queue: Queue to push new pivots for scoring
            granularity: Timeframe for Renko building (S5, M1, M5, etc.)
            poll_interval: Seconds between CandleManager polls (default: 1s for fast exit detection)
            lookback: Pivot detection lookback window
        """
        super().__init__(daemon=True)
        self.candle_manager = candle_manager
        self.api = api
        self.pairs_config = pairs_config
        self.pivot_queue = pivot_queue
        if granularity is None:
            raise ValueError("RenkoEngine requires an explicit granularity (e.g., S5, M1) from config")
        self.granularity = granularity
        self.poll_interval = poll_interval
        if lookback is None:
            raise ValueError("RenkoEngine requires an explicit lookback (use config pivot_lookback)")
        self.lookback = lookback
        self.brick_event_callback = brick_event_callback
        self.k_lags = feature_k_lags
        self.include_speed = include_speed

        # State tracking per pair
        self.states: Dict[str, RenkoState] = {}
        for pair, cfg in pairs_config.items():
            brick_size = cfg['brick_size'] * (10 ** cfg['pip_location'])
            start_date = cfg.get('start_date')
            if start_date is None:
                logger.warning(
                    "%s: start_date missing; defaulting to 2024-01-01T00:00:00Z",
                    pair,
                )
                start_date = "2024-01-01T00:00:00Z"
            pair_meta = get_pair_config(pair)
            spread_pips = pair_meta.get("spread_pips", 1.8)
            self.states[pair] = RenkoState(pair, brick_size, start_date, spread_pips, self.granularity)

        # Cache for last scored pivot per pair (set by main loop after ML scoring)
        self.last_scored_pivots: Dict[str, dict] = {}

        self.running = False
        self.name = "RenkoEngine"
        self._last_trigger_log_ts: float = 0.0  # Throttles info logs for candle triggers
        self._last_no_candle_log_ts: Dict[str, float] = {}  # Throttles noisy "no new candle" logs per pair
        self._last_fetch_window_log_ts: Dict[str, float] = {}  # Throttles window diagnostics per pair
        # Rebuild throttling
        self.rebuild_cooldown_seconds: float = 5.0  # min time between rebuilds unless large move
        # Allow full brick moves without being blocked; only skip near-duplicate prices
        self.min_price_delta_fraction: float = 0.05  # fraction of brick size required beyond last rebuild price unless large move
        # Candle save cadence (batches of new candles) and atomic save helper
        self.candle_save_every: int = 3

    def run(self):
        """Main processing loop - polls CandleManager for new candles at configured granularity."""
        self.running = True
        logger.info(f"Renko Engine started (granularity: {self.granularity}, poll_interval={self.poll_interval}s)")

        # Initialize all pairs on first start
        self.initialize_all_pairs()

        while self.running:
            try:
                # Check for new candles using CandleManager
                triggered = self.candle_manager.update_timings()

                # Process each triggered pair
                for pair_info in triggered:
                    pair = pair_info['pair']
                    granularity = pair_info['granularity']

                    # Only process candles at our configured granularity
                    if granularity == self.granularity and pair in self.states:
                        # Log at DEBUG - only show console output when bricks actually form
                        logger.debug(f"{pair}: New {self.granularity} candle → checking brick formation")
                        self.process_candle_update(pair)

                # Temporary info log to confirm candle-only mode is producing triggers
                if triggered:
                    now_ts = time.time()
                    if now_ts - self._last_trigger_log_ts > 60:  # throttle to once per minute
                        pairs_str = ", ".join(f"{p['pair']}_{p['granularity']}" for p in triggered)
                        logger.info(
                            f"[{self.name}] Candle triggers ({self.granularity}, mode=candle_only): {pairs_str}"
                        )
                        self._last_trigger_log_ts = now_ts

                # Sleep until next poll
                time.sleep(self.poll_interval)

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                time.sleep(self.poll_interval)

        logger.info("Renko Engine stopped")

    def initialize_all_pairs(self):
        """Initialize all pairs with historical data on first start."""
        logger.info("=" * 60)
        logger.info("INITIALIZATION: Loading historical data for all pairs")
        logger.info("=" * 60)

        for pair, state in self.states.items():
            if not state.initialized:
                logger.info(f"{pair}: Starting initialization from {state.start_date}")
                self.initialize_pair(pair)
            else:
                logger.info(f"{pair}: Already initialized, skipping")

        logger.info("=" * 60)
        logger.info("INITIALIZATION COMPLETE")
        logger.info("=" * 60)

    def initialize_pair(self, pair: str):
        """
        Initialize a pair with historical data.

        Steps:
        1. Load candles from file (priority) OR fetch from API (fallback)
        2. Build Renko on entire dataset
        3. Detect pivots on entire dataset
        4. Engineer features on entire dataset
        5. Mark as initialized
        """
        state = self.states[pair]

        try:
            def fetch_range(from_date, to_date):
                from infrastructure.collect_data import fetch_candles

                candle_dfs = []
                time_step = INCREMENTS.get(self.granularity, 3000)  # minutes
                current_from = from_date

                while current_from < to_date:
                    current_to = current_from + pd.Timedelta(minutes=time_step)
                    if current_to > to_date:
                        current_to = to_date

                    chunk_df = fetch_candles(
                        pair=pair,
                        granularity=self.granularity,
                        date_f=current_from,
                        date_t=current_to,
                        api=self.api
                    )

                    if chunk_df is not None and not chunk_df.empty:
                        candle_dfs.append(chunk_df)
                        logger.info(f"{pair}: Fetched {len(chunk_df)} {self.granularity} from {current_from} to {current_to}")

                    current_from = current_to

                if candle_dfs:
                    combined = pd.concat(candle_dfs, ignore_index=True)
                    combined.drop_duplicates(subset=['time'], inplace=True)
                    combined.sort_values(by='time', inplace=True)
                    combined.reset_index(drop=True, inplace=True)
                    return combined

                return None

            # Try loading from file first (pre-fetched data from start_date)
            from pathlib import Path
            import pandas as pd

            candle_file = Path(f"./data/{pair}_{self.granularity}.pkl")
            m1_df = None

            if candle_file.exists():
                logger.info(f"{pair}: Loading {self.granularity} candles from file...")
                try:
                    import pickle

                    m1_df = pd.read_pickle(candle_file)
                except (pickle.UnpicklingError, EOFError, OSError, ValueError) as exc:
                    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    corrupt_path = candle_file.with_suffix(f"{candle_file.suffix}.corrupt_{timestamp}")
                    logger.warning(
                        f"{pair}: Candle file unreadable ({exc}); moving to {corrupt_path} and refetching"
                    )
                    try:
                        candle_file.replace(corrupt_path)
                    except Exception as move_exc:
                        logger.warning(f"{pair}: Failed to move corrupt candle file: {move_exc}")
                    m1_df = None

                start_dt = _parse_start_date(state.start_date)
                if start_dt is None:
                    logger.error("%s: Invalid start_date %r", pair, state.start_date)
                    return

                # Filter from start_date
                if m1_df is not None and not m1_df.empty:
                    m1_df = m1_df[m1_df['time'] >= start_dt].copy()
                    m1_df.reset_index(drop=True, inplace=True)

                if m1_df is not None:
                    logger.info(
                        f"{pair}: Loaded {len(m1_df)} {self.granularity} candles from file (from {state.start_date})"
                    )

                updated_file = False

                if m1_df is not None and not m1_df.empty:
                    first_in_file = m1_df.iloc[0]['time']
                    leading_gap = first_in_file - start_dt
                    if leading_gap > timedelta(days=2):
                        logger.info(
                            f"{pair}: Backfilling {self.granularity} from {start_dt} to {first_in_file} "
                            f"(file starts later)"
                        )
                        backfill_df = fetch_range(start_dt, first_in_file)
                        if backfill_df is not None and not backfill_df.empty:
                            m1_df = pd.concat([backfill_df, m1_df], ignore_index=True)
                            m1_df.drop_duplicates(subset=['time'], inplace=True)
                            m1_df.sort_values(by='time', inplace=True)
                            m1_df.reset_index(drop=True, inplace=True)
                            updated_file = True
                    elif leading_gap > timedelta(0):
                        logger.info(
                            f"{pair}: Skipping leading backfill gap of {leading_gap} "
                            f"(likely market closed)"
                        )

                # Fetch gap from last file timestamp to now (not first start)
                if m1_df is not None and not m1_df.empty:
                    last_in_file = m1_df.iloc[-1]['time']
                    now_utc = datetime.now(timezone.utc)
                    expected_delta = granularity_to_timedelta(self.granularity)
                    if now_utc - last_in_file < expected_delta:
                        logger.info(
                            f"{pair}: Last {self.granularity} candle {last_in_file} is recent; "
                            f"skipping gap fetch"
                        )
                    else:
                        logger.info(f"{pair}: Fetching {self.granularity} gap from {last_in_file} to now...")

                        gap_candles = self.api.get_candles_df(
                            pair,
                            granularity=self.granularity,
                            date_from=last_in_file,
                            date_to=now_utc
                        )

                        if gap_candles is not None and not gap_candles.empty:
                            logger.info(f"{pair}: Fetched {len(gap_candles)} {self.granularity} candles to close gap")
                            m1_df = pd.concat([m1_df, gap_candles], ignore_index=True)
                            m1_df.drop_duplicates(subset=['time'], inplace=True)
                            m1_df.sort_values(by='time', inplace=True)
                            m1_df.reset_index(drop=True, inplace=True)
                            updated_file = True

                if m1_df is not None and updated_file:
                    logger.info(f"{pair}: Saving updated {self.granularity} data to file ({len(m1_df)} candles)...")
                    m1_df.to_pickle(candle_file)
                    logger.info(f"{pair}: ✅ {self.granularity} data saved successfully")
                elif m1_df is not None:
                    logger.info(f"{pair}: No gap to fetch, file is up-to-date")

            if m1_df is None or m1_df.empty:
                # Fallback: Use collect_data.py to fetch from start_date
                logger.warning(f"{pair}: No {self.granularity} file found, collecting from {state.start_date} to now...")
                from_date = _parse_start_date(state.start_date)
                if from_date is None:
                    logger.error("%s: Invalid start_date %r", pair, state.start_date)
                    return
                to_date = datetime.now(timezone.utc)
                m1_df = fetch_range(from_date, to_date)

                if m1_df is not None and not m1_df.empty:
                    logger.info(f"{pair}: Collected {len(m1_df)} {self.granularity} candles total")

                    # Save newly collected candle data to file
                    logger.info(f"{pair}: Saving {self.granularity} data to file ({len(m1_df)} candles)...")
                    candle_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure data/ directory exists
                    m1_df.to_pickle(candle_file)
                    logger.info(f"{pair}: ✅ {self.granularity} data saved to {candle_file}")
                else:
                    m1_df = None
                    logger.error(f"{pair}: Failed to collect {self.granularity} data")

            if m1_df is None or m1_df.empty:
                logger.error(f"{pair}: Failed to load {self.granularity} data for initialization")
                return

            logger.info(f"{pair}: Using {len(m1_df)} {self.granularity} candles for Renko building")

            # Store candle data
            state.candle_data = m1_df
            state.last_candle_close_time = m1_df.iloc[-1]['time']

            # Build complete Renko from entire candle dataset
            logger.info(f"{pair}: Building Renko from entire {self.granularity} dataset...")
            self.build_renko_and_features(pair)

            # Mark as initialized
            state.initialized = True
            logger.info(f"{pair}: ✅ Initialization complete - {len(state.renko_df)} bricks, last {self.granularity}: {state.last_candle_close_time}")

        except Exception as e:
            logger.error(f"{pair}: Error during initialization: {e}", exc_info=True)

    def process_candle_update(self, pair: str):
        """
        Process new candle for a pair (ONGOING updates).

        Steps:
        1. Fetch new candles from last_candle_close_time to now
        2. Append to candle buffer
        3. Check if mid_c has breached brick_size threshold
        4. If yes: Rebuild Renko and features
        5. If no: Do nothing
        """
        state = self.states[pair]

        if not state.initialized:
            logger.warning(f"{pair}: Not initialized, cannot process update")
            return

        try:
            # 1. Fetch new candles from last close time
            logger.debug(f"{pair}: Fetching {self.granularity} from {state.last_candle_close_time} to now...")

            # Throttled window diagnostic (helps when API returns no data)
            now_utc = datetime.now(timezone.utc)
            now_ts = time.time()
            last_window_log = self._last_fetch_window_log_ts.get(pair, 0.0)
            if now_ts - last_window_log > 60:  # once per minute per pair
                logger.info(
                    f"{pair}: Fetch window {state.last_candle_close_time} → {now_utc} ({self.granularity})"
                )
                self._last_fetch_window_log_ts[pair] = now_ts

            new_candles = self.api.get_candles_df(
                pair,
                granularity=self.granularity,
                date_from=state.last_candle_close_time,
                date_to=now_utc
            )

            if new_candles is None or new_candles.empty:
                # Fallback: try a small count-based fetch to re-sync if the range call failed
                fallback_df = self.api.get_candles_df(pair, granularity=self.granularity, count=10)
                if fallback_df is not None and not fallback_df.empty:
                    logger.info(f"{pair}: Fallback fetched {len(fallback_df)} {self.granularity} candles via count")
                    # Keep only candles after the last known close to avoid duplicates
                    new_candles = fallback_df[fallback_df['time'] > state.last_candle_close_time]
                    # If still empty, advance the pointer to the latest candle to re-sync
                    if new_candles.empty:
                        state.last_candle_close_time = fallback_df.iloc[-1]['time']
                        logger.info(
                            f"{pair}: Re-synced last_candle_close_time to {state.last_candle_close_time} "
                            f"after fallback"
                        )
                        return
                else:
                    now_ts = time.time()
                    last_log = self._last_no_candle_log_ts.get(pair, 0)
                    if now_ts - last_log > 60:  # throttle once per minute per pair
                        logger.info(
                            f"{pair}: No new {self.granularity} candles "
                            f"from {state.last_candle_close_time} to {datetime.now(timezone.utc)}"
                        )
                        self._last_no_candle_log_ts[pair] = now_ts
                    return

            logger.debug(f"{pair}: Fetched {len(new_candles)} new {self.granularity} candle(s)")
            last_ts = new_candles.iloc[-1]['time']
            last_mid = new_candles.iloc[-1].get('mid_c', float('nan'))
            # logger.info(f"{pair}: {len(new_candles)} new {self.granularity} candle(s) through {last_ts} (mid_c={last_mid:.5f})")

            # 2. Append to candle buffer
            state.candle_data = pd.concat([state.candle_data, new_candles], ignore_index=True)
            state.last_candle_close_time = state.candle_data.iloc[-1]['time']

            # Track updates for periodic file saves (every 10 candles)
            state.candle_updates_since_save += len(new_candles)
            if state.candle_updates_since_save >= self.candle_save_every:
                self._save_candle_data(pair, state)
                state.candle_updates_since_save = 0

            # 3. Check if current mid_c has breached brick_size threshold
            current_price = state.candle_data.iloc[-1]['mid_c']

            ref_price = state.current_reference_price
            # Use close-based extremes to reduce false triggers (bricks form on closes)
            max_close = new_candles['mid_c'].max()
            min_close = new_candles['mid_c'].min()

            if ref_price is None:
                # First time, set reference from last brick
                should_rebuild = True
                trigger_price = last_mid
                logger.info(
                    f"{pair}: No reference price yet -> rebuild (last_mid={last_mid:.5f}, brick={state.brick_size:.5f})"
                )
            else:
                up_dist = max_close - ref_price
                dn_dist = ref_price - min_close
                hit_up = up_dist >= state.brick_size
                hit_dn = dn_dist >= state.brick_size
                should_rebuild = hit_up or hit_dn
                trigger_price = max_close if hit_up else min_close if hit_dn else last_mid
                # Only surface this at INFO when it triggers a rebuild; otherwise keep at DEBUG to avoid noisy logs.
                # log_msg = (
                #     f"{pair}: Batch close-range check ref {ref_price:.5f} | max_close {max_close:.5f} (+{up_dist:.5f}) "
                #     f"min_close {min_close:.5f} (-{dn_dist:.5f}) | brick {state.brick_size:.5f} -> "
                #     f"{'REBUILD' if should_rebuild else 'hold'}"
                # )
                # if should_rebuild:
                #     logger.info(log_msg)
                # else:
                #     logger.debug(log_msg)

            # 3.5 Safety: always rebuild if live price is a full brick away from last finalized brick
            force_rebuild = False
            if state.last_brick_close is not None:
                distance_from_last_brick = abs(current_price - state.last_brick_close)
                if distance_from_last_brick >= state.brick_size:
                    force_rebuild = True
                    # logger.info(
                    #     f"{pair}: Force rebuild - live price {current_price:.5f} is {distance_from_last_brick:.5f} "
                    #     f"from last brick close {state.last_brick_close:.5f} (>= 1 brick)"
                    # )

            if not should_rebuild and not force_rebuild:
                logger.debug(f"{pair}: No high/low breach for {self.granularity} batch; holding reference {state.current_reference_price}")
                return

            # 4. Rebuild throttling/duplication guards
            now = datetime.now(timezone.utc)
            large_move = False
            if ref_price is not None and trigger_price is not None:
                large_move = abs(trigger_price - ref_price) >= 2 * state.brick_size

            if state.last_rebuild_time is not None and not large_move and not force_rebuild:
                if (now - state.last_rebuild_time).total_seconds() < self.rebuild_cooldown_seconds:
                    logger.debug(
                        f"{pair}: Rebuild skipped (cooldown {self.rebuild_cooldown_seconds}s) "
                        f"last={state.last_rebuild_time}, now={now}"
                    )
                    return

            # Skip rebuilds that are within ~1 brick of the last rebuild price unless a large move was detected
            if state.last_rebuild_price is not None and not large_move:
                delta_from_last = abs(trigger_price - state.last_rebuild_price)
                if delta_from_last < state.brick_size:
                    logger.debug(
                        f"{pair}: Rebuild skipped (price delta {delta_from_last:.5f} < 1 brick since last rebuild at {state.last_rebuild_price:.5f})"
                    )
                    return

            # 5. Candle batch breached threshold - rebuild Renko and features
            # logger.info(f"{pair}: Brick threshold breached, rebuilding (trigger_price={trigger_price:.5f})")
            self.build_renko_and_features(pair, trigger_price=trigger_price)
            state.last_rebuild_price = trigger_price
            state.last_rebuild_time = now

        except Exception as e:
            logger.error(f"{pair}: Error processing {self.granularity} update: {e}", exc_info=True)

    def _save_candle_data(self, pair: str, state: RenkoState):
        """Atomic save of candle data to prevent truncated pickles."""
        try:
            from pathlib import Path

            candle_file = Path(f"./data/{pair}_{self.granularity}.pkl")
            tmp_file = candle_file.with_suffix(".pkl.tmp")

            # Ensure target directory exists before writing temp file
            tmp_file.parent.mkdir(parents=True, exist_ok=True)

            logger.debug(
                f"{pair}: Saving {self.granularity} data to file ({len(state.candle_data)} candles, "
                f"{state.candle_updates_since_save} new) -> {candle_file}"
            )
            state.candle_data.to_pickle(tmp_file)
            tmp_file.replace(candle_file)  # atomic replace on same filesystem
            logger.debug(f"{pair}: ✅ {self.granularity} data saved atomically")
        except Exception as e:
            logger.warning(f"{pair}: ⚠️ Error saving candle data: {e}", exc_info=True)

    def build_renko_and_features(self, pair: str, trigger_price: Optional[float] = None):
        """
        Build Renko and engineer features from candle data.

        This is called:
        - Once during initialization (full dataset)
        - On ongoing updates when price breaches brick threshold

        Steps:
        1. Build Renko from candle data
        2. Detect pivots
        3. Engineer features
        4. Extract new pivots
        5. Push to queue
        """
        state = self.states[pair]

        try:
            # if trigger_price is not None:
            #     logger.info(
            #         f"{pair}: REBUILD start @ price {trigger_price:.5f} (brick {state.brick_size:.5f})"
            #     )

            # 1. Build Renko from candle data (SAME as training)
            renko_df = build_renko(state.candle_data, state.brick_size)
            if renko_df.empty:
                logger.warning(f"{pair}: Renko building returned empty")
                return

            logger.debug(f"{pair}: Built {len(renko_df)} Renko bricks")

            # Store Renko data
            state.renko_df = renko_df

            # 2. Engineer features (prob_signals parity)
            renko_df = renko_df.copy()
            renko_df["brick_size"] = state.brick_size
            features, piv_df = build_enhanced_features(
                renko_df,
                k_lags=self.k_lags,
                pivot_lookback=self.lookback,
                include_speed=self.include_speed,
                include_pivots=True
            )

            # Avoid duplicate pivot flags when merging
            piv_df = piv_df.drop(columns=["pivot_high", "pivot_low", "is_pivot"], errors="ignore")

            # Keep full, non-dropped copy for brick counting (needs every brick, not only feature-complete)
            full_df = pd.concat([piv_df, features], axis=1)
            full_df_no_drop = full_df.copy()

            # Backtest parity: drop rows with incomplete feature windows, keep original indices
            full_df.dropna(inplace=True)
            if full_df.empty:
                logger.warning(f"{pair}: Feature engineering resulted in empty DataFrame - setting brick tracking from renko_df")
                # Still update brick tracking even if features failed
                # This allows ongoing updates to work
                state.last_brick_close = renko_df.iloc[-1]['mid_c']
                state.current_reference_price = state.last_brick_close
                now_ts = datetime.now(timezone.utc)
                state.last_update = now_ts
                state.last_rebuild_price = trigger_price if trigger_price is not None else state.candle_data.iloc[-1]['mid_c']
                state.last_rebuild_time = now_ts
                logger.info(f"{pair}: Brick tracking set - reference: {state.current_reference_price:.5f}")
                # Provide brick data (without dropna) for brick monitor even if features are sparse
                state.feature_df_full = full_df_no_drop.copy() if not full_df_no_drop.empty else None
                state.feature_df_complete = None
                return
                # logger.info(f"{pair}: ✅ Feature engineering complete: {len(full_df)} bricks ready (feature-complete rows)")
            # Always store full, non-dropped frame for brick monitor so every brick is counted
            state.feature_df_full = full_df_no_drop.copy()
            state.feature_df_complete = full_df.copy()

            # Mark all historical pivots as processed on first build so we only trade new ones
            if not state.initialized:
                if "pivot_high" in full_df.columns and "pivot_low" in full_df.columns:
                    pivot_mask = full_df["pivot_high"].astype(bool) | full_df["pivot_low"].astype(bool)
                else:
                    pivot_mask = (full_df['raw_pivot_high'] == 1) | (full_df['raw_pivot_low'] == 1)
                pivot_indices = full_df.index[pivot_mask].tolist()
                if pivot_indices:
                    state.last_pivot_idx = max(pivot_indices)
                    logger.info(f"{pair}: Marked {len(pivot_indices)} historical pivots as processed (will only trade new pivots)")

            # 4. Get new pivots since last update (only during ongoing updates)
            if state.initialized:
                # Use full history (backtest parity) so we never miss pivots when tail slides
                new_pivots = state.get_new_pivots(full_df)

                if not new_pivots.empty:
                    # 6. Push each new pivot to scoring queue (only recent ones)
                    now = datetime.now(timezone.utc)
                    queued_count = 0

                    for idx, row in new_pivots.iterrows():
                        pivot_time = row.get('time')

                        # Skip pivots older than 3 minutes
                        if pivot_time.tzinfo is None:
                            pivot_time = pivot_time.replace(tzinfo=timezone.utc)

                        age = (now - pivot_time).total_seconds() / 60  # minutes
                        if age > 3:
                            logger.debug(f"{pair}: Skipping old pivot ({age:.1f}m old) @ {row['Close']:.5f}")
                            continue

                        if "pivot_low" in row and bool(row["pivot_low"]):
                            pivot_type = 'low'
                        else:
                            pivot_type = 'high'

                        # Compact pivot log with key features
                        brick_run = row.get('brick_run_length', 0)
                        log_msg = f"📊 {pair} {pivot_type.upper()} pivot @ {row['Close']:.5f} | run={brick_run:.0f}"
                        logger.info(log_msg)
                        print(f"{log_msg}")  # Console: pivots are important events

                        feature_cols = list(features.columns)
                        pivot_features = {col: row.get(col, 0) for col in feature_cols}
                        pivot_signal = PivotSignal(
                            pair=pair,
                            time=pivot_time,
                            pivot_type=pivot_type,
                            price=row["Close"],
                            brick_idx=idx,
                            brick_num=full_df_no_drop.index.get_loc(idx) + 1,
                            features=pivot_features,
                            brick_size=state.brick_size,
                        )

                        self.pivot_queue.put(pivot_signal)
                        queued_count += 1

                    if queued_count > 0:
                        summary = f"✅ {pair}: {queued_count} pivot(s) → ML scoring"
                        logger.info(summary)
                        print(f"   {summary}")
                else:
                    logger.debug(f"{pair}: No new pivots detected")

            # Log last pivot info after rebuild (with ML score if available)
            if state.initialized and not full_df.empty:
                # Find last pivot in full_df
                if "pivot_high" in full_df.columns and "pivot_low" in full_df.columns:
                    pivot_mask = full_df["pivot_high"].astype(bool) | full_df["pivot_low"].astype(bool)
                else:
                    pivot_mask = (full_df['raw_pivot_high'] == 1) | (full_df['raw_pivot_low'] == 1)
                if pivot_mask.any():
                    last_pivot_idx = full_df.index[pivot_mask].max()
                    last_pivot = full_df.loc[last_pivot_idx]
                    if "pivot_low" in last_pivot and bool(last_pivot["pivot_low"]):
                        pivot_type = 'LOW'
                    else:
                        pivot_type = 'HIGH'
                    pivot_time = last_pivot['time']

                    # Ensure timezone-aware for age calculation
                    if pivot_time.tzinfo is None:
                        pivot_time = pivot_time.replace(tzinfo=timezone.utc)

                    age_minutes = (datetime.now(timezone.utc) - pivot_time).total_seconds() / 60

                    # Check if we have ML score for this pivot
                    if pair in self.last_scored_pivots:
                        score_info = self.last_scored_pivots[pair]
                        direction = score_info.get('direction', 'N/A')
                        ml_score = score_info.get('score', 0)
                        logger.info(f"{pair}: 🎯 Last pivot: {pivot_type} @ {last_pivot['Close']:.5f} | {direction} | Score: {ml_score:.3f} ({pivot_time}, {age_minutes:.1f}m ago)")
                    else:
                        # Check if pivot is too old (that's why no score)
                        if age_minutes > 3:
                            pass
                            # logger.info(f"{pair}: 🎯 Last pivot: {pivot_type} @ {last_pivot['Close']:.5f} ({pivot_time}, {age_minutes:.1f}m ago) - TOO OLD for trading")
                        else:
                            pass
                            # logger.info(f"{pair}: 🎯 Last pivot: {pivot_type} @ {last_pivot['Close']:.5f} ({pivot_time}, {age_minutes:.1f}m ago) - pending ML score")

            # 6.5. Check if new bricks formed and log details
            new_brick_count = len(full_df_no_drop)
            if state.initialized and state.last_brick_count > 0 and new_brick_count > state.last_brick_count:
                num_new_bricks = new_brick_count - state.last_brick_count
                # Use full frame (no dropna) so every brick is counted
                last_brick_df = full_df_no_drop.iloc[-num_new_bricks:]

                # Log and print brick formation (important event)
                brick_events = []
                for idx, brick in last_brick_df.iterrows():
                    brick_direction = "🟢 UP" if brick['Close'] > brick['Open'] else "🔴 DOWN"
                    brick_size_actual = abs(brick['Close'] - brick['Open'])
                    brick_num = full_df_no_drop.index.get_loc(idx) + 1

                    # Compact log format
                    log_msg = f"🧱 {pair} Brick #{brick_num}: {brick_direction} | {brick['Open']:.5f}→{brick['Close']:.5f} | {brick['time'].strftime('%H:%M:%S')}"
                    logger.info(log_msg)

                    # Console: Show brick formation prominently
                    print(f"{log_msg}")
                    brick_events.append({
                        "brick_index": int(brick_num),
                        "brick_time": brick["time"],
                        "brick_open": brick["Open"],
                        "brick_close": brick["Close"],
                    })

                # Summary
                summary = f"📊 {pair}: {new_brick_count} total bricks (+{num_new_bricks} new)"
                logger.info(summary)
                print(f"   {summary}")

                if self.brick_event_callback:
                    try:
                        self.brick_event_callback(pair, brick_events)
                    except Exception as e:
                        logger.warning(f"{pair}: Brick event callback failed: {e}")

            # 7. Update state
            now_ts = datetime.now(timezone.utc)
            state.last_brick_count = len(full_df_no_drop)
            state.last_update = now_ts

            # 8. Update brick-in-progress tracking
            state.update_brick_tracking(full_df_no_drop)
            if state.last_brick_close is not None:
                state.current_reference_price = state.last_brick_close

            # 9. Track rebuild metadata to avoid redundant rebuilds on the next M1
            state.last_rebuild_price = trigger_price if trigger_price is not None else state.candle_data.iloc[-1]['mid_c']
            state.last_rebuild_time = now_ts

        except Exception as e:
            logger.error(f"{pair}: Error building Renko and features: {e}", exc_info=True)

    def check_price_update(self, pair: str, current_price: float):
        """
        Check if current price would trigger new brick formation.
        If so, trigger candle update processing.

        This allows detecting brick formation BEFORE candle completes.

        Args:
            pair: Currency pair
            current_price: Current mid price
        """
        state = self.states.get(pair)
        if state is None:
            return

        # Check if new brick would form
        if state.check_new_brick_from_price(current_price):
            # Avoid spamming rebuilds at nearly the same price right after a rebuild
            if (
                state.last_rebuild_price is not None
                and abs(current_price - state.last_rebuild_price) < state.brick_size
            ):
                return
            logger.info(f"{pair}: Current price {current_price:.5f} triggers new brick (ref: {state.current_reference_price:.5f}), processing...")
            self.process_candle_update(pair)

    def stop(self):
        """Stop the engine gracefully."""
        logger.info("Stopping Renko Engine...")
        self.running = False

    def get_state(self, pair: str) -> Optional[RenkoState]:
        """Get current state for a pair."""
        return self.states.get(pair)

    def get_status_summary(self):
        """Print status summary for all pairs."""
        logger.info("=" * 60)
        logger.info("Renko Engine Status Summary")
        logger.info("=" * 60)

        for pair, state in self.states.items():
            if state.initialized:
                last_update_str = state.last_update.strftime('%Y-%m-%d %H:%M:%S') if state.last_update else 'Never'
                logger.info(f"{pair}:")
                logger.info(f"  Brick count: {state.last_brick_count}")
                logger.info(f"  Last pivot idx: {state.last_pivot_idx}")
                logger.info(f"  Ref price: {state.current_reference_price}")
                logger.info(f"  Last rebuild: {state.last_rebuild_time}")
                logger.info(f"  Last update: {last_update_str}")
            else:
                logger.info(f"{pair}: Not initialized")

        logger.info("=" * 60)
