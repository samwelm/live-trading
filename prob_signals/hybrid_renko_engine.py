"""
Hybrid Renko Engine - Combines candle polling with optional tick streaming.

Two modes:
1. Candle-only mode: Polls candle completion at configured granularity (0-60s latency)
2. Hybrid mode: Polls candles + checks streaming ticks for brick formation (near-zero latency)

Both modes rebuild Renko from candle data for feature parity with training.
"""
import threading
import time
import logging
from datetime import datetime
from typing import Optional, Dict
from queue import Queue

from api.oanda_api import OandaApi
from .renko_engine import RenkoEngine

logger = logging.getLogger(__name__)


class HybridRenkoEngine(RenkoEngine):
    """
    Hybrid engine that extends RenkoEngine with optional tick monitoring.

    Modes:
    - enable_tick_monitoring=False: Pure candle polling (simple, 0-60s lag)
    - enable_tick_monitoring=True: Candle polling + tick checks (complex, near-zero lag)
    """

    def __init__(
        self,
        candle_manager,
        api: OandaApi,
        pairs_config: Dict[str, dict],
        pivot_queue: Queue,
        granularity: str = "S5",
        poll_interval: int = 3,
        lookback: Optional[int] = None,
        brick_event_callback=None,
        feature_k_lags: int = 12,
        include_speed: bool = True,
        enable_tick_monitoring: bool = False,
        tick_check_interval: int = 1
    ):
        """
        Args:
            granularity: Timeframe for Renko building (S5, M1, M5, etc.)
            enable_tick_monitoring: If True, check current price between candles
            tick_check_interval: Seconds between tick price checks (if enabled)
        """
        super().__init__(
            candle_manager=candle_manager,
            api=api,
            pairs_config=pairs_config,
            pivot_queue=pivot_queue,
            granularity=granularity,
            poll_interval=poll_interval,
            lookback=lookback,
            brick_event_callback=brick_event_callback,
            feature_k_lags=feature_k_lags,
            include_speed=include_speed
        )

        self.enable_tick_monitoring = enable_tick_monitoring
        self.tick_check_interval = tick_check_interval

        # Separate thread for tick monitoring
        self.tick_monitor_thread = None

        self.name = "HybridRenkoEngine"

    def run(self):
        """Main processing loop."""
        self.running = True
        logger.info(f"Hybrid Renko Engine started (tick monitoring: {self.enable_tick_monitoring}, poll_interval={self.poll_interval}s)")

        # Start tick monitoring thread if enabled
        if self.enable_tick_monitoring:
            self.tick_monitor_thread = threading.Thread(
                target=self._tick_monitoring_loop,
                daemon=True,
                name="TickMonitor"
            )
            self.tick_monitor_thread.start()
            logger.info("Tick monitoring thread started")

        # Main candle polling loop (inherited from RenkoEngine)
        super().run()

    def _tick_monitoring_loop(self):
        """
        Monitor current prices between M1 candles.
        Triggers rebuild if price breaches brick threshold.
        """
        logger.info("Tick monitoring loop started")

        while self.running:
            try:
                # Check current price for each pair
                for pair in self.states.keys():
                    current_price = self._get_current_price(pair)

                    if current_price is not None:
                        # Check if this price would trigger new brick
                        self.check_price_update(pair, current_price)
                    elif logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"{pair}: tick monitor has no current price")

                # Sleep until next check
                time.sleep(self.tick_check_interval)

            except Exception as e:
                logger.error(f"Error in tick monitoring loop: {e}", exc_info=True)
                time.sleep(self.tick_check_interval)

        logger.info("Tick monitoring loop stopped")

    def _get_current_price(self, pair: str) -> Optional[float]:
        """
        Get current mid price for a pair.

        Options:
        1. Fetch latest pricing from API
        2. Use streaming tick if available
        3. Use last M1 close as fallback

        Args:
            pair: Currency pair

        Returns:
            Current mid price or None
        """
        try:
            # Option 1: Use last complete candle's close (mid_c) for current reference
            df = self.api.get_candles_df(pair, granularity=self.granularity, count=2)
            if df is not None and not df.empty:
                mid_close = df.iloc[-1].get("mid_c")
                if mid_close is not None:
                    return float(mid_close)

            # Fallback: Use last brick close
            state = self.states.get(pair)
            if state and state.last_brick_close:
                return state.last_brick_close

            return None

        except Exception as e:
            logger.debug(f"{pair}: Error fetching current price: {e}")
            return None

    def stop(self):
        """Stop the engine and all threads."""
        logger.info("Stopping Hybrid Renko Engine...")
        self.running = False

        # Wait for tick monitoring thread to stop
        if self.tick_monitor_thread and self.tick_monitor_thread.is_alive():
            self.tick_monitor_thread.join(timeout=3)

        logger.info("Hybrid Renko Engine stopped")


def create_engine(
    candle_manager,
    api: OandaApi,
    pairs_config: Dict[str, dict],
    pivot_queue: Queue,
    mode: str = "candle_only",
    granularity: Optional[str] = None,
    lookback: Optional[int] = None,
    poll_interval: int = 3,
    brick_event_callback=None,
    feature_k_lags: int = 12,
    include_speed: bool = True
) -> RenkoEngine:
    """
    Factory function to create appropriate engine based on mode.

    Args:
        mode: "candle_only" or "hybrid"
        granularity: Timeframe for Renko building (S5, M1, M5, etc.) from config
        lookback: Pivot detection lookback window (should match training config)

    Returns:
        Configured engine instance
    """
    if granularity is None:
        raise ValueError("create_engine requires explicit granularity from config (e.g., S5, M1)")

    if mode == "candle_only":
        logger.info(f"Creating Renko engine (simple, 0-60s latency, granularity={granularity}, lookback={lookback})")
        return RenkoEngine(
            candle_manager=candle_manager,
            api=api,
            pairs_config=pairs_config,
            pivot_queue=pivot_queue,
            granularity=granularity,
            poll_interval=poll_interval,
            lookback=lookback,
            brick_event_callback=brick_event_callback,
            feature_k_lags=feature_k_lags,
            include_speed=include_speed
        )

    elif mode == "hybrid":
        logger.info(f"Creating hybrid engine (complex, near-zero latency, granularity={granularity}, lookback={lookback})")
        return HybridRenkoEngine(
            candle_manager=candle_manager,
            api=api,
            pairs_config=pairs_config,
            pivot_queue=pivot_queue,
            granularity=granularity,
            poll_interval=poll_interval,
            lookback=lookback,
            brick_event_callback=brick_event_callback,
            feature_k_lags=feature_k_lags,
            include_speed=include_speed,
            enable_tick_monitoring=True,
            tick_check_interval=1  # Check price every second
        )

    else:
        raise ValueError(f"Unknown mode: {mode}. Use 'candle_only' or 'hybrid'")
