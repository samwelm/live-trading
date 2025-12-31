import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from queue import Queue

from api.oanda_api import OandaApi
from bot.candle_manager import CandleManager
from infrastructure.instrument_collection import instrumentCollection
from infrastructure.update_data import update_all_pairs
from core.signals import PivotSignal
from prob_signals.hybrid_renko_engine import create_engine

logger = logging.getLogger(__name__)


def log_message(message: str, source: str = "main") -> None:
    """Log message handler for CandleManager."""
    logger.info("[%s] %s", source, message)


@dataclass(slots=True)
class PairGranularityProvider:
    """Minimal pair/granularity provider for CandleManager (no external backing)."""

    pair_settings: dict
    default_granularity: str = "M1"
    strategies: list[dict] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        for pair, settings in self.pair_settings.items():
            self.strategies.append(
                {
                    "pair": pair,
                    "granularity": settings.get("granularity", self.default_granularity),
                }
            )

    def get_strategies(self) -> list[dict]:
        return self.strategies


def load_instruments(*, data_dir: str, api: OandaApi) -> None:
    """Load instruments from file or API (writing to disk if needed)."""
    try:
        instrumentCollection.LoadInstruments(data_dir)
        if instrumentCollection.instruments_dict:
            print(
                f"OK: Loaded {len(instrumentCollection.instruments_dict)} instruments from file"
            )
            return
        raise RuntimeError("No instruments in file")
    except Exception as exc:
        print(f"WARNING: File load failed: {exc}, trying API...")
        api_instruments = api.get_account_instruments()
        if api_instruments:
            instrumentCollection.CreateFile(api_instruments, data_dir)
            print(
                f"OK: Loaded {len(instrumentCollection.instruments_dict)} instruments from API"
            )
            return
        raise RuntimeError("Failed to load instruments from file or API")


def refresh_candles(
    *,
    api: OandaApi,
    pairs: list[str],
    granularities: list[str],
    data_dir: str,
    start_dates: dict[str, object] | None = None,
) -> bool:
    """Update candle pickles for the requested granularities."""
    all_successful = True
    for gran in granularities:
        print(f"\n   Updating {gran} data...")
        update_results = update_all_pairs(
            pairs=pairs,
            granularity=gran,
            data_dir=data_dir,
            api=api,
            force_save=True,
            start_dates=start_dates,
        )

        failed_pairs = [pair for pair, success in update_results.items() if not success]
        if failed_pairs:
            print(
                f"   WARNING: Failed to update {len(failed_pairs)} pairs at {gran}: "
                f"{', '.join(failed_pairs)}"
            )
            all_successful = False
        else:
            print(f"   OK: All {len(pairs)} pairs updated for {gran}")
    return all_successful


def create_candle_manager(
    *,
    api: OandaApi,
    pair_settings: dict,
    granularity: str,
) -> CandleManager:
    strategy_repo = PairGranularityProvider(
        pair_settings, default_granularity=granularity
    )
    return CandleManager(
        oanda_api=api,
        strategy_repository=strategy_repo,
        log_message=log_message,
    )


def _pip_location_from_instrument(instr) -> int | None:
    try:
        pip_value = float(instr.pipLocation)
        if pip_value <= 0:
            return None
        return int(round(math.log10(pip_value)))
    except Exception:
        return None


def build_pip_location_map(
    *,
    pairs: list[str],
    api: OandaApi,
) -> dict[str, int]:
    pip_location_map: dict[str, int] = {}
    for pair in pairs:
        instr = instrumentCollection.instruments_dict.get(pair)
        pip_loc = _pip_location_from_instrument(instr) if instr else None
        if pip_loc is not None:
            pip_location_map[pair] = pip_loc

    missing_pips = [pair for pair in pairs if pair not in pip_location_map]
    if missing_pips:
        api_instruments = api.get_account_instruments()
        if api_instruments:
            for inst in api_instruments:
                name = inst.get("name")
                if name in missing_pips and "pipLocation" in inst:
                    try:
                        pip_location_map[name] = int(inst["pipLocation"])
                    except (TypeError, ValueError):
                        continue
        missing_pips = [pair for pair in pairs if pair not in pip_location_map]
        if missing_pips:
            logger.warning("Missing pipLocation for pairs: %s", ", ".join(missing_pips))

    return pip_location_map


def build_pairs_config(
    *,
    pair_settings: dict,
    global_settings: dict,
    pip_location_map: dict[str, int],
) -> dict[str, dict]:
    pairs_config: dict[str, dict] = {}
    for pair, settings in pair_settings.items():
        pip_location = settings.get("pip_location")
        if pip_location is None:
            pip_location = pip_location_map.get(pair)
        if pip_location is None:
            pip_location = -4 if "_JPY" not in pair else -2
            logger.warning(
                "Pair %s missing pip_location from config/API; using %s",
                pair,
                pip_location,
            )
        pairs_config[pair] = {
            "brick_size": settings.get(
                "brick_size", global_settings.get("default_brick_size")
            ),
            "pip_location": pip_location,
            "start_date": settings.get(
                "start_date", global_settings.get("default_start_date")
            ),
        }
    return pairs_config


def start_candle_maintenance(
    api: OandaApi,
    pairs: list[str],
    granularity: str,
    start_dates: dict[str, object] | None = None,
    data_dir: str = "./data",
    interval_seconds: int = 300,
) -> threading.Thread:
    """Periodically run update_all_pairs to refresh candle pickles."""

    def loop() -> None:
        while True:
            try:
                logger.info(
                    "[CandleMaintenance] Updating %s pairs at %s", len(pairs), granularity
                )
                update_all_pairs(
                    pairs=pairs,
                    granularity=granularity,
                    data_dir=data_dir,
                    api=api,
                    force_save=True,
                    start_dates=start_dates,
                )
            except Exception as exc:
                logger.error(
                    "[CandleMaintenance] Error updating candles: %s", exc, exc_info=True
                )
            time.sleep(interval_seconds)

    thread = threading.Thread(target=loop, daemon=True, name="CandleMaintenance")
    thread.start()
    return thread


def start_renko_engine(
    *,
    api: OandaApi,
    candle_manager: CandleManager,
    pairs_config: dict[str, dict],
    mode: str,
    granularity: str,
    lookback: int,
    poll_interval: int,
    feature_k_lags: int,
    include_speed: bool,
    trade_ledger,
    target_bricks: int,
    stop_bricks: int,
):
    pivot_queue: Queue[PivotSignal] = Queue()
    pending_exit_signals = deque()
    pending_exit_lock = threading.Lock()

    def handle_brick_events(pair: str, brick_events: list[dict]) -> None:
        exit_signals = trade_ledger.handle_brick_events(
            pair=pair,
            brick_events=brick_events,
            target_bricks=target_bricks,
            stop_bricks=stop_bricks,
        )
        if exit_signals:
            with pending_exit_lock:
                pending_exit_signals.extend(exit_signals)

    renko_engine = create_engine(
        candle_manager=candle_manager,
        api=api,
        pairs_config=pairs_config,
        pivot_queue=pivot_queue,
        mode=mode,
        granularity=granularity,
        lookback=lookback,
        poll_interval=poll_interval,
        brick_event_callback=handle_brick_events,
        feature_k_lags=feature_k_lags,
        include_speed=include_speed,
    )
    renko_engine.initialize_all_pairs()
    trade_ledger.attach_engine(renko_engine)
    trade_ledger.sync_all_pairs_from_engine()
    trade_ledger.reconcile_with_broker(
        api,
        target_bricks=target_bricks,
        stop_bricks=stop_bricks,
    )
    renko_engine.start()
    return renko_engine, pivot_queue, pending_exit_signals, pending_exit_lock
