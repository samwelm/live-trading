"""
Data Update Utility - Bridge gap from last timestamp to current time

This module updates existing candle data files by:
1. Reading the last timestamp from each pickle file
2. Fetching new data from last timestamp to now
3. Appending and saving the updated data
"""
import pandas as pd
import datetime as dt
from pathlib import Path
from typing import Optional
import logging

from infrastructure.collect_data import (
    INCREMENTS,
    fetch_candles,
    granularity_to_timedelta,
    save_file,
)
from api.oanda_api import OandaApi

logger = logging.getLogger(__name__)


def _parse_start_date(start_date: Optional[object]) -> Optional[dt.datetime]:
    if not start_date:
        return None
    if isinstance(start_date, dt.datetime):
        parsed = start_date
    else:
        try:
            parsed = pd.to_datetime(start_date, utc=True)
        except Exception as exc:
            logger.warning("Invalid start_date %r: %s", start_date, exc)
            return None
    if isinstance(parsed, pd.Timestamp):
        if pd.isna(parsed):
            return None
        parsed = parsed.to_pydatetime()
    if isinstance(parsed, dt.datetime):
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    return None


def _clean_time_index(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize time column for consistent gap detection."""
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df.drop_duplicates(subset=["time"], inplace=True)
    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _find_missing_ranges(
    times: pd.Series,
    window_start: dt.datetime,
    window_end: dt.datetime,
    expected_delta: dt.timedelta,
    tolerance: dt.timedelta,
) -> list[tuple[dt.datetime, dt.datetime]]:
    """Return missing [start, end] ranges within the requested window."""
    times_in_window = times[(times >= window_start) & (times <= window_end)].drop_duplicates().sort_values()

    if times_in_window.empty:
        return [(window_start, window_end)]

    missing: list[tuple[dt.datetime, dt.datetime]] = []

    first_time = times_in_window.iloc[0]
    if first_time - window_start > tolerance:
        missing.append((window_start, first_time))

    prev = first_time
    for current in times_in_window.iloc[1:]:
        gap = current - prev
        if gap > expected_delta + tolerance:
            gap_start = prev + expected_delta
            gap_end = min(current, window_end)
            if gap_start < gap_end:
                missing.append((gap_start, gap_end))
        prev = current

    last_time = times_in_window.iloc[-1]
    if window_end - last_time > tolerance:
        gap_start = last_time + expected_delta
        missing.append((gap_start, window_end))

    return missing


def fetch_candles_to_now(pair: str, granularity: str, date_from: dt.datetime, api: OandaApi):
    """
    Fetch candles from date_from to OANDA's current server time.
    Avoids 'time in the future' errors by not specifying 'to' parameter.

    Args:
        pair: Currency pair
        granularity: Timeframe
        date_from: Start date
        api: OANDA API instance

    Returns:
        DataFrame of candles or None
    """
    attempts = 0
    while attempts < 3:
        # Pass date_from without date_to - OANDA will use server's current time
        candles_df = api.get_candles_df(
            pair,
            granularity=granularity,
            date_from=date_from,
            # date_to deliberately omitted
        )

        if candles_df is not None:
            break

        attempts += 1

    if candles_df is not None and not candles_df.empty:
        return candles_df
    else:
        return None


def rebuild_corrupted_file(
    pair: str,
    granularity: str,
    data_dir: str,
    api: OandaApi,
    lookback_days: int = 30,
    start_date: Optional[dt.datetime] = None,
) -> bool:
    """
    Rebuild a corrupted candle file from scratch using recent data.

    Args:
        pair: Currency pair
        granularity: Timeframe
        data_dir: Data directory
        api: OANDA API instance
        lookback_days: Days to look back for rebuild

    Returns:
        True if rebuild successful, False otherwise
    """
    try:
        # Calculate date range for rebuild using OANDA server time
        to_date = api.get_server_time()
        if to_date is None:
            logger.error(f"{pair} {granularity}: Failed to get OANDA server time, using local time")
            to_date = dt.datetime.now(dt.timezone.utc)
        if start_date is not None:
            from_date = start_date
            if from_date >= to_date:
                logger.warning(
                    "%s %s: start_date %s is after server time %s; using lookback",
                    pair,
                    granularity,
                    from_date,
                    to_date,
                )
                from_date = to_date - dt.timedelta(days=lookback_days)
        else:
            from_date = to_date - dt.timedelta(days=lookback_days)

        logger.info("%s %s: Rebuilding from %s to %s", pair, granularity, from_date, to_date)

        # Determine fetch strategy based on granularity
        if granularity in INCREMENTS:
            time_step = INCREMENTS[granularity]
        else:
            # Default to ~3000 candles worth of time
            time_step = 3000  # minutes

        # Fetch data in batches
        candle_dfs = []
        current_from = from_date

        while current_from < to_date:
            current_to = current_from + dt.timedelta(minutes=time_step)
            if current_to > to_date:
                current_to = to_date

            candles = fetch_candles(
                pair=pair,
                granularity=granularity,
                date_f=current_from,
                date_t=current_to,
                api=api
            )

            if candles is not None and not candles.empty:
                candle_dfs.append(candles)
                logger.info(f"{pair} {granularity}: Fetched {len(candles)} candles from {current_from} to {current_to}")

            current_from = current_to

        if not candle_dfs:
            logger.error(f"{pair} {granularity}: No data fetched for rebuild")
            return False

        # Combine and save
        combined_df = pd.concat(candle_dfs, ignore_index=True)
        save_file(combined_df, f"{Path(data_dir).as_posix().rstrip('/')}/", granularity, pair)

        logger.info(f"{pair} {granularity}: Rebuilt successfully with {len(combined_df)} candles")
        return True

    except Exception as e:
        logger.error(f"Error rebuilding {pair} {granularity}: {e}", exc_info=True)
        return False


def get_last_timestamp(file_path: Path) -> Optional[dt.datetime]:
    """
    Read the last timestamp from a pickle file.

    Args:
        file_path: Path to the pickle file

    Returns:
        Last timestamp as datetime, or None if file doesn't exist/is corrupted
    """
    try:
        if not file_path.exists():
            return None

        df = pd.read_pickle(file_path)
        if df.empty:
            return None

        # Get last timestamp
        last_time = df['time'].max()

        # Convert to datetime if needed
        if isinstance(last_time, str):
            last_time = pd.to_datetime(last_time)

        return last_time

    except Exception as e:
        logger.warning(f"Error reading {file_path.name}: {e}")
        return None


def update_candle_file(
    pair: str,
    granularity: str,
    data_dir: str,
    api: OandaApi,
    rebuild_if_corrupted: bool = True,
    default_lookback_days: int = 30,
    start_date: Optional[object] = None,
    force_save: bool = False,
) -> bool:
    """
    Update a single candle file from last timestamp to now.

    Args:
        pair: Currency pair (e.g., 'GBP_USD')
        granularity: Timeframe (e.g., 'S5', 'M1')
        data_dir: Directory containing pickle files
        api: OANDA API instance
        rebuild_if_corrupted: If True, rebuild corrupted files from scratch
        default_lookback_days: Days to look back when rebuilding corrupted files

    Returns:
        True if update successful, False otherwise
    """
    file_path = Path(data_dir) / f"{pair}_{granularity}.pkl"
    file_prefix = f"{Path(data_dir).as_posix().rstrip('/')}/"

    try:
        parsed_start_date = _parse_start_date(start_date)
        if parsed_start_date is not None:
            rebuild_desc = f"start_date {parsed_start_date}"
            rebuild_kwargs = {"start_date": parsed_start_date}
        else:
            rebuild_desc = f"last {default_lookback_days} days"
            rebuild_kwargs = {}

        if not file_path.exists():
            logger.warning(
                "%s: File does not exist, rebuilding from %s",
                file_path.name,
                rebuild_desc,
            )
            return rebuild_corrupted_file(
                pair,
                granularity,
                data_dir,
                api,
                default_lookback_days,
                **rebuild_kwargs,
            )

        try:
            existing_df = pd.read_pickle(file_path)
        except Exception as exc:
            if rebuild_if_corrupted:
                logger.warning(
                    "%s: Corrupted file detected (%s), rebuilding from %s",
                    file_path.name,
                    exc,
                    rebuild_desc,
                )
                return rebuild_corrupted_file(
                    pair,
                    granularity,
                    data_dir,
                    api,
                    default_lookback_days,
                    **rebuild_kwargs,
                )
            logger.warning(f"{file_path.name}: Corrupted file detected ({exc}), skipping")
            return False

        if existing_df is None or existing_df.empty:
            if rebuild_if_corrupted:
                logger.warning(
                    "%s: Empty or unreadable, rebuilding from %s",
                    file_path.name,
                    rebuild_desc,
                )
                return rebuild_corrupted_file(
                    pair,
                    granularity,
                    data_dir,
                    api,
                    default_lookback_days,
                    **rebuild_kwargs,
                )
            logger.warning(f"{file_path.name}: Empty or unreadable, skipping")
            return False

        existing_df = _clean_time_index(existing_df)

        expected_delta = granularity_to_timedelta(granularity)
        batch_size_minutes = INCREMENTS.get(granularity, 3000)

        server_time = api.get_server_time()
        if server_time is None:
            logger.warning(f"{pair} {granularity}: Failed to get OANDA server time, using local UTC time")
            server_time = dt.datetime.now(dt.timezone.utc)

        # Incremental fetch: start after the last known candle
        last_time = existing_df["time"].max()
        request_start = last_time + expected_delta
        request_end = server_time

        if request_end <= request_start:
            if force_save:
                save_file(existing_df, file_prefix, granularity, pair)
                logger.info(f"{pair} {granularity}: No time advance; force-saved existing data")
            else:
                logger.info(f"{pair} {granularity}: No time advance; nothing to fetch")
            return True

        new_candle_dfs: list[pd.DataFrame] = []

        current_from = request_start
        while current_from < request_end:
            current_to = current_from + dt.timedelta(minutes=batch_size_minutes)
            if current_to > request_end:
                current_to = request_end

            candles = fetch_candles(
                pair=pair,
                granularity=granularity,
                date_f=current_from,
                date_t=current_to,
                api=api,
            )

            if candles is not None and not candles.empty:
                new_candle_dfs.append(candles)
                logger.info(f"{pair} {granularity}: Fetched {len(candles)} candles from {current_from} to {current_to}")
            else:
                logger.info(f"{pair} {granularity}: No candles from {current_from} to {current_to}")

            current_from = current_to

        # Always finish with a short open-ended fetch to cover server-time drift (defaults to count=10)
        tail_df = api.get_candles_df(pair, granularity=granularity)
        if tail_df is not None and not tail_df.empty:
            # Keep only candles after last_time to avoid dupes
            tail_df = tail_df[tail_df["time"] > last_time]
            if not tail_df.empty:
                new_candle_dfs.append(tail_df)
                logger.info(f"{pair} {granularity}: Added open-ended tail fetch ({len(tail_df)} candles)")

        if not new_candle_dfs:
            if force_save:
                save_file(existing_df, file_prefix, granularity, pair)
                logger.info(f"{pair} {granularity}: No new candles fetched - force-saved existing data")
            else:
                logger.info(f"{pair} {granularity}: No new candles fetched")
            return True

        combined_df = pd.concat([existing_df] + new_candle_dfs, ignore_index=True)
        save_file(combined_df, file_prefix, granularity, pair)

        logger.info(f"{pair} {granularity}: Added {sum(len(df) for df in new_candle_dfs)} new candles (before dedupe)")
        return True

    except Exception as e:
        logger.error(f"Error updating {pair} {granularity}: {e}", exc_info=True)
        return False


def update_all_pairs(
    pairs: list,
    granularity: str,
    data_dir: str,
    api: OandaApi,
    force_save: bool = False,
    start_dates: Optional[dict[str, object]] = None,
    default_lookback_days: int = 30,
) -> dict:
    """
    Update candle files for multiple pairs.

    Args:
        pairs: List of currency pairs
        granularity: Timeframe (e.g., 'S5', 'M1')
        data_dir: Directory containing pickle files
        api: OANDA API instance

    Returns:
        Dictionary with update results: {pair: success_bool}
    """
    results = {}

    logger.info(f"Starting data update for {len(pairs)} pairs at {granularity} granularity")

    for pair in pairs:
        start_date = start_dates.get(pair) if start_dates else None
        success = update_candle_file(
            pair,
            granularity,
            data_dir,
            api,
            force_save=force_save,
            start_date=start_date,
            default_lookback_days=default_lookback_days,
        )
        results[pair] = success

    # Summary
    successful = sum(1 for v in results.values() if v)
    logger.info(f"Update complete: {successful}/{len(pairs)} pairs updated successfully")

    return results


def verify_data_integrity(file_path: Path) -> bool:
    """
    Verify that a pickle file is not corrupted and has valid data.

    Args:
        file_path: Path to pickle file

    Returns:
        True if file is valid, False otherwise
    """
    try:
        df = pd.read_pickle(file_path)

        # Check basic integrity
        if df.empty:
            logger.warning(f"{file_path.name}: File is empty")
            return False

        # Check required columns
        required_cols = ['time', 'mid_o', 'mid_h', 'mid_l', 'mid_c']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.warning(f"{file_path.name}: Missing columns: {missing_cols}")
            return False

        # Check for time continuity (no major gaps)
        df_sorted = df.sort_values('time')
        time_diffs = df_sorted['time'].diff()

        logger.info(f"{file_path.name}: {len(df)} candles, "
                   f"from {df['time'].min()} to {df['time'].max()}")

        return True

    except Exception as e:
        logger.error(f"{file_path.name}: Integrity check failed - {e}")
        return False
