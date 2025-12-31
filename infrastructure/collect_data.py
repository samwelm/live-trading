import datetime as dt
from pathlib import Path

import pandas as pd

from api.oanda_api import OandaApi

CANDLE_COUNT = 3000

INCREMENTS = {
    "S5": (5 / 60) * CANDLE_COUNT,
    "S10": (10 / 60) * CANDLE_COUNT,
    "S30": (30 / 60) * CANDLE_COUNT,
    "M1": 1 * CANDLE_COUNT,
    "M5": 5 * CANDLE_COUNT,
    "M15": 15 * CANDLE_COUNT,
    "M30": 30 * CANDLE_COUNT,
    "H1": 60 * CANDLE_COUNT,
    "H3": 120 * CANDLE_COUNT,
    "H4": 240 * CANDLE_COUNT,
    "H6": 360 * CANDLE_COUNT,
    "H8": 480 * CANDLE_COUNT,
    "D": 1440 * CANDLE_COUNT,
    "W": 10080 * CANDLE_COUNT,
}


def granularity_to_timedelta(granularity: str) -> dt.timedelta:
    """Convert OANDA granularity string to a timedelta for gap detection."""
    unit = granularity[0].upper()
    if unit == "S":
        return dt.timedelta(seconds=int(granularity[1:]))
    if unit == "M":
        return dt.timedelta(minutes=int(granularity[1:]))
    if unit == "H":
        return dt.timedelta(hours=int(granularity[1:]))
    if granularity == "D":
        return dt.timedelta(days=1)
    if granularity == "W":
        return dt.timedelta(weeks=1)
    raise ValueError(f"Unsupported granularity '{granularity}'")


def save_file(final_df: pd.DataFrame, file_prefix: str, granularity: str, pair: str) -> None:
    filename = f"{file_prefix}{pair}_{granularity}.pkl"

    final_df.drop_duplicates(subset=["time"], inplace=True)
    final_df.sort_values(by="time", inplace=True)
    final_df.reset_index(drop=True, inplace=True)
    final_df.to_pickle(filename)

    s1 = f"*** {pair} {granularity} {final_df.time.min()} {final_df.time.max()}"
    print(f"*** {s1} --> {final_df.shape[0]} candles ***")


def fetch_candles(
    pair: str,
    granularity: str,
    date_f: dt.datetime,
    date_t: dt.datetime,
    api: OandaApi,
) -> pd.DataFrame | None:
    attempts = 0

    while attempts < 3:
        candles_df = api.get_candles_df(
            pair,
            granularity=granularity,
            date_from=date_f,
            date_to=date_t,
        )

        if candles_df is not None:
            break

        attempts += 1

    if candles_df is not None and not candles_df.empty:
        return candles_df

    return None
