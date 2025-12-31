#!/usr/bin/env python3
"""
Enhanced feature engineering for Renko ML (live-trading only).

Includes:
- Direction lags and rolling stats
- Session indicators
- Speed/duration z-score
- Momentum features
- Volatility features
- Pivot context features
- Market regime features
- Clean pivot detection
"""

from typing import Tuple

import numpy as np
import pandas as pd

try:
    from numba import njit
except Exception:  # pragma: no cover - optional speedup only
    def njit(*args, **kwargs):
        def wrapper(func):
            return func
        if args and callable(args[0]):
            return args[0]
        return wrapper


@njit
def _deduplicate_pivots_numba(
    highs: np.ndarray,
    lows: np.ndarray,
    raw_pivots: np.ndarray,
    lookback: int,
) -> np.ndarray:
    """
    Causal deduplication of pivots.
    Keeps only the strongest pivot within a backward lookback window.
    """
    n = len(raw_pivots)
    clean_pivots = raw_pivots.copy()

    for i in range(n):
        if raw_pivots[i] == 0:
            continue

        current_type = raw_pivots[i]
        current_high = highs[i]
        current_low = lows[i]
        lookback_start = max(0, i - lookback)

        if current_type == 1 or current_type == 3:
            is_strongest = True
            for j in range(lookback_start, i):
                if raw_pivots[j] == 1 or raw_pivots[j] == 3:
                    if highs[j] >= current_high:
                        is_strongest = False
                        break

            if not is_strongest:
                if current_type == 1:
                    clean_pivots[i] = 0
                else:
                    clean_pivots[i] = 2

        current_type = clean_pivots[i]
        if current_type == 2 or current_type == 3:
            is_strongest = True
            for j in range(lookback_start, i):
                if raw_pivots[j] == 2 or raw_pivots[j] == 3:
                    if lows[j] <= current_low:
                        is_strongest = False
                        break

            if not is_strongest:
                if clean_pivots[i] == 2:
                    clean_pivots[i] = 0
                elif clean_pivots[i] == 3:
                    clean_pivots[i] = 1

    return clean_pivots


def detect_clean_pivots(
    df: pd.DataFrame,
    lookback: int = 10,
    deduplicate: bool = True,
) -> pd.DataFrame:
    """
    Detect clean pivots with optional deduplication.

    Pivot encoding:
        0 = no pivot
        1 = pivot high
        2 = pivot low
        3 = both (rare)
    """
    df = df.copy()

    if 'High' not in df.columns and 'mid_h' in df.columns:
        df['High'] = df['mid_h']
        df['Low'] = df['mid_l']

    highs = df['High'].values
    lows = df['Low'].values

    prev_max = df['High'].rolling(lookback).max().shift(1)
    prev_min = df['Low'].rolling(lookback).min().shift(1)

    pivot_high = (df['High'] > prev_max) & prev_max.notna()
    pivot_low = (df['Low'] < prev_min) & prev_min.notna()

    raw_pivots = np.zeros(len(df), dtype=np.int8)
    raw_pivots[pivot_high.values] += 1
    raw_pivots[pivot_low.values] += 2

    if deduplicate:
        clean_pivots = _deduplicate_pivots_numba(highs, lows, raw_pivots, lookback)
    else:
        clean_pivots = raw_pivots.copy()

    df['raw_pivot'] = raw_pivots
    df['clean_pivot'] = clean_pivots
    df['pivot_high'] = (clean_pivots == 1) | (clean_pivots == 3)
    df['pivot_low'] = (clean_pivots == 2) | (clean_pivots == 3)
    df['is_pivot'] = clean_pivots > 0

    return df


def build_enhanced_features(
    df: pd.DataFrame,
    k_lags: int = 12,
    pivot_lookback: int = 10,
    include_speed: bool = True,
    include_pivots: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build feature set for live Renko ML scoring.

    Returns:
        X: Feature DataFrame
        df: Renko DataFrame with pivot columns populated
    """
    df = df.copy().sort_values("time").reset_index(drop=True)

    if 'High' not in df.columns and 'mid_h' in df.columns:
        df['High'] = df['mid_h']
        df['Low'] = df['mid_l']
        df['Open'] = df['mid_o']
        df['Close'] = df['mid_c']

    close = df['mid_c'] if 'mid_c' in df.columns else df['Close']
    direction = df['direction']

    X = pd.DataFrame(index=df.index)

    for j in range(1, k_lags + 1):
        X[f"d_lag{j}"] = direction.shift(j)

    for w in [3, 5, 8, 13, 21]:
        X[f"sum_dir_{w}"] = direction.rolling(w).sum()
        X[f"p_up_{w}"] = (direction == 1).rolling(w).mean()

    X["balance_21"] = X["sum_dir_21"].abs() / 21.0
    X["reversal"] = (direction != direction.shift(1)).astype(int)

    streak = np.zeros(len(df), dtype=int)
    prev, run = 0, 0
    dirs = direction.to_numpy().astype(int)
    for i, d in enumerate(dirs):
        if d == prev:
            run += 1
        else:
            run = 1
            prev = d
        streak[i] = run * d
    X["streak"] = streak

    h = df["time"].dt.hour
    session = pd.cut(h, [-1, 6, 12, 20, 23], labels=["ASIA", "EUROPE", "US", "OFF"])
    X = pd.concat([X, pd.get_dummies(session, prefix="sess")], axis=1)

    if include_speed and "brick_duration" in df.columns:
        bd = pd.to_numeric(df["brick_duration"], errors="coerce")
        mu = bd.rolling(500, min_periods=50).mean()
        sd = bd.rolling(500, min_periods=50).std()
        X["bd_z"] = (bd - mu) / (sd + 1e-9)

    X["ret1"] = close.pct_change(fill_method=None)
    X["mom_5"] = close.pct_change(5, fill_method=None)
    X["mom_10"] = close.pct_change(10, fill_method=None)

    X["dir_flips_5"] = (direction.diff() != 0).rolling(5).sum()

    runs = (direction != direction.shift()).cumsum()
    X["run_length"] = df.groupby(runs)["direction"].transform("count")

    logret = np.log(close / close.shift(1))
    X["vol_10"] = logret.rolling(10).std()
    X["vol_20"] = logret.rolling(20).std()

    brick_size = df['brick_size'].iloc[0] if 'brick_size' in df.columns else None
    if brick_size is None:
        diffs = close.diff().abs()
        diffs = diffs[diffs > 0]
        brick_size = diffs.mode().iloc[0] if len(diffs) > 0 else 0.001

    X["slope_bricks_5"] = (close - close.shift(5)) / (5 * brick_size)
    X["slope_bricks_10"] = (close - close.shift(10)) / (10 * brick_size)

    ma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    X["close_z_20"] = (close - ma20) / (std20 + 1e-9)

    ma50 = close.rolling(50).mean()
    std50 = close.rolling(50).std()
    X["close_z_50"] = (close - ma50) / (std50 + 1e-9)

    if 'High' in df.columns and 'Low' in df.columns:
        range_ratio = (df['High'] - df['Low']) / close
        rr_mean = range_ratio.rolling(50).mean()
        rr_std = range_ratio.rolling(50).std()
        X["range_ratio_z"] = (range_ratio - rr_mean) / (rr_std + 1e-9)

    if include_pivots:
        df_piv = detect_clean_pivots(df, lookback=pivot_lookback, deduplicate=True)

        X["pivot_high"] = df_piv["pivot_high"].astype(int)
        X["pivot_low"] = df_piv["pivot_low"].astype(int)
        X["is_pivot"] = df_piv["is_pivot"].astype(int)

        pivot_flag = df_piv["is_pivot"]
        last_piv_close = []
        last_val = np.nan
        for c, p in zip(close, pivot_flag):
            if p:
                last_val = c
            last_piv_close.append(last_val)
        X["dist_last_pivot_bricks"] = (
            close - pd.Series(last_piv_close, index=close.index)
        ).abs() / brick_size

        X["bricks_since_pivot"] = (~pivot_flag).astype(int)
        X["bricks_since_pivot"] = X["bricks_since_pivot"].groupby(pivot_flag.cumsum()).cumsum()

        last_piv_high_close = []
        last_val = np.nan
        for c, p in zip(close, df_piv["pivot_high"]):
            if p:
                last_val = c
            last_piv_high_close.append(last_val)
        X["dist_last_pivot_high"] = (close - pd.Series(last_piv_high_close, index=close.index)) / brick_size

        last_piv_low_close = []
        last_val = np.nan
        for c, p in zip(close, df_piv["pivot_low"]):
            if p:
                last_val = c
            last_piv_low_close.append(last_val)
        X["dist_last_pivot_low"] = (close - pd.Series(last_piv_low_close, index=close.index)) / brick_size

        df["pivot_high"] = df_piv["pivot_high"]
        df["pivot_low"] = df_piv["pivot_low"]
        df["is_pivot"] = df_piv["is_pivot"]

    return X, df
