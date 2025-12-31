import numpy as np
import pandas as pd


def Renko(df: pd.DataFrame, brick_size: float = 0.0010, volume_col: str = "volume") -> pd.DataFrame:
    """
    Vectorized Renko chart calculation with volume aggregation.

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with price data (must contain 'mid_c', 'time' columns)
    brick_size : float
        Size of each Renko brick (default: 0.0010)
    volume_col : str
        Name of volume column in df (default: 'volume')

    Returns:
    --------
    pd.DataFrame
        Renko bricks with columns: time, mid_o, mid_h, mid_l, mid_c, direction,
        volume, brick_duration, tick_count
    """
    if len(df) == 0:
        return pd.DataFrame()

    prices = df['mid_c'].values
    times = df['time'].values
    volumes = df[volume_col].values if volume_col in df.columns else np.ones(len(df))

    renko_data = []
    reference_price = prices[0]

    i = 0
    while i < len(prices):
        current_price = prices[i]
        price_move = current_price - reference_price

        if abs(price_move) >= brick_size:
            brick_direction = 1 if price_move > 0 else -1
            num_bricks = int(abs(price_move) / brick_size)

            start_idx = max(0, i - 1) if i > 0 else 0
            end_idx = i

            period_volume = np.sum(volumes[start_idx:end_idx + 1])
            tick_count = end_idx - start_idx + 1
            brick_duration = (
                (times[end_idx] - times[start_idx]).total_seconds()
                if hasattr(times[start_idx], 'total_seconds')
                else 1
            )

            for brick_num in range(num_bricks):
                brick_open = reference_price
                brick_close = reference_price + (brick_direction * brick_size)

                brick_volume = period_volume / num_bricks

                renko_data.append({
                    'time': times[i],
                    'mid_o': brick_open,
                    'mid_h': max(brick_open, brick_close),
                    'mid_l': min(brick_open, brick_close),
                    'mid_c': brick_close,
                    'direction': brick_direction,
                    'volume': brick_volume,
                    'brick_duration': brick_duration / num_bricks,
                    'tick_count': tick_count // num_bricks + (1 if brick_num < tick_count % num_bricks else 0)
                })

                reference_price = brick_close

        i += 1

    return pd.DataFrame(renko_data)
