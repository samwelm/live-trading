import pandas as pd
from technicals.indicators import Renko
from .pairs_config import get_pair_config


def infer_brick_size(pair: str, default_brick: float, jpy_brick: float) -> float:
    # prefer pair config if exists
    cfg = get_pair_config(pair)
    if "brick_size" in cfg:
        return cfg["brick_size"]
    return jpy_brick if "JPY" in pair else default_brick


def build_renko(df_m1: pd.DataFrame, brick_size: float) -> pd.DataFrame:
    return Renko(df_m1, brick_size=brick_size)
