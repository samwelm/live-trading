from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from constants import defs


@dataclass(init=False, slots=True)
class TradeDecision:
    pair: str
    signal: int
    strategy_id: str
    time: datetime
    gain: float
    loss: float
    sl: float
    tp: float
    entry: float
    diff_z: float
    spread: float
    mid_c: float
    granularity: str
    valid_until: Optional[datetime]

    def __init__(self, row):
        self.pair = row.PAIR
        self.signal = row.SIGNAL
        self.strategy_id = row.strategy_id
        self.time = row.time
        self.gain = row.GAIN
        self.loss = row.LOSS
        self.sl = row.SL
        self.tp = row.TP
        self.entry = getattr(row, "ENTRY", 0.0)

        if hasattr(row, "diff_z"):
            self.diff_z = row.diff_z
        else:
            z_columns = [col for col in dir(row) if col.startswith("z_w")]
            if z_columns:
                self.diff_z = getattr(row, z_columns[0])
            else:
                self.diff_z = 0.0

        self.spread = row.SPREAD
        self.mid_c = row.mid_c
        self.granularity = row.granularity
        self.valid_until = row.valid_until if hasattr(row, "valid_until") else None

    @classmethod
    def from_row(cls, row) -> "TradeDecision":
        return cls(row)

    def __str__(self) -> str:
        dir_str = "1" if self.signal == defs.BUY else "-1"
        return (
            f"TradeDecision(): {self.pair} [{self.strategy_id}] dir:{dir_str} "
            f"entry:{self.entry:.5f} gain:{self.gain:.4f} sl:{self.sl:.4f} tp:{self.tp:.4f}"
        )
