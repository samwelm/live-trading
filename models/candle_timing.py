# models/candle_timing.py
from dataclasses import dataclass
import datetime as dt


@dataclass(slots=True)
class CandleTiming:
    last_time: dt.datetime
    is_ready: bool = False

    def __repr__(self) -> str:
        return (
            f"last_candle:{dt.datetime.strftime(self.last_time, '%y-%m-%d %H:%M')} "
            f"is_ready:{self.is_ready}"
        )
