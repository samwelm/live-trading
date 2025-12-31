from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(slots=True)
class EvUpdateRow:
    pair: str
    brick_index: int
    ev_long: Optional[float]
    ev_short: Optional[float]
    ev_signal: Optional[int]

    def to_tuple(self) -> Tuple[str, int, Optional[float], Optional[float], Optional[int]]:
        return (self.pair, int(self.brick_index), self.ev_long, self.ev_short, self.ev_signal)
