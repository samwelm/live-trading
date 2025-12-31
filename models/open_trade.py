from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from dateutil import parser


@dataclass(slots=True)
class OpenTrade:
    id: str
    instrument: str
    open_time: Optional[datetime]
    price: float
    currentUnits: float
    unrealizedPL: float
    marginUsed: float
    strategy_id: Optional[str] = None

    def __init__(self, api_ob: dict):
        self.id = api_ob["id"]
        self.instrument = api_ob["instrument"]
        self.open_time = parser.parse(api_ob["openTime"]) if "openTime" in api_ob else None
        self.price = float(api_ob["price"])
        self.currentUnits = float(api_ob["currentUnits"])
        self.unrealizedPL = float(api_ob["unrealizedPL"])
        self.marginUsed = float(api_ob["marginUsed"])
        self.strategy_id = None

        extensions = api_ob.get("clientExtensions") or api_ob.get("tradeClientExtensions")
        if extensions and "tag" in extensions:
            self.strategy_id = extensions["tag"]

    @classmethod
    def from_api(cls, api_ob: dict) -> "OpenTrade":
        return cls(api_ob)

    
