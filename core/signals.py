from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(slots=True)
class PivotPayload:
    type: Optional[str] = None
    time: Optional[str] = None
    price: Optional[float] = None
    brick_size: Optional[float] = None
    brick_index: Optional[int] = None
    signal_source: Optional[str] = None
    pivot_score: Optional[float] = None
    pivot_threshold: Optional[float] = None
    ev_score: Optional[float] = None
    ev_threshold: Optional[float] = None
    rollover: Optional[bool] = None

    def to_dict(self) -> dict:
        payload = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if value is not None:
                payload[field_name] = value
        return payload

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> Optional["PivotPayload"]:
        if not data:
            return None
        return cls(**data)


@dataclass(slots=True)
class SignalData:
    pair: str
    time: datetime
    direction: str
    price: Optional[float]
    brick_size: float
    signal_source: str
    brick_num: Optional[int] = None
    granularity: Optional[str] = None
    pivot_type: Optional[str] = None
    pivot_data: Optional[PivotPayload] = None

    def to_dict(self) -> dict:
        payload = {
            "pair": self.pair,
            "time": self.time,
            "direction": self.direction,
            "price": self.price,
            "brick_size": self.brick_size,
            "signal_source": self.signal_source,
        }
        if self.brick_num is not None:
            payload["brick_num"] = self.brick_num
        if self.granularity is not None:
            payload["granularity"] = self.granularity
        if self.pivot_type is not None:
            payload["pivot_type"] = self.pivot_type
        if self.pivot_data is not None:
            payload["pivot_data"] = self.pivot_data.to_dict()
        return payload

    @classmethod
    def from_dict(cls, data: dict) -> "SignalData":
        pivot_payload = PivotPayload.from_dict(data.get("pivot_data"))
        return cls(
            pair=data["pair"],
            time=data["time"],
            direction=data["direction"],
            price=data.get("price"),
            brick_size=data["brick_size"],
            signal_source=data["signal_source"],
            brick_num=data.get("brick_num"),
            granularity=data.get("granularity"),
            pivot_type=data.get("pivot_type"),
            pivot_data=pivot_payload,
        )


@dataclass(slots=True)
class PivotSignal:
    pair: str
    time: datetime
    pivot_type: str
    price: float
    brick_size: float
    features: dict
    brick_idx: Optional[int] = None
    brick_num: Optional[int] = None

    def to_dict(self) -> dict:
        payload = {
            "pair": self.pair,
            "time": self.time,
            "pivot_type": self.pivot_type,
            "price": self.price,
            "brick_size": self.brick_size,
            "features": self.features,
        }
        if self.brick_idx is not None:
            payload["brick_idx"] = self.brick_idx
        if self.brick_num is not None:
            payload["brick_num"] = self.brick_num
        return payload

    @classmethod
    def from_dict(cls, data: dict) -> "PivotSignal":
        return cls(
            pair=data["pair"],
            time=data["time"],
            pivot_type=data["pivot_type"],
            price=data["price"],
            brick_size=data["brick_size"],
            features=data.get("features", {}),
            brick_idx=data.get("brick_idx"),
            brick_num=data.get("brick_num"),
        )
