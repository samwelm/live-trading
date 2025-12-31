from dataclasses import dataclass


@dataclass(slots=True)
class Instrument:
    name: str
    ins_type: str
    displayName: str
    pipLocation: float
    tradeUnitsPrecision: int
    marginRate: float
    displayPrecision: int

    def __post_init__(self) -> None:
        self.pipLocation = pow(10, self.pipLocation)
        self.marginRate = float(self.marginRate)

    @classmethod
    def FromApiObject(cls, ob: dict) -> "Instrument":
        return cls(
            ob["name"],
            ob["type"],
            ob["displayName"],
            ob["pipLocation"],
            ob["tradeUnitsPrecision"],
            ob["marginRate"],
            ob["displayPrecision"],
        )

    @classmethod
    def from_api(cls, ob: dict) -> "Instrument":
        return cls.FromApiObject(ob)
