from dataclasses import dataclass


@dataclass(slots=True)
class ApiPrice:
    instrument: str
    ask: float
    bid: float
    sell_conv: float
    buy_conv: float

    def __init__(self, api_ob: dict, homeConversions: list[dict]):
        self.instrument = api_ob["instrument"]
        self.ask = float(api_ob["asks"][0]["price"])
        self.bid = float(api_ob["bids"][0]["price"])

        self.sell_conv = 1.0
        self.buy_conv = 1.0

        quote_currency = self.instrument.split("_")[1]

        for hc in homeConversions:
            if hc["currency"] == quote_currency:
                self.sell_conv = float(hc["accountLoss"])
                self.buy_conv = float(hc["accountGain"])
                break

    @classmethod
    def from_api(cls, api_ob: dict, home_conversions: list[dict]) -> "ApiPrice":
        return cls(api_ob, home_conversions)

    def __repr__(self) -> str:
        return f"ApiPrice() {self.instrument} {self.ask} {self.bid} {self.sell_conv:.6f} {self.buy_conv:.6f}"
