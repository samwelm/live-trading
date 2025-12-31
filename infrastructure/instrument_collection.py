import json
from dataclasses import dataclass, field

from models.instrument import Instrument


@dataclass(slots=True)
class InstrumentCollection:
    FILENAME = "instruments.json"
    API_KEYS = [
        "name",
        "type",
        "displayName",
        "pipLocation",
        "displayPrecision",
        "tradeUnitsPrecision",
        "marginRate",
    ]
    instruments_dict: dict = field(default_factory=dict)

    def LoadInstruments(self, path: str) -> None:
        self.instruments_dict = {}
        fileName = f"{path}/{self.FILENAME}"
        with open(fileName, "r") as f:
            data = json.load(f)
            for k, v in data.items():
                self.instruments_dict[k] = Instrument.from_api(v)

    def CreateFile(self, data, path: str) -> None:
        if data is None:
            print("Instrument file creation failed")
            return

        instruments_dict = {}
        for i in data:
            key = i['name']
            instruments_dict[key] = {k: i[k] for k in self.API_KEYS}

        fileName = f"{path}/{self.FILENAME}"
        with open(fileName, "w") as f:
            f.write(json.dumps(instruments_dict, indent=2))

    def PrintInstruments(self) -> None:
        for key, value in self.instruments_dict.items():
            print(key, value)
        print(len(self.instruments_dict.keys()), "instruments")


instrumentCollection = InstrumentCollection()
