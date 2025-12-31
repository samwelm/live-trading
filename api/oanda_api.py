# api/oanda_api.py
"""Singleton wrapper around OANDA’s REST v20 API."""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime as dt
from typing import List, Dict, Any, Optional

import pandas as pd
import requests
from dateutil import parser

import constants.defs as defs
from infrastructure.instrument_collection import instrumentCollection as ic
from models.api_price import ApiPrice
from models.open_trade import OpenTrade

__all__ = ["OandaApi", "oanda_api"]

logger = logging.getLogger(__name__)


class OandaApi:
    """Singleton HTTP client for OANDA's v20 REST API."""

    _instance: "OandaApi | None" = None
    _lock = threading.Lock()  # to guard first‑time construction

    # ──────────────────────────────────────────────────────────
    # Construction
    # ──────────────────────────────────────────────────────────
    def __new__(cls, *args, **kwargs):  # noqa: D401
        # Double‑checked locking – safe for Python threads
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # still?
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        # Heavy init should only execute once
        if getattr(self, "_initialised", False):
            return

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {defs.API_KEY}",
                "Content-Type": "application/json",
            }
        )
        # Basic config sanity logging (helps when API returns empty)
        if not defs.OANDA_URL or not defs.ACCOUNT_ID or not defs.API_KEY:
            logger.warning(
                f"OandaApi init missing config: "
                f"OANDA_URL={bool(defs.OANDA_URL)} ACCOUNT_ID={bool(defs.ACCOUNT_ID)} API_KEY={bool(defs.API_KEY)}"
            )

        self._initialised = True

    # ──────────────────────────────────────────────────────────
    # Low‑level request helper
    # ──────────────────────────────────────────────────────────
    def _request(
        self,
        url: str,
        verb: str = "get",
        expected_code: int = 200,
        *,
        params: Dict[str, Any] | None = None,
        data: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
    ) -> tuple[bool, Dict[str, Any]]:
        full_url = f"{defs.OANDA_URL}/{url}"
        if data is not None:
            data = json.dumps(data)

        try:
            response: requests.Response | None
            match verb.lower():
                case "get":
                    response = self.session.get(full_url, params=params, data=data, headers=headers)
                case "post":
                    response = self.session.post(full_url, params=params, data=data, headers=headers)
                case "put":
                    response = self.session.put(full_url, params=params, data=data, headers=headers)
                case _:
                    return False, {"error": f"Unsupported verb '{verb}'"}

            if response.status_code == expected_code:
                return True, response.json()

            # Non‑expected status
            try:
                err_body = response.json()
            except json.JSONDecodeError:
                err_body = {
                    "status": response.status_code,
                    "message": response.text[:120],
                }

            logger.warning(
                f"OANDA request failed {verb.upper()} {full_url} status={response.status_code} "
                f"expected={expected_code} params={params} body={err_body}"
            )
            return False, err_body

        except requests.RequestException as exc:
            logger.warning(f"OANDA request exception {verb.upper()} {full_url}: {exc}")
            return False, {"exception": str(exc)}

    # ──────────────────────────────────────────────────────────
    # Account helpers
    # ──────────────────────────────────────────────────────────
    def _account_ep(self, ep: str, key: str) -> Optional[Dict[str, Any]]:
        ok, data = self._request(f"accounts/{defs.ACCOUNT_ID}/{ep}")
        return data.get(key) if ok else None

    def get_account_summary(self) -> Optional[Dict[str, Any]]:
        return self._account_ep("summary", "account")

    def get_account_instruments(self) -> Optional[List[Dict[str, Any]]]:
        return self._account_ep("instruments", "instruments")

    # ──────────────────────────────────────────────────────────
    # Candle data
    # ──────────────────────────────────────────────────────────
    def fetch_candles(
        self,
        pair: str,
        *,
        count: int = 10,
        granularity: str = "H1",
        price: str = "MBA",
        date_from: dt | None = None,
        date_to: dt | None = None,
    ) -> Optional[List[Dict[str, Any]]]:
        url = f"instruments/{pair}/candles"
        params: Dict[str, Any] = {"granularity": granularity, "price": price}

        if date_from and date_to:
            # Both from and to specified
            fmt = "%Y-%m-%dT%H:%M:%SZ"
            params["from"] = dt.strftime(date_from, fmt)
            params["to"] = dt.strftime(date_to, fmt)
        elif date_from:
            # Only from specified - OANDA will use server's current time as 'to'
            fmt = "%Y-%m-%dT%H:%M:%SZ"
            params["from"] = dt.strftime(date_from, fmt)
        else:
            # Neither from nor to - use count
            params["count"] = count

        ok, data = self._request(url, params=params)
        return data.get("candles") if ok else None

    def get_candles_df(self, pair: str, **kwargs) -> pd.DataFrame:
        candles = self.fetch_candles(pair, **kwargs)
        if not candles:
            date_from = kwargs.get("date_from")
            date_to = kwargs.get("date_to")
            count = kwargs.get("count")
            logger.warning(
                f"get_candles_df: empty response for {pair} "
                f"granularity={kwargs.get('granularity')} "
                f"count={count} from={date_from} to={date_to}"
            )
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for c in candles:
            if not c["complete"]:
                continue
            row = {"time": parser.parse(c["time"]), "volume": c["volume"]}
            for side in ("mid", "bid", "ask"):
                if side in c:
                    row |= {f"{side}_{k}": float(v) for k, v in c[side].items()}
            rows.append(row)
        return pd.DataFrame(rows)

    def last_complete_candle(self, pair: str, granularity: str) -> dt | None:
        df = self.get_candles_df(pair, granularity=granularity, count=10)
        return df.iloc[-1].time if not df.empty else None

    # ──────────────────────────────────────────────────────────
    # Trades
    # ──────────────────────────────────────────────────────────
    def place_trade(
        self,
        pair: str,
        units: float,
        direction: int,
        *,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        client_extensions: Dict[str, str] | None = None,
        trade_client_extensions: Dict[str, str] | None = None,
        order_client_extensions: Dict[str, str] | None = None,
    ) -> Optional[str]:
        """Place a MARKET order with optional SL/TP and client extensions."""

        def _normalize_extensions(payload: Dict[str, str] | None) -> Dict[str, str] | None:
            if not payload:
                return None
            # OANDA expects id/tag/comment keys; ignore unknowns.
            normalized: Dict[str, str] = {}
            for key in ("id", "tag", "comment"):
                value = payload.get(key)
                if value is not None and str(value) != "":
                    normalized[key] = str(value)
            return normalized or None

        # Backward-compatible: client_extensions maps to trade-level extensions.
        trade_extensions = _normalize_extensions(trade_client_extensions or client_extensions)
        order_extensions = _normalize_extensions(order_client_extensions)

        instrument = ic.instruments_dict[pair]
        units = round(units, instrument.tradeUnitsPrecision)
        if direction == defs.SELL:
            units *= -1

        order: Dict[str, Any] = {
            "units": str(units),
            "instrument": pair,
            "type": "MARKET",
        }
        if trade_extensions:
            # Attach extensions to the resulting trade (not just the order).
            order["tradeClientExtensions"] = trade_extensions
        if order_extensions:
            order["clientExtensions"] = order_extensions
        if stop_loss:
            order["stopLossOnFill"] = {"price": str(round(stop_loss, instrument.displayPrecision))}
        if take_profit:
            order["takeProfitOnFill"] = {"price": str(round(take_profit, instrument.displayPrecision))}

        ok, resp = self._request(
            f"accounts/{defs.ACCOUNT_ID}/orders", verb="post", expected_code=201, data={"order": order}
        )
        if ok and "orderFillTransaction" in resp:
            return resp["orderFillTransaction"]["id"]
        # Log the actual error for debugging
        print(f"⚠️ OANDA API order placement error for {pair}: {resp}")
        return None

    def close_trade(self, trade_id: str) -> bool:
        ok, _ = self._request(f"accounts/{defs.ACCOUNT_ID}/trades/{trade_id}/close", verb="put")
        return ok

    def get_open_trade(self, trade_id: str) -> Optional[OpenTrade]:
        ok, data = self._request(f"accounts/{defs.ACCOUNT_ID}/trades/{trade_id}")
        return OpenTrade.from_api(data["trade"]) if ok else None

    def get_open_trades(self) -> Optional[List[OpenTrade]]:
        ok, data = self._request(f"accounts/{defs.ACCOUNT_ID}/openTrades")
        return [OpenTrade.from_api(t) for t in data["trades"]] if ok else None

    def get_trades(
        self,
        *,
        state: str = "OPEN",
        count: Optional[int] = None,
        instrument: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        params: Dict[str, Any] = {"state": state}
        if count is not None:
            params["count"] = int(count)
        if instrument:
            params["instrument"] = instrument
        ok, data = self._request(f"accounts/{defs.ACCOUNT_ID}/trades", params=params)
        return data.get("trades") if ok else None

    # ──────────────────────────────────────────────────────────
    # Pricing
    # ──────────────────────────────────────────────────────────
    def get_server_time(self) -> Optional[dt]:
        """
        Get OANDA's server time by making a lightweight pricing request.
        Uses EUR_USD as it's always available and liquid.

        Returns:
            Server time as datetime object, or None if request fails
        """
        params = {"instruments": "EUR_USD"}
        ok, data = self._request(f"accounts/{defs.ACCOUNT_ID}/pricing", params=params)
        if ok and "time" in data:
            return parser.parse(data["time"])
        return None

    def get_prices(self, instruments: List[str]) -> Optional[List[ApiPrice]]:
        """Get current prices with proper home currency conversions"""
        params = {"instruments": ",".join(instruments), "includeHomeConversions": True}
        ok, data = self._request(f"accounts/{defs.ACCOUNT_ID}/pricing", params=params)
        if ok:
            return [ApiPrice.from_api(p, data["homeConversions"]) for p in data["prices"]]
        # Log the actual error for debugging
        print(f"⚠️ OANDA API pricing error for {instruments}: {data}")
        return None

    # ──────────────────────────────────────────────────────────
    # Misc trade helpers (SL/TP patch / remove)
    # ──────────────────────────────────────────────────────────
    def _patch_trade(
        self, trade_id: str, payload: Dict[str, Any]
    ) -> bool:
        ok, _ = self._request(
            f"accounts/{defs.ACCOUNT_ID}/trades/{trade_id}/orders", verb="put", data=payload
        )
        return ok

    def update_trade_sl(self, trade_id: str, price: float | None) -> bool:
        payload = {"stopLoss": None} if price is None else {"stopLoss": {"price": str(price), "timeInForce": "GTC"}}
        return self._patch_trade(trade_id, payload)

    def update_trade_tp(self, trade_id: str, price: float | None) -> bool:
        payload = {"takeProfit": None} if price is None else {"takeProfit": {"price": str(price), "timeInForce": "GTC"}}
        return self._patch_trade(trade_id, payload)


# Singleton export (import‑safe)
oanda_api: OandaApi = OandaApi()

# ──────────────────────────────────────────────────────────────
