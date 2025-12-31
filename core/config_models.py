from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Optional


@dataclass(slots=True)
class RiskConfig:
    type: str = "percent"
    value: float = 0.0
    comment: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RiskConfig":
        return cls(
            type=data.get("type", "percent"),
            value=data.get("value", 0.0),
            comment=data.get("comment"),
        )

    def to_dict(self) -> dict:
        payload = {"type": self.type, "value": self.value}
        if self.comment:
            payload["comment"] = self.comment
        return payload


@dataclass(slots=True)
class TradingSchedule:
    timezone: str = "America/Chicago"
    disabled_weekdays: list[str] = field(default_factory=list)
    comment: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TradingSchedule":
        return cls(
            timezone=data.get("timezone", "America/Chicago"),
            disabled_weekdays=data.get("disabled_weekdays", []) or [],
            comment=data.get("comment"),
        )

    def to_dict(self) -> dict:
        payload = {
            "timezone": self.timezone,
            "disabled_weekdays": list(self.disabled_weekdays),
        }
        if self.comment:
            payload["comment"] = self.comment
        return payload


@dataclass(slots=True)
class GlobalSettings:
    FIELD_NAMES: ClassVar[set[str]] = {
        "granularity",
        "default_brick_size",
        "default_start_date",
        "warmup_bricks",
        "pivot_lookback",
        "cache_interval_bricks",
        "log_brick_finalized",
        "max_concurrent_pairs",
        "signal_stale_minutes",
        "target_bricks",
        "stop_bricks",
        "strategy_id",
        "renko_mode",
        "feature_k_lags",
        "include_speed",
        "ev_threshold",
        "ev_rollover",
        "enable_pivot_signals",
        "risk",
        "trading_schedule",
        "poll_interval_seconds",
        "ev_startup_backfill",
    }

    granularity: Optional[str] = None
    default_brick_size: Optional[float] = None
    default_start_date: Optional[str] = None
    warmup_bricks: Optional[int] = None
    pivot_lookback: Optional[int] = None
    cache_interval_bricks: Optional[int] = None
    log_brick_finalized: Optional[bool] = None
    max_concurrent_pairs: Optional[int] = None
    signal_stale_minutes: Optional[int] = None
    target_bricks: Optional[int] = None
    stop_bricks: Optional[int] = None
    strategy_id: Optional[str] = None
    renko_mode: Optional[str] = None
    feature_k_lags: Optional[int] = None
    include_speed: Optional[bool] = None
    ev_threshold: Optional[float] = None
    ev_rollover: Optional[bool] = None
    enable_pivot_signals: Optional[bool] = None
    risk: Optional[RiskConfig] = None
    trading_schedule: Optional[TradingSchedule] = None
    poll_interval_seconds: Optional[int] = None
    ev_startup_backfill: Optional[int] = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GlobalSettings":
        kwargs = {name: data.get(name) for name in cls.FIELD_NAMES}
        risk = data.get("risk")
        if isinstance(risk, dict):
            kwargs["risk"] = RiskConfig.from_dict(risk)
        schedule = data.get("trading_schedule")
        if isinstance(schedule, dict):
            kwargs["trading_schedule"] = TradingSchedule.from_dict(schedule)
        extra = {k: v for k, v in data.items() if k not in cls.FIELD_NAMES}
        return cls(**kwargs, extra=extra)

    def get(self, key: str, default: Any = None) -> Any:
        if key in self.FIELD_NAMES:
            value = getattr(self, key)
            return default if value is None else value
        return self.extra.get(key, default)


@dataclass(slots=True)
class StreamConfig:
    global_settings: GlobalSettings
    pair_settings: dict[str, dict]
    disabled_pairs: Optional[dict] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StreamConfig":
        global_settings = GlobalSettings.from_dict(data.get("global_settings", {}))
        raw_pair_settings = data.get("pair_settings", {})
        pair_settings: dict[str, dict] = {}
        if isinstance(raw_pair_settings, dict):
            for pair, settings in raw_pair_settings.items():
                if isinstance(settings, dict):
                    converted = dict(settings)
                    risk = settings.get("risk")
                    if isinstance(risk, dict):
                        converted["risk"] = RiskConfig.from_dict(risk)
                    pair_settings[pair] = converted
                else:
                    pair_settings[pair] = settings
        else:
            pair_settings = raw_pair_settings
        disabled_pairs = data.get("disabled_pairs")
        return cls(
            global_settings=global_settings,
            pair_settings=pair_settings,
            disabled_pairs=disabled_pairs,
        )

    @classmethod
    def load(cls, path: str) -> "StreamConfig":
        with open(path) as handle:
            return cls.from_dict(json.load(handle))

    def normalize_pair_settings(self) -> list[str]:
        warnings: list[str] = []
        if not isinstance(self.pair_settings, dict):
            return warnings

        default_brick = self.global_settings.get("default_brick_size")
        default_start = self.global_settings.get("default_start_date")

        for pair, settings in self.pair_settings.items():
            if not isinstance(settings, dict):
                continue

            if settings.get("brick_size") is None:
                if default_brick is not None:
                    settings["brick_size"] = default_brick
                    warnings.append(
                        f"pair_settings.{pair}.brick_size defaulted to global_settings.default_brick_size"
                    )
                else:
                    warnings.append(
                        f"pair_settings.{pair}.brick_size missing and no global default"
                    )

            if settings.get("start_date") is None:
                if default_start is not None:
                    settings["start_date"] = default_start
                    warnings.append(
                        f"pair_settings.{pair}.start_date defaulted to global_settings.default_start_date"
                    )
                else:
                    warnings.append(
                        f"pair_settings.{pair}.start_date missing and no global default"
                    )

        return warnings

    def validate(self) -> tuple[list[str], list[str]]:
        warnings: list[str] = []
        errors: list[str] = []

        if not isinstance(self.pair_settings, dict):
            errors.append("pair_settings must be an object")
        elif not self.pair_settings:
            warnings.append("pair_settings is empty")
        else:
            for pair, settings in self.pair_settings.items():
                if not isinstance(settings, dict):
                    errors.append(f"pair_settings.{pair} must be an object")
                    continue
                if settings.get("brick_size") is None:
                    errors.append(f"pair_settings.{pair}.brick_size is missing")
                if settings.get("start_date") is None:
                    errors.append(f"pair_settings.{pair}.start_date is missing")
                granularity = settings.get("granularity")
                if granularity is not None and not isinstance(granularity, str):
                    errors.append(f"pair_settings.{pair}.granularity must be a string")
                pip_location = settings.get("pip_location")
                if pip_location is not None and not isinstance(pip_location, (int, float)):
                    errors.append(f"pair_settings.{pair}.pip_location must be a number")
                risk = settings.get("risk")
                if isinstance(risk, RiskConfig):
                    if risk.type not in {"percent", "fixed"}:
                        errors.append(
                            f"pair_settings.{pair}.risk.type must be percent or fixed (got {risk.type})"
                        )
                elif isinstance(risk, dict):
                    risk_type = risk.get("type")
                    if risk_type and risk_type not in {"percent", "fixed"}:
                        errors.append(
                            f"pair_settings.{pair}.risk.type must be percent or fixed (got {risk_type})"
                        )

        default_brick = self.global_settings.get("default_brick_size")
        if default_brick is None:
            warnings.append("global_settings.default_brick_size is missing")
        default_start = self.global_settings.get("default_start_date")
        if default_start is None:
            warnings.append("global_settings.default_start_date is missing")
        risk = self.global_settings.get("risk")
        if risk is None:
            warnings.append("global_settings.risk is missing (will use defaults)")
        elif isinstance(risk, RiskConfig):
            if risk.type not in {"percent", "fixed"}:
                errors.append(f"global_settings.risk.type must be percent or fixed (got {risk.type})")
        elif isinstance(risk, dict):
            risk_type = risk.get("type")
            if risk_type and risk_type not in {"percent", "fixed"}:
                errors.append(f"global_settings.risk.type must be percent or fixed (got {risk_type})")

        schedule = self.global_settings.get("trading_schedule")
        if schedule is not None and isinstance(schedule, TradingSchedule):
            if not schedule.timezone:
                warnings.append("global_settings.trading_schedule.timezone is empty")
        elif schedule is not None and isinstance(schedule, dict):
            if not schedule.get("timezone"):
                warnings.append("global_settings.trading_schedule.timezone is empty")

        return warnings, errors


@dataclass(slots=True)
class ModelRoute:
    pair: Optional[str] = None
    name: str = ""
    model_path: str = ""
    threshold: float = 0.6
    ev_threshold: Optional[float] = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelRoute":
        known = {"pair", "name", "model_path", "threshold", "ev_threshold"}
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(
            pair=data.get("pair"),
            name=data.get("name", ""),
            model_path=data.get("model_path", ""),
            threshold=data.get("threshold", 0.6),
            ev_threshold=data.get("ev_threshold"),
            extra=extra,
        )

    def validate(self, label: str) -> tuple[list[str], list[str]]:
        warnings: list[str] = []
        errors: list[str] = []

        if self.pair:
            if not isinstance(self.pair, str) or len(self.pair.split("_")) != 2:
                errors.append(f"{label}: pair must be like EUR_USD")
        if self.name and self.pair and not self.name.startswith(self.pair):
            warnings.append(f"{label}: name does not start with pair ({self.pair})")

        if self.threshold is not None and not (0.0 <= float(self.threshold) <= 1.0):
            errors.append(f"{label}: threshold must be between 0 and 1")
        if self.ev_threshold is not None and not (0.0 <= float(self.ev_threshold) <= 1.0):
            errors.append(f"{label}: ev_threshold must be between 0 and 1")

        if not isinstance(self.model_path, str):
            errors.append(f"{label}: model_path must be a string")

        return warnings, errors


@dataclass(slots=True)
class ModelRoutesConfig:
    routes: list[ModelRoute] = field(default_factory=list)
    description: Optional[str] = None
    total_risk_budget: Optional[str] = None
    disabled_pairs: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelRoutesConfig":
        known = {"routes", "description", "total_risk_budget", "disabled_pairs"}
        routes = [ModelRoute.from_dict(item) for item in data.get("routes", [])]
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(
            routes=routes,
            description=data.get("description"),
            total_risk_budget=data.get("total_risk_budget"),
            disabled_pairs=data.get("disabled_pairs", []) or [],
            extra=extra,
        )

    @classmethod
    def load(cls, path: str) -> "ModelRoutesConfig":
        with open(path) as handle:
            return cls.from_dict(json.load(handle))

    def validate(self) -> tuple[list[str], list[str]]:
        warnings: list[str] = []
        errors: list[str] = []

        for idx, route in enumerate(self.routes):
            label = route.pair or route.name or f"route[{idx}]"
            route_warnings, route_errors = route.validate(label)
            warnings.extend(route_warnings)
            errors.extend(route_errors)

            if not route.model_path:
                errors.append(f"{label}: model_path is missing")
                continue
            if not route.pair and "_" not in route.name:
                warnings.append(f"{label}: missing pair and name lacks pair prefix")
            if not Path(route.model_path).exists():
                warnings.append(f"{label}: model_path not found ({route.model_path})")

        return warnings, errors
