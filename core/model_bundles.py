from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional, Union


@dataclass(slots=True)
class DualModelBundle:
    type: Literal["dual"] = "dual"
    ev_models: dict[str, Any] | None = None
    pivot_model: Any = None
    ev_features: list[str] | None = None
    pivot_features: list[str] | None = None
    meta: dict[str, Any] | None = None

    def validate(self, *, pair: str, path: str) -> list[str]:
        warnings: list[str] = []
        ev_models = self.ev_models or {}
        if not ev_models.get("long") or not ev_models.get("short"):
            warnings.append(f"{pair}: missing EV long/short models in {path}")
        if not self.pivot_model:
            warnings.append(f"{pair}: missing pivot model in {path}")
        if not (self.ev_features or []):
            warnings.append(f"{pair}: missing ev_features in {path}")
        if not (self.pivot_features or []):
            warnings.append(f"{pair}: missing pivot_features in {path}")
        return warnings


@dataclass(slots=True)
class PivotModelBundle:
    type: Literal["pivot"] = "pivot"
    pivot_model: Any = None
    pivot_features: list[str] | None = None
    meta: dict[str, Any] | None = None

    def validate(self, *, pair: str, path: str) -> list[str]:
        warnings: list[str] = []
        if not self.pivot_model:
            warnings.append(f"{pair}: missing pivot model in {path}")
        if not self.pivot_features:
            warnings.append(f"{pair}: missing pivot_features in {path}")
        return warnings


@dataclass(slots=True)
class RuntimeModel:
    pair: str
    bundle: ModelBundle
    pivot_threshold: float
    ev_threshold: float

    @property
    def type(self) -> str:
        return self.bundle.type

    @property
    def pivot_model(self):
        return getattr(self.bundle, "pivot_model", None)

    @property
    def pivot_features(self) -> list[str]:
        return getattr(self.bundle, "pivot_features", []) or []

    @property
    def ev_models(self) -> Optional[dict[str, Any]]:
        return getattr(self.bundle, "ev_models", None)

    @property
    def ev_features(self) -> list[str]:
        return getattr(self.bundle, "ev_features", []) or []

    @property
    def meta(self) -> dict[str, Any]:
        return getattr(self.bundle, "meta", {}) or {}


ModelBundle = Union[DualModelBundle, PivotModelBundle]
