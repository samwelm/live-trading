import logging
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from core.model_bundles import (
    DualModelBundle,
    ModelBundle,
    PivotModelBundle,
    RuntimeModel,
)
from prob_signals.renko_ml_features import build_enhanced_features

logger = logging.getLogger(__name__)


def load_model_bundle(model_path: Path) -> ModelBundle:
    """Load a model bundle from disk."""
    with open(model_path, "rb") as handle:
        bundle = pickle.load(handle)

    if isinstance(bundle, dict) and "every_brick" in bundle and "pivot" in bundle:
        meta = bundle.get("meta", {})
        return DualModelBundle(
            ev_models={
                "long": bundle["every_brick"]["long"],
                "short": bundle["every_brick"]["short"],
            },
            pivot_model=bundle["pivot"],
            ev_features=meta.get("every_brick", {}).get("feature_columns", []),
            pivot_features=meta.get("pivot", {}).get("feature_columns", []),
            meta=meta,
        )

    if isinstance(bundle, dict) and "model" in bundle and "features" in bundle:
        return PivotModelBundle(
            pivot_model=bundle["model"],
            pivot_features=bundle.get("features", []),
            meta=bundle.get("meta", {}),
        )

    raise ValueError(f"Unsupported model bundle format: {model_path}")


def validate_model_bundle(bundle: ModelBundle, *, path: Path, pair: str) -> list[str]:
    if isinstance(bundle, DualModelBundle):
        return bundle.validate(pair=pair, path=path.name)
    if isinstance(bundle, PivotModelBundle):
        return bundle.validate(pair=pair, path=path.name)
    return [f"{pair}: unknown bundle type in {path.name}"]


def build_feature_template(
    *,
    k_lags: int,
    pivot_lookback: int,
    include_speed: bool,
) -> set[str]:
    rows = max(60, k_lags + 5)
    times = pd.date_range("2025-01-01", periods=rows, freq="h", tz="UTC")
    base = 1.1 + (np.arange(rows) * 0.0001)
    direction = np.where(np.arange(rows) % 2 == 0, 1, -1)
    df = pd.DataFrame(
        {
            "time": times,
            "mid_o": base - 0.00005,
            "mid_h": base + 0.0001,
            "mid_l": base - 0.0001,
            "mid_c": base,
            "direction": direction,
            "brick_duration": np.full(rows, 60),
            "brick_size": np.full(rows, 0.001),
        }
    )
    features, _ = build_enhanced_features(
        df,
        k_lags=k_lags,
        pivot_lookback=pivot_lookback,
        include_speed=include_speed,
        include_pivots=True,
    )
    return set(features.columns)


def check_feature_parity(
    *,
    models: dict[str, RuntimeModel],
    k_lags: int,
    pivot_lookback: int,
    include_speed: bool,
) -> None:
    try:
        live_features = build_feature_template(
            k_lags=k_lags,
            pivot_lookback=pivot_lookback,
            include_speed=include_speed,
        )
    except Exception as exc:
        logger.warning("Feature parity check skipped: %s", exc)
        return

    for pair, model in models.items():
        pivot_missing = set(model.pivot_features or []) - live_features
        if pivot_missing:
            logger.warning(
                "%s: pivot model uses %s unknown features: %s",
                pair,
                len(pivot_missing),
                ", ".join(sorted(pivot_missing)),
            )
        ev_features = model.ev_features or []
        if ev_features:
            ev_missing = set(ev_features) - live_features
            if ev_missing:
                logger.warning(
                    "%s: EV model uses %s unknown features: %s",
                    pair,
                    len(ev_missing),
                    ", ".join(sorted(ev_missing)),
                )


def load_models_for_pairs(
    *,
    model_config,
    pairs: list[str],
    ev_threshold_default: float,
) -> dict[str, RuntimeModel]:
    models: dict[str, RuntimeModel] = {}
    for route in model_config.routes:
        pair = route.pair
        if not pair:
            name_parts = route.name.split("_")
            if len(name_parts) < 2:
                logger.warning("Skipping route with invalid name format: %s", route.name)
                continue
            pair = f"{name_parts[0]}_{name_parts[1]}"

        if pair in pairs:
            model_path = Path(route.model_path)
            pivot_threshold = route.threshold if route.threshold is not None else 0.6
            ev_threshold = (
                route.ev_threshold if route.ev_threshold is not None else ev_threshold_default
            )

            if model_path.exists():
                try:
                    bundle = load_model_bundle(model_path)
                    bundle_warnings = validate_model_bundle(bundle, path=model_path, pair=pair)
                    for warning in bundle_warnings:
                        logger.warning(warning)
                    models[pair] = RuntimeModel(
                        pair=pair,
                        bundle=bundle,
                        ev_threshold=ev_threshold,
                        pivot_threshold=pivot_threshold,
                    )
                    if isinstance(bundle, DualModelBundle):
                        print(f"OK: Loaded dual model for {pair}: {model_path.name}")
                    else:
                        print(f"OK: Loaded pivot model for {pair}: {model_path.name}")
                except Exception as exc:
                    logger.error("Failed to load model for %s: %s", pair, exc)
            else:
                logger.warning("Model file not found for %s: %s", pair, model_path)
    return models
