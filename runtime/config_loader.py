import logging

from core.config_models import ModelRoutesConfig, StreamConfig

logger = logging.getLogger(__name__)


def load_stream_config(config_path: str = "config/stream_config.json") -> StreamConfig:
    """Load and validate stream configuration."""
    config = StreamConfig.load(config_path)
    normalize_warnings = config.normalize_pair_settings()
    for warning in normalize_warnings:
        logger.warning("Stream config: %s", warning)
    warnings, errors = config.validate()
    for warning in warnings:
        logger.warning("Stream config: %s", warning)
    if errors:
        raise ValueError("Stream config errors: " + "; ".join(errors))
    return config


def load_model_routes(config_path: str = "config/model_routes.json") -> ModelRoutesConfig:
    """Load and validate model routing configuration."""
    try:
        model_config = ModelRoutesConfig.load(config_path)
        warnings, errors = model_config.validate()
        for warning in warnings:
            logger.warning("Model routes: %s", warning)
        if errors:
            raise ValueError("Model route errors: " + "; ".join(errors))
        return model_config
    except FileNotFoundError:
        logger.warning("Model config not found: %s, using defaults", config_path)
        return ModelRoutesConfig()
