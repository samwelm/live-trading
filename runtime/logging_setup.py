import logging

from infrastructure.log_wrapper import LogWrapper


def configure_logging(name: str = "live_trading") -> logging.Logger:
    """Configure shared logging handlers and return a module logger."""
    log_wrapper = LogWrapper(name, mode="a", console_output=True)

    root = logging.getLogger()
    root.handlers.clear()
    for handler in log_wrapper.logger.handlers:
        root.addHandler(handler)
    root.setLevel(logging.INFO)

    return logging.getLogger(__name__)
