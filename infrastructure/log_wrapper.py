# infrastructure/log_wrapper.py - Enhanced multi-file logging with category separation

import logging
import os
import time

LOG_FORMAT = "%(asctime)sZ %(levelname)s %(message)s"
DEFAULT_LEVEL = logging.INFO

class LogWrapper:

    PATH = './logs'

    # Shared handlers for multi-file logging
    _trading_handler = None
    _data_handler = None
    _error_handler = None
    _main_handler = None

    def __init__(self, name, mode="w", console_output=False, enable_multi_file=True):
        """
        Initialize log wrapper with multi-file support

        Args:
            name: Logger name
            mode: File mode ('w' for overwrite, 'a' for append)
            console_output: If True, also output to console
            enable_multi_file: If True, route logs to specialized files
        """
        self.create_directory()
        self.filename = f"{LogWrapper.PATH}/{name}.log"
        self.logger = logging.getLogger(name)
        self.logger.setLevel(DEFAULT_LEVEL)
        self.enable_multi_file = enable_multi_file

        # Clear any existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Primary file handler (named log file)
        file_handler = logging.FileHandler(self.filename, mode=mode)
        formatter = logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
        formatter.converter = time.gmtime
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Add multi-file handlers if enabled
        if enable_multi_file:
            self._setup_multi_file_handlers(mode)

        # Optional console handler for important logs
        if console_output or name in ['main', 'error']:
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter('%(levelname)s: %(message)s')
            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(logging.WARNING)  # Only show warnings and errors on console
            self.logger.addHandler(console_handler)

        self.logger.info(f"LogWrapper init() {self.filename}")

    def _setup_multi_file_handlers(self, mode):
        """Setup shared handlers for multi-file logging (class-level)"""
        formatter = logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
        formatter.converter = time.gmtime

        # Initialize shared handlers once
        if LogWrapper._trading_handler is None:
            LogWrapper._trading_handler = logging.FileHandler(f"{LogWrapper.PATH}/trading_events.log", mode=mode)
            LogWrapper._trading_handler.setFormatter(formatter)
            LogWrapper._trading_handler.addFilter(TradingEventFilter())

        if LogWrapper._data_handler is None:
            LogWrapper._data_handler = logging.FileHandler(f"{LogWrapper.PATH}/data_operations.log", mode=mode)
            LogWrapper._data_handler.setFormatter(formatter)
            LogWrapper._data_handler.addFilter(DataOperationFilter())

        if LogWrapper._error_handler is None:
            LogWrapper._error_handler = logging.FileHandler(f"{LogWrapper.PATH}/errors.log", mode=mode)
            LogWrapper._error_handler.setFormatter(formatter)
            LogWrapper._error_handler.setLevel(logging.WARNING)

        if LogWrapper._main_handler is None:
            LogWrapper._main_handler = logging.FileHandler(f"{LogWrapper.PATH}/main.log", mode=mode)
            LogWrapper._main_handler.setFormatter(formatter)

        # Add all handlers to this logger
        self.logger.addHandler(LogWrapper._trading_handler)
        self.logger.addHandler(LogWrapper._data_handler)
        self.logger.addHandler(LogWrapper._error_handler)
        self.logger.addHandler(LogWrapper._main_handler)

    def create_directory(self):
        if not os.path.exists(LogWrapper.PATH):
            os.makedirs(LogWrapper.PATH)
            print(f"📁 Created logs directory: {LogWrapper.PATH}")

    def debug(self, message):
        """Debug level logging"""
        self.logger.debug(message)

    def info(self, message):
        """Info level logging"""
        self.logger.info(message)

    def warning(self, message):
        """Warning level logging"""
        self.logger.warning(message)

    def error(self, message):
        """Error level logging"""
        self.logger.error(message)

    def set_level(self, level):
        """Change the logging level"""
        self.logger.setLevel(level)

    def set_debug_level(self):
        """Enable debug level logging"""
        self.set_level(logging.DEBUG)

    def set_info_level(self):
        """Set to info level logging"""
        self.set_level(logging.INFO)


# Log Filters for routing to specialized files
class TradingEventFilter(logging.Filter):
    """Filter for trading events: bricks, ML scores, signals, positions"""
    def filter(self, record):
        keywords = ['brick', 'pivot', 'score', 'signal', 'trade', 'position', 'entry', 'exit', 'RENKO', '🎯', '✅', '🛑']
        msg = record.getMessage().lower()
        return any(kw.lower() in msg for kw in keywords)


class DataOperationFilter(logging.Filter):
    """Filter for data operations: S5 fetch, persistence, candle updates"""
    def filter(self, record):
        keywords = ['fetch', 's5', 'candle', 'activity', 'warmup', 'persistence', 'dataframe', 'update']
        msg = record.getMessage().lower()
        # Exclude trading events to avoid duplication
        trading_keywords = ['brick', 'pivot', 'score', 'signal', 'trade', 'position', 'entry', 'exit']
        has_data_keyword = any(kw.lower() in msg for kw in keywords)
        has_trading_keyword = any(kw.lower() in msg for kw in trading_keywords)
        return has_data_keyword and not has_trading_keyword
