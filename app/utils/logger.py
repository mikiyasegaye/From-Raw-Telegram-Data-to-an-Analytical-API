"""
Logging utility for the Telegram Data Pipeline.

Provides centralized logging configuration with file and console outputs,
different log levels for different components, and structured logging.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from app.core.config import settings


def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: The name of the logger (usually __name__)
        log_file: Optional specific log file path

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    # Set log level
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_path = Path(settings.LOGS_DIR) / log_file
    else:
        # Default log file based on component
        if 'scraper' in name.lower():
            log_file = 'scraping.log'
        elif 'api' in name.lower():
            log_file = 'api.log'
        elif 'database' in name.lower():
            log_file = 'database.log'
        else:
            log_file = 'app.log'

        log_path = Path(settings.LOGS_DIR) / log_file

    # Ensure log directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Error file handler for critical errors
    error_log_path = Path(settings.LOGS_DIR) / 'errors.log'
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_path,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=3,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    return logger


def log_scraping_progress(channel: str, message_count: int, total_channels: int, current_channel: int):
    """
    Log scraping progress information.

    Args:
        channel: Channel being scraped
        message_count: Number of messages scraped from current channel
        total_channels: Total number of channels to scrape
        current_channel: Current channel index (1-based)
    """
    logger = get_logger(__name__)
    progress = (current_channel / total_channels) * 100
    logger.info(
        f"Progress: {progress:.1f}% - Channel {current_channel}/{total_channels}: {channel} ({message_count} messages)")


def log_rate_limit_warning(channel: str, wait_time: int):
    """
    Log rate limiting warnings.

    Args:
        channel: Channel that triggered rate limiting
        wait_time: Time to wait in seconds
    """
    logger = get_logger(__name__)
    logger.warning(
        f"Rate limited for channel {channel}. Waiting {wait_time} seconds...")


def log_channel_error(channel: str, error: str, error_type: str = "general"):
    """
    Log channel-specific errors.

    Args:
        channel: Channel that encountered an error
        error: Error message
        error_type: Type of error (rate_limit, private, admin_required, etc.)
    """
    logger = get_logger(__name__)
    logger.error(f"Error scraping channel {channel} ({error_type}): {error}")


def log_scraping_summary(stats: dict):
    """
    Log scraping summary statistics.

    Args:
        stats: Dictionary containing scraping statistics
    """
    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info("SCRAPING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Channels scraped: {stats.get('channels_scraped', 0)}")
    logger.info(f"Messages scraped: {stats.get('messages_scraped', 0)}")
    logger.info(f"Images downloaded: {stats.get('images_downloaded', 0)}")
    logger.info(f"Errors encountered: {stats.get('errors', 0)}")
    logger.info("=" * 60)


def log_data_lake_storage(file_path: str, message_count: int):
    """
    Log data lake storage operations.

    Args:
        file_path: Path where data was stored
        message_count: Number of messages stored
    """
    logger = get_logger(__name__)
    logger.info(f"Stored {message_count} messages in data lake: {file_path}")


def log_media_download(channel: str, message_id: int, file_path: str):
    """
    Log media download operations.

    Args:
        channel: Channel name
        message_id: Message ID
        file_path: Path where media was saved
    """
    logger = get_logger(__name__)
    logger.info(
        f"Downloaded media from {channel} message {message_id}: {file_path}")


def log_partition_creation(partition_path: str):
    """
    Log partition directory creation.

    Args:
        partition_path: Path of the created partition
    """
    logger = get_logger(__name__)
    logger.info(f"Created partition directory: {partition_path}")


# Convenience functions for different components
def get_scraper_logger():
    """Get logger specifically for scraping operations."""
    return get_logger('telegram_scraper', 'scraping.log')


def get_api_logger():
    """Get logger specifically for API operations."""
    return get_logger('api', 'api.log')


def get_database_logger():
    """Get logger specifically for database operations."""
    return get_logger('database', 'database.log')


def get_general_logger():
    """Get general application logger."""
    return get_logger('app', 'app.log')
