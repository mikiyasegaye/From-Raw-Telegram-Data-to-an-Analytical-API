"""
Command-line interface for running the Telegram scraper.

This script provides a convenient way to run the scraper with various options
and configurations for different use cases.
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from app.scrapers.telegram_scraper import TelegramScraper
from app.scrapers.data_loader import DataLoader
from app.utils.logger import get_logger

logger = get_logger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Telegram Data Scraper for Ethiopian Medical Channels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all channels with default settings
  python run_scraper.py

  # Scrape specific number of messages per channel
  python run_scraper.py --limit 500

  # Scrape specific channels only
  python run_scraper.py --channels lobelia4cosmetics tikvahpharma

  # Load data into database after scraping
  python run_scraper.py --load-data

  # Scrape and load data for specific date
  python run_scraper.py --load-data --date 2024-01-15
        """
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=1000,
        help='Maximum number of messages to scrape per channel (default: 1000)'
    )

    parser.add_argument(
        '--channels',
        nargs='+',
        help='Specific channels to scrape (default: all configured channels)'
    )

    parser.add_argument(
        '--load-data',
        action='store_true',
        help='Load scraped data into database after scraping'
    )

    parser.add_argument(
        '--date',
        type=str,
        help='Date filter for loading data (YYYY-MM-DD format)'
    )

    parser.add_argument(
        '--scrape-only',
        action='store_true',
        help='Only scrape data, do not load into database'
    )

    parser.add_argument(
        '--load-only',
        action='store_true',
        help='Only load existing data into database, do not scrape'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform a dry run without actually scraping or loading'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    return parser.parse_args()


def validate_date(date_str: str) -> bool:
    """Validate date string format."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def update_channel_config(channels: list):
    """Update the channel configuration with provided channels."""
    if channels:
        from app.core.config import settings
        settings.TELEGRAM_CHANNELS = channels
        logger.info(f"Updated channels to scrape: {channels}")


async def run_scraping(limit: int, dry_run: bool = False):
    """Run the scraping process."""
    logger.info("Starting Telegram scraping process")

    if dry_run:
        logger.info("DRY RUN MODE - No actual scraping will be performed")
        return {'channels_scraped': 0, 'messages_scraped': 0, 'images_downloaded': 0, 'errors': 0}

    scraper = TelegramScraper()
    stats = await scraper.scrape_all_channels(limit)
    scraper.log_scraping_stats()
    return stats


def run_loading(date_filter: str = None, dry_run: bool = False):
    """Run the data loading process."""
    logger.info("Starting data loading process")

    if dry_run:
        logger.info("DRY RUN MODE - No actual loading will be performed")
        return {'files_processed': 0, 'messages_loaded': 0, 'errors': 0, 'duplicates_skipped': 0}

    loader = DataLoader()
    stats = loader.load_all_data(date_filter)
    loader.log_loading_stats()
    return stats


def check_prerequisites():
    """Check if all prerequisites are met."""
    logger.info("Checking prerequisites...")

    # Check if .env file exists
    env_file = Path('.env')
    if not env_file.exists():
        logger.error(
            "Environment file (.env) not found. Please create it from env.example")
        return False

    # Check if required directories exist
    required_dirs = ['data/raw/telegram_messages', 'data/images', 'logs']
    for dir_path in required_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    logger.info("Prerequisites check completed")
    return True


def main():
    """Main function."""
    args = parse_arguments()

    # Set up logging level
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("TELEGRAM DATA SCRAPER")
    logger.info("=" * 60)

    # Check prerequisites
    if not check_prerequisites():
        sys.exit(1)

    # Update channel configuration if specified
    if args.channels:
        update_channel_config(args.channels)

    # Validate date if provided
    if args.date and not validate_date(args.date):
        logger.error(
            f"Invalid date format: {args.date}. Use YYYY-MM-DD format")
        sys.exit(1)

    scraping_stats = None
    loading_stats = None

    try:
        # Run scraping if not load-only mode
        if not args.load_only:
            logger.info("Starting scraping phase...")
            scraping_stats = asyncio.run(
                run_scraping(args.limit, args.dry_run))

        # Run loading if not scrape-only mode
        if not args.scrape_only or args.load_data:
            logger.info("Starting loading phase...")
            loading_stats = run_loading(args.date, args.dry_run)

        # Log final summary
        logger.info("=" * 60)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 60)

        if scraping_stats:
            logger.info("Scraping Results:")
            logger.info(
                f"  Channels scraped: {scraping_stats.get('channels_scraped', 0)}")
            logger.info(
                f"  Messages scraped: {scraping_stats.get('messages_scraped', 0)}")
            logger.info(
                f"  Images downloaded: {scraping_stats.get('images_downloaded', 0)}")
            logger.info(f"  Errors: {scraping_stats.get('errors', 0)}")

        if loading_stats:
            logger.info("Loading Results:")
            logger.info(
                f"  Files processed: {loading_stats.get('files_processed', 0)}")
            logger.info(
                f"  Messages loaded: {loading_stats.get('messages_loaded', 0)}")
            logger.info(
                f"  Duplicates skipped: {loading_stats.get('duplicates_skipped', 0)}")
            logger.info(f"  Errors: {loading_stats.get('errors', 0)}")

        logger.info("=" * 60)
        logger.info("Process completed successfully!")

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Process failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
