"""
Test script for the Telegram scraper.

This script tests the scraper functionality without actually scraping data.
It verifies configuration, connections, and basic functionality.
"""

import asyncio
import sys
from pathlib import Path
from app.scrapers.telegram_scraper import TelegramScraper
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def test_configuration():
    """Test if the configuration is properly set up."""
    logger.info("Testing configuration...")

    # Check required environment variables
    required_vars = ['TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TELEGRAM_PHONE']
    missing_vars = []

    for var in required_vars:
        if not getattr(settings, var):
            missing_vars.append(var)

    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return False

    logger.info("Configuration test passed")
    return True


async def test_telegram_connection():
    """Test Telegram API connection."""
    logger.info("Testing Telegram API connection...")

    try:
        scraper = TelegramScraper()
        success = await scraper.initialize_client()

        if success:
            logger.info("Telegram API connection test passed")
            await scraper.client.disconnect()
            return True
        else:
            logger.error("Telegram API connection test failed")
            return False

    except Exception as e:
        logger.error(f"Telegram API connection test failed: {e}")
        return False


def test_directory_structure():
    """Test if the required directory structure exists."""
    logger.info("Testing directory structure...")

    required_dirs = [
        settings.RAW_DATA_DIR,
        settings.IMAGES_DIR,
        settings.LOGS_DIR
    ]

    for dir_path in required_dirs:
        path = Path(dir_path)
        if not path.exists():
            logger.warning(f"Creating directory: {path}")
            path.mkdir(parents=True, exist_ok=True)
        else:
            logger.info(f"Directory exists: {path}")

    logger.info("Directory structure test passed")
    return True


def test_channel_configuration():
    """Test channel configuration."""
    logger.info("Testing channel configuration...")

    if not settings.TELEGRAM_CHANNELS:
        logger.error("No channels configured")
        return False

    logger.info(f"Configured channels: {settings.TELEGRAM_CHANNELS}")
    logger.info("Channel configuration test passed")
    return True


async def run_all_tests():
    """Run all tests."""
    logger.info("=" * 50)
    logger.info("RUNNING SCRAPER TESTS")
    logger.info("=" * 50)

    tests = [
        ("Configuration", test_configuration),
        ("Directory Structure", lambda: test_directory_structure()),
        ("Channel Configuration", lambda: test_channel_configuration()),
        ("Telegram Connection", test_telegram_connection),
    ]

    results = []

    for test_name, test_func in tests:
        logger.info(f"\nRunning {test_name} test...")
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results.append((test_name, result))

            if result:
                logger.info(f"✅ {test_name} test passed")
            else:
                logger.error(f"❌ {test_name} test failed")

        except Exception as e:
            logger.error(f"❌ {test_name} test failed with exception: {e}")
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("TEST SUMMARY")
    logger.info("=" * 50)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        logger.info(f"{test_name}: {status}")

    logger.info(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All tests passed! The scraper is ready to use.")
        return True
    else:
        logger.error(
            "❌ Some tests failed. Please fix the issues before running the scraper.")
        return False


def main():
    """Main function."""
    try:
        success = asyncio.run(run_all_tests())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Tests failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
