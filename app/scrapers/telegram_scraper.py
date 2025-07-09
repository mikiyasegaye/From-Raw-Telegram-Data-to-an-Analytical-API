"""
Telegram Data Scraper for Ethiopian Medical Channels

This module handles the extraction of data from public Telegram channels
relevant to Ethiopian medical businesses, including messages and media files.
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any
import pandas as pd
from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, ChatAdminRequiredError
from telethon.tl.types import Message, Channel, Chat
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class TelegramScraper:
    """
    Telegram scraper for extracting data from medical channels.
    """

    def __init__(self):
        """Initialize the Telegram scraper."""
        self.client = None
        self.channels = settings.TELEGRAM_CHANNELS
        self.raw_data_dir = Path(settings.RAW_DATA_DIR)
        self.images_dir = Path(settings.IMAGES_DIR)
        self.logs_dir = Path(settings.LOGS_DIR)

        # Create necessary directories
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Scraping statistics
        self.stats = {
            'channels_scraped': 0,
            'messages_scraped': 0,
            'images_downloaded': 0,
            'errors': 0
        }

    async def initialize_client(self) -> bool:
        """Initialize the Telegram client."""
        try:
            if not all([settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH, settings.TELEGRAM_PHONE]):
                logger.error("Missing required Telegram API credentials")
                return False

            self.client = TelegramClient(
                settings.TELEGRAM_SESSION_NAME,
                int(settings.TELEGRAM_API_ID),
                settings.TELEGRAM_API_HASH
            )

            await self.client.start(phone=settings.TELEGRAM_PHONE)
            logger.info("Telegram client initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Telegram client: {e}")
            return False

    def get_partition_path(self, date: datetime, channel_name: str) -> Path:
        """Get the partitioned directory path for storing data."""
        date_str = date.strftime("%Y-%m-%d")
        return self.raw_data_dir / date_str / f"{channel_name}.json"

    def get_image_path(self, channel_name: str, message_id: int, file_extension: str = "jpg") -> Path:
        """Get the path for storing downloaded images."""
        return self.images_dir / f"{channel_name}_{message_id}.{file_extension}"

    async def download_media(self, message: Message, channel_name: str) -> Optional[str]:
        """Download media from a message and return the file path."""
        try:
            if not message.media:
                return None

            # Get file extension
            if hasattr(message.media, 'photo'):
                file_extension = "jpg"
            elif hasattr(message.media, 'document'):
                file_extension = message.media.document.mime_type.split(
                    '/')[-1]
            else:
                return None

            # Create file path
            file_path = self.get_image_path(
                channel_name, message.id, file_extension)

            # Download the media
            await self.client.download_media(message.media, str(file_path))

            logger.info(f"Downloaded media: {file_path}")
            self.stats['images_downloaded'] += 1

            return str(file_path)

        except Exception as e:
            logger.error(
                f"Failed to download media from message {message.id}: {e}")
            return None

    def serialize_message(self, message: Message, channel_info: Dict, media_path: Optional[str] = None) -> Dict[str, Any]:
        """Serialize a Telegram message to a dictionary."""
        return {
            'message_id': message.id,
            'channel_id': channel_info.get('id'),
            'channel_name': channel_info.get('username'),
            'channel_title': channel_info.get('title'),
            'sender_id': message.sender_id if message.sender_id else None,
            'sender_name': None,  # Will be populated if sender info is available
            'message_text': message.text if message.text else '',
            'message_date': message.date.isoformat() if message.date else None,
            'has_media': message.media is not None,
            'media_type': self._get_media_type(message.media) if message.media else None,
            'media_url': None,  # Could be populated with direct URLs if available
            'file_path': media_path,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

    def _get_media_type(self, media) -> Optional[str]:
        """Get the media type from a Telegram media object."""
        if hasattr(media, 'photo'):
            return 'photo'
        elif hasattr(media, 'document'):
            return 'document'
        elif hasattr(media, 'video'):
            return 'video'
        elif hasattr(media, 'audio'):
            return 'audio'
        else:
            return 'unknown'

    async def scrape_channel(self, channel_username: str, limit: int = 1000) -> bool:
        """
        Scrape messages from a specific channel.

        Args:
            channel_username: The username of the channel to scrape
            limit: Maximum number of messages to scrape

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Starting to scrape channel: {channel_username}")

            # Get channel entity
            entity = await self.client.get_entity(channel_username)
            channel_info = {
                'id': entity.id,
                'username': getattr(entity, 'username', None),
                'title': getattr(entity, 'title', None)
            }

            # Create partition directory
            today = datetime.now()
            partition_path = self.get_partition_path(today, channel_username)
            partition_path.parent.mkdir(parents=True, exist_ok=True)

            # Initialize messages list
            messages = []

            # Scrape messages
            async for message in self.client.iter_messages(entity, limit=limit):
                try:
                    # Download media if present
                    media_path = None
                    if message.media:
                        media_path = await self.download_media(message, channel_username)

                    # Serialize message
                    message_data = self.serialize_message(
                        message, channel_info, media_path)
                    messages.append(message_data)

                    self.stats['messages_scraped'] += 1

                except Exception as e:
                    logger.error(f"Error processing message {message.id}: {e}")
                    self.stats['errors'] += 1
                    continue

            # Save messages to JSON file
            if messages:
                with open(partition_path, 'w', encoding='utf-8') as f:
                    json.dump(messages, f, ensure_ascii=False, indent=2)

                logger.info(
                    f"Saved {len(messages)} messages to {partition_path}")

            self.stats['channels_scraped'] += 1
            logger.info(f"Successfully scraped channel: {channel_username}")
            return True

        except FloodWaitError as e:
            logger.warning(f"Rate limited for {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return False
        except ChannelPrivateError:
            logger.error(
                f"Channel {channel_username} is private or not accessible")
            return False
        except ChatAdminRequiredError:
            logger.error(
                f"Admin rights required for channel {channel_username}")
            return False
        except Exception as e:
            logger.error(f"Error scraping channel {channel_username}: {e}")
            self.stats['errors'] += 1
            return False

    async def scrape_all_channels(self, limit_per_channel: int = 1000) -> Dict[str, Any]:
        """
        Scrape all configured channels.

        Args:
            limit_per_channel: Maximum number of messages per channel

        Returns:
            Dict containing scraping statistics
        """
        logger.info(f"Starting to scrape {len(self.channels)} channels")

        # Initialize client
        if not await self.initialize_client():
            return self.stats

        try:
            # Scrape each channel
            for channel in self.channels:
                logger.info(f"Processing channel: {channel}")
                await self.scrape_channel(channel, limit_per_channel)

                # Add delay between channels to avoid rate limiting
                await asyncio.sleep(2)

            logger.info("Scraping completed successfully")

        except Exception as e:
            logger.error(f"Error during scraping: {e}")
        finally:
            if self.client:
                await self.client.disconnect()

        return self.stats

    def log_scraping_stats(self):
        """Log the final scraping statistics."""
        logger.info("=" * 50)
        logger.info("SCRAPING STATISTICS")
        logger.info("=" * 50)
        logger.info(f"Channels scraped: {self.stats['channels_scraped']}")
        logger.info(f"Messages scraped: {self.stats['messages_scraped']}")
        logger.info(f"Images downloaded: {self.stats['images_downloaded']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 50)


async def main():
    """Main function to run the scraper."""
    scraper = TelegramScraper()
    stats = await scraper.scrape_all_channels()
    scraper.log_scraping_stats()
    return stats


if __name__ == "__main__":
    asyncio.run(main())
