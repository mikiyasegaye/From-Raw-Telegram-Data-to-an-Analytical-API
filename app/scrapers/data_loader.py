"""
Data Loader for Raw Telegram Data

This module handles loading raw JSON data from the data lake into the PostgreSQL database.
It processes the partitioned JSON files and inserts them into the raw.telegram_messages table.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, IntegrityError
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DataLoader:
    """
    Data loader for raw Telegram data.
    """

    def __init__(self):
        """Initialize the data loader."""
        self.engine = None
        self.raw_data_dir = Path(settings.RAW_DATA_DIR)
        self.stats = {
            'files_processed': 0,
            'messages_loaded': 0,
            'errors': 0,
            'duplicates_skipped': 0
        }

    def create_database_engine(self):
        """Create SQLAlchemy engine for database connection."""
        try:
            self.engine = create_engine(
                settings.DATABASE_URL,
                echo=settings.DEBUG,
                pool_pre_ping=True,
                pool_recycle=300
            )
            logger.info("Database engine created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            return False

    def test_connection(self) -> bool:
        """Test database connection."""
        if not self.engine:
            return False

        try:
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                logger.info("Database connection successful")
                return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def find_json_files(self, date_filter: Optional[str] = None) -> List[Path]:
        """
        Find all JSON files in the raw data directory.

        Args:
            date_filter: Optional date filter (YYYY-MM-DD format)

        Returns:
            List of JSON file paths
        """
        json_files = []

        if date_filter:
            # Look in specific date directory
            date_dir = self.raw_data_dir / date_filter
            if date_dir.exists():
                json_files.extend(date_dir.glob("*.json"))
        else:
            # Look in all date directories
            for date_dir in self.raw_data_dir.iterdir():
                if date_dir.is_dir():
                    json_files.extend(date_dir.glob("*.json"))

        logger.info(f"Found {len(json_files)} JSON files to process")
        return json_files

    def load_json_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Load and parse a JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            List of message dictionaries
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if isinstance(data, list):
                return data
            else:
                logger.warning(f"Unexpected JSON structure in {file_path}")
                return []

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON file {file_path}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return []

    def prepare_message_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare message data for database insertion.

        Args:
            message: Raw message dictionary from JSON

        Returns:
            Prepared message dictionary for database
        """
        # Extract date from the partition path or message
        message_date = None
        if message.get('message_date'):
            try:
                message_date = datetime.fromisoformat(
                    message['message_date'].replace('Z', '+00:00'))
            except:
                message_date = datetime.now()
        else:
            message_date = datetime.now()

        return {
            'message_id': message.get('message_id'),
            'channel_id': message.get('channel_id'),
            'channel_name': message.get('channel_name'),
            'sender_id': message.get('sender_id'),
            'sender_name': message.get('sender_name'),
            'message_text': message.get('message_text', ''),
            'message_date': message_date,
            'has_media': message.get('has_media', False),
            'media_type': message.get('media_type'),
            'media_url': message.get('media_url'),
            'file_path': message.get('file_path'),
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }

    def insert_messages_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """
        Insert a batch of messages into the database.

        Args:
            messages: List of message dictionaries

        Returns:
            bool: True if successful, False otherwise
        """
        if not messages:
            return True

        try:
            with self.engine.connect() as connection:
                # Prepare the insert statement
                insert_stmt = text("""
                    INSERT INTO raw.telegram_messages (
                        message_id, channel_id, channel_name, sender_id, sender_name,
                        message_text, message_date, has_media, media_type, media_url, file_path,
                        created_at, updated_at
                    ) VALUES (
                        :message_id, :channel_id, :channel_name, :sender_id, :sender_name,
                        :message_text, :message_date, :has_media, :media_type, :media_url, :file_path,
                        :created_at, :updated_at
                    ) ON CONFLICT (message_id, channel_id) DO NOTHING
                """)

                # Execute batch insert
                result = connection.execute(insert_stmt, messages)
                connection.commit()

                self.stats['messages_loaded'] += len(messages)
                logger.info(f"Inserted {len(messages)} messages into database")
                return True

        except IntegrityError as e:
            # Handle duplicate key errors
            logger.warning(f"Duplicate messages detected: {e}")
            self.stats['duplicates_skipped'] += len(messages)
            return True
        except Exception as e:
            logger.error(f"Failed to insert messages: {e}")
            self.stats['errors'] += 1
            return False

    def process_file(self, file_path: Path) -> bool:
        """
        Process a single JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Processing file: {file_path}")

            # Load JSON data
            messages = self.load_json_file(file_path)
            if not messages:
                logger.warning(f"No messages found in {file_path}")
                return True

            # Prepare messages for database
            prepared_messages = []
            for message in messages:
                prepared_message = self.prepare_message_data(message)
                prepared_messages.append(prepared_message)

            # Insert into database
            success = self.insert_messages_batch(prepared_messages)

            if success:
                self.stats['files_processed'] += 1
                logger.info(f"Successfully processed {file_path}")

            return success

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            self.stats['errors'] += 1
            return False

    def load_all_data(self, date_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Load all raw data from the data lake into the database.

        Args:
            date_filter: Optional date filter (YYYY-MM-DD format)

        Returns:
            Dict containing loading statistics
        """
        logger.info("Starting data loading process")

        # Initialize database connection
        if not self.create_database_engine():
            return self.stats

        if not self.test_connection():
            return self.stats

        try:
            # Find JSON files
            json_files = self.find_json_files(date_filter)

            if not json_files:
                logger.warning("No JSON files found to process")
                return self.stats

            # Process each file
            for file_path in json_files:
                self.process_file(file_path)

            logger.info("Data loading completed successfully")

        except Exception as e:
            logger.error(f"Error during data loading: {e}")
        finally:
            if self.engine:
                self.engine.dispose()

        return self.stats

    def log_loading_stats(self):
        """Log the final loading statistics."""
        logger.info("=" * 50)
        logger.info("DATA LOADING STATISTICS")
        logger.info("=" * 50)
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Messages loaded: {self.stats['messages_loaded']}")
        logger.info(f"Duplicates skipped: {self.stats['duplicates_skipped']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 50)


def main():
    """Main function to run the data loader."""
    loader = DataLoader()
    stats = loader.load_all_data()
    loader.log_loading_stats()
    return stats


if __name__ == "__main__":
    main()
