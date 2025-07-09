#!/usr/bin/env python3
"""
Script to load raw JSON data from the data lake into PostgreSQL raw schema.
This script reads JSON files from the data lake and loads them into the database.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RawDataLoader:
    def __init__(self):
        self.db_url = (
            f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'password')}@"
            f"{os.getenv('DB_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{os.getenv('POSTGRES_DB', 'telegram_data')}"
        )
        self.engine = create_engine(self.db_url)
        self.data_lake_path = Path("data/raw")

    def create_raw_schema(self):
        """Create the raw schema if it doesn't exist."""
        with self.engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            conn.commit()
            logger.info("Raw schema created/verified")

    def load_json_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load and parse a JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                else:
                    return [data]
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return []

    def extract_channel_from_path(self, file_path: Path) -> str:
        """Extract channel name from file path."""
        # Path format: data/raw/YYYY/MM/DD/channel_name/messages.json
        parts = file_path.parts
        if len(parts) >= 6:
            return parts[-2]  # channel_name
        return None

    def extract_date_from_path(self, file_path: Path) -> str:
        """Extract date from file path."""
        # Path format: data/raw/YYYY/MM/DD/channel_name/messages.json
        parts = file_path.parts
        if len(parts) >= 6:
            year, month, day = parts[-4], parts[-3], parts[-2]
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        return None

    def process_messages(self, messages: List[Dict], channel: str, date: str) -> List[Dict]:
        """Process messages and add metadata."""
        processed_messages = []

        for msg in messages:
            # Extract date from message_date if available, otherwise use extraction date
            message_date = None
            if msg.get('message_date'):
                try:
                    # Parse ISO format date
                    message_date = pd.to_datetime(
                        msg.get('message_date')).replace(tzinfo=None)
                except:
                    message_date = None

            # Ensure extraction_date is always set (use today's date if missing)
            extraction_date = date if date else pd.Timestamp.now().date()

            processed_msg = {
                'id': msg.get('message_id'),
                'date': message_date,
                'message': msg.get('message_text', ''),
                'sender_id': msg.get('sender_id'),
                'sender_username': None,  # Not available in current scraper
                'sender_first_name': None,  # Not available in current scraper
                'sender_last_name': None,  # Not available in current scraper
                'reply_to_msg_id': None,  # Not available in current scraper
                'forward_from_id': None,  # Not available in current scraper
                'forward_from_name': None,  # Not available in current scraper
                'has_media': msg.get('has_media', False),
                'media_type': msg.get('media_type'),
                # Using file_path as media_filename
                'media_filename': msg.get('file_path'),
                'media_path': msg.get('file_path'),
                'views': None,  # Not available in current scraper
                'forwards': None,  # Not available in current scraper
                'replies': None,  # Not available in current scraper
                'reactions': None,  # Not available in current scraper
                'raw_data': json.dumps(msg),
                # Use channel_name from message if available
                'channel': msg.get('channel_name', channel),
                'extraction_date': extraction_date,
                'created_at': pd.Timestamp.now()
            }
            processed_messages.append(processed_msg)

        return processed_messages

    def load_to_database(self, data: List[Dict]):
        """Load processed data to PostgreSQL."""
        if not data:
            return

        df = pd.DataFrame(data)

        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            id BIGINT,
            date TIMESTAMP,
            message TEXT,
            sender_id BIGINT,
            sender_username VARCHAR(255),
            sender_first_name VARCHAR(255),
            sender_last_name VARCHAR(255),
            reply_to_msg_id BIGINT,
            forward_from_id BIGINT,
            forward_from_name VARCHAR(255),
            has_media BOOLEAN,
            media_type VARCHAR(50),
            media_filename VARCHAR(500),
            media_path VARCHAR(500),
            views INTEGER,
            forwards INTEGER,
            replies INTEGER,
            reactions JSONB,
            raw_data JSONB,
            channel VARCHAR(255),
            extraction_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()

        # Load data
        df.to_sql('telegram_messages', self.engine, schema='raw',
                  if_exists='append', index=False, method='multi')

        logger.info(f"Loaded {len(data)} messages to database")

    def clear_existing_data(self):
        """Clear existing data from the raw.telegram_messages table."""
        with self.engine.connect() as conn:
            conn.execute(text("DELETE FROM raw.telegram_messages"))
            conn.commit()
            logger.info(
                "Cleared existing data from raw.telegram_messages table")

    def run(self):
        """Main execution method."""
        logger.info("Starting raw data loading process...")

        # Create raw schema
        self.create_raw_schema()

        # Clear existing data
        self.clear_existing_data()

        # Find all JSON files in the data lake
        json_files = list(self.data_lake_path.rglob("*.json"))

        if not json_files:
            logger.warning("No JSON files found in data lake")
            return

        total_messages = 0

        for file_path in json_files:
            logger.info(f"Processing {file_path}")

            # Extract metadata from path
            channel = self.extract_channel_from_path(file_path)
            date = self.extract_date_from_path(file_path)

            # Load JSON data
            messages = self.load_json_file(file_path)

            if messages:
                # Process messages
                processed_messages = self.process_messages(
                    messages, channel, date)

                # Load to database
                self.load_to_database(processed_messages)

                total_messages += len(processed_messages)

        logger.info(f"Completed loading {total_messages} total messages")


def main():
    """Main function."""
    loader = RawDataLoader()
    loader.run()


if __name__ == "__main__":
    main()
