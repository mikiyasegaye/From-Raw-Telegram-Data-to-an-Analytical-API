"""
Database setup module for the Telegram Data Pipeline.
Handles database initialization and connection setup.
"""

import logging
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from app.core.config import settings

# Configure logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)


def create_database_engine():
    """Create SQLAlchemy engine for database connection."""
    try:
        engine = create_engine(
            settings.DATABASE_URL,
            echo=settings.DEBUG,
            pool_pre_ping=True,
            pool_recycle=300
        )
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        return None


def test_database_connection():
    """Test database connection."""
    engine = create_database_engine()
    if not engine:
        return False

    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
    except OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during database connection: {e}")
        return False


def setup_database():
    """Set up database tables and schemas."""
    engine = create_database_engine()
    if not engine:
        logger.error("Cannot setup database: engine creation failed")
        return False

    try:
        with engine.connect() as connection:
            # Create schemas if they don't exist
            schemas = ["raw", "staging", "marts"]
            for schema in schemas:
                connection.execute(
                    text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

            # Create raw.telegram_messages table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                id SERIAL PRIMARY KEY,
                message_id BIGINT,
                channel_id BIGINT,
                channel_name VARCHAR(255),
                sender_id BIGINT,
                sender_name VARCHAR(255),
                message_text TEXT,
                message_date TIMESTAMP,
                has_media BOOLEAN DEFAULT FALSE,
                media_type VARCHAR(50),
                media_url TEXT,
                file_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            connection.execute(text(create_table_sql))

            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel_id ON raw.telegram_messages(channel_id);",
                "CREATE INDEX IF NOT EXISTS idx_telegram_messages_message_date ON raw.telegram_messages(message_date);",
                "CREATE INDEX IF NOT EXISTS idx_telegram_messages_has_media ON raw.telegram_messages(has_media);"
            ]

            for index_sql in indexes:
                connection.execute(text(index_sql))

            connection.commit()
            logger.info("Database setup completed successfully")
            return True

    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False


def main():
    """Main function to run database setup."""
    logger.info("Starting database setup...")

    # Create necessary directories
    settings.create_directories()

    # Test connection
    if not test_database_connection():
        logger.error("Database connection test failed")
        sys.exit(1)

    # Setup database
    if not setup_database():
        logger.error("Database setup failed")
        sys.exit(1)

    logger.info("Database setup completed successfully")


if __name__ == "__main__":
    main()
