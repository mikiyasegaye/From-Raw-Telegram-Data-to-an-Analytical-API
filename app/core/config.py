"""
Configuration module for the Telegram Data Pipeline application.
Handles environment variables and provides centralized settings.
"""

import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    # Database Configuration
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql://postgres:password@localhost:5432/telegram_data")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "password")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "telegram_data")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))

    # Telegram API Configuration
    TELEGRAM_API_ID: Optional[str] = os.getenv("TELEGRAM_API_ID")
    TELEGRAM_API_HASH: Optional[str] = os.getenv("TELEGRAM_API_HASH")
    TELEGRAM_PHONE: Optional[str] = os.getenv("TELEGRAM_PHONE")
    TELEGRAM_SESSION_NAME: str = os.getenv(
        "TELEGRAM_SESSION_NAME", "telegram_session")

    # Application Configuration
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"

    # YOLO Configuration
    YOLO_MODEL: str = os.getenv("YOLO_MODEL", "yolov8n.pt")
    CONFIDENCE_THRESHOLD: float = float(
        os.getenv("CONFIDENCE_THRESHOLD", "0.5"))

    # API Configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))

    # Dagster Configuration
    DAGSTER_HOME: str = os.getenv("DAGSTER_HOME", "/app/dagster")

    # File Paths
    DATA_DIR: str = os.getenv("DATA_DIR", "data")
    RAW_DATA_DIR: str = os.path.join(DATA_DIR, "raw", "telegram_messages")
    PROCESSED_DATA_DIR: str = os.path.join(DATA_DIR, "processed")
    IMAGES_DIR: str = os.path.join(DATA_DIR, "images")
    LOGS_DIR: str = os.getenv("LOGS_DIR", "logs")

    # Telegram Channels to scrape
    TELEGRAM_CHANNELS = [
        # CheMed - የመድሀኒትና የህክምና እቃዎች አፋላጊ እና አቅራቢ ድርጅት (477 subscribers)
        "CheMed123",
        # Lobelia pharmacy and cosmetics - American and Canadian Genuine products (16,868 subscribers)
        "lobelia4cosmetics",
        # Tikvah | Pharma - Pharma Consultant, Sales, Marketing, Promotion (90,154 members)
        "tikvahpharma",
        # Add more channels from https://et.tgstat.com/medicine as needed
    ]

    @classmethod
    def validate(cls) -> bool:
        """Validate that all required environment variables are set."""
        required_vars = [
            "TELEGRAM_API_ID",
            "TELEGRAM_API_HASH",
            "TELEGRAM_PHONE"
        ]

        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)

        if missing_vars:
            print(
                f"Missing required environment variables: {', '.join(missing_vars)}")
            print("Please set these variables in your .env file")
            return False

        return True

    @classmethod
    def create_directories(cls):
        """Create necessary directories if they don't exist."""
        directories = [
            cls.RAW_DATA_DIR,
            cls.PROCESSED_DATA_DIR,
            cls.IMAGES_DIR,
            cls.LOGS_DIR
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)


# Create global settings instance
settings = Settings()
