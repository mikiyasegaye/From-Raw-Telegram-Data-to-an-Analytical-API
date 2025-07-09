#!/usr/bin/env python3
"""
Simple script to test database connection.
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def test_db_connection():
    """Test database connection with current environment variables."""

    # Get connection parameters
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'password')
    dbname = os.getenv('POSTGRES_DB', 'telegram_data')

    print(f"Testing connection to: {host}:{port}/{dbname}")
    print(f"User: {user}")

    try:
        # Connect to database
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )

        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()

        print("✅ Database connection successful!")
        print(f"PostgreSQL version: {version[0]}")

        # Test if raw schema exists
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'raw'
        """)

        if cursor.fetchone():
            print("✅ Raw schema exists")
        else:
            print("⚠️  Raw schema does not exist (will be created when loading data)")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


if __name__ == "__main__":
    test_db_connection()
