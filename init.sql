-- Initialize the telegram_data database
-- This script runs when the PostgreSQL container starts
-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
-- Create raw tables for initial data storage
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
-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel_id ON raw.telegram_messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_message_date ON raw.telegram_messages(message_date);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_has_media ON raw.telegram_messages(has_media);
-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = CURRENT_TIMESTAMP;
RETURN NEW;
END;
$$ language 'plpgsql';
-- Create trigger for updated_at
CREATE TRIGGER update_telegram_messages_updated_at BEFORE
UPDATE ON raw.telegram_messages FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA marts TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO postgres;