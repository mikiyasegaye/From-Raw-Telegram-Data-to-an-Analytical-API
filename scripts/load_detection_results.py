#!/usr/bin/env python3
"""
Load YOLO Detection Results to Database

This script loads the results from medical detection into the database
so they can be used in dbt models.
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from app.utils.logger import get_logger

logger = get_logger(__name__)


def load_detection_results_to_db(csv_file: str, db_url: str):
    """Load detection results from CSV to database."""
    
    try:
        # Read CSV file
        logger.info(f"Reading detection results from: {csv_file}")
        df = pd.read_csv(csv_file)
        
        if df.empty:
            logger.warning("No detection results to load")
            return
        
        logger.info(f"Found {len(df)} detection records")
        
        # Create database connection
        engine = create_engine(db_url)
        
        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.image_detections (
            id SERIAL PRIMARY KEY,
            image_file VARCHAR(255) NOT NULL,
            class_name VARCHAR(100) NOT NULL,
            confidence DECIMAL(5,4) NOT NULL,
            bbox_x1 INTEGER,
            bbox_y1 INTEGER,
            bbox_x2 INTEGER,
            bbox_y2 INTEGER,
            bbox_norm_x1 DECIMAL(10,8),
            bbox_norm_y1 DECIMAL(10,8),
            bbox_norm_x2 DECIMAL(10,8),
            bbox_norm_y2 DECIMAL(10,8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        # Prepare data for insertion
        insert_data = []
        for _, row in df.iterrows():
            bbox = eval(row['bbox']) if isinstance(row['bbox'], str) else row['bbox']
            bbox_norm = eval(row['bbox_normalized']) if isinstance(row['bbox_normalized'], str) else row['bbox_normalized']
            
            insert_data.append({
                'image_file': row['image_file'],
                'class_name': row['class_name'],
                'confidence': row['confidence'],
                'bbox_x1': int(bbox[0]) if len(bbox) >= 4 else None,
                'bbox_y1': int(bbox[1]) if len(bbox) >= 4 else None,
                'bbox_x2': int(bbox[2]) if len(bbox) >= 4 else None,
                'bbox_y2': int(bbox[3]) if len(bbox) >= 4 else None,
                'bbox_norm_x1': float(bbox_norm[0]) if len(bbox_norm) >= 4 else None,
                'bbox_norm_y1': float(bbox_norm[1]) if len(bbox_norm) >= 4 else None,
                'bbox_norm_x2': float(bbox_norm[2]) if len(bbox_norm) >= 4 else None,
                'bbox_norm_y2': float(bbox_norm[3]) if len(bbox_norm) >= 4 else None
            })
        
        # Insert data
        insert_df = pd.DataFrame(insert_data)
        insert_df.to_sql('image_detections', engine, schema='raw', if_exists='append', index=False)
        
        logger.info(f"Successfully loaded {len(insert_data)} detection records to database")
        
        # Print summary
        print(f"\n📊 Detection Results Summary:")
        print(f"  Total detections: {len(df)}")
        print(f"  Images processed: {df['image_file'].nunique()}")
        print(f"  Classes detected: {df['class_name'].nunique()}")
        print(f"  Average confidence: {df['confidence'].mean():.3f}")
        
        # Class distribution
        print(f"\n🏷️ Detections by class:")
        class_counts = df['class_name'].value_counts()
        for class_name, count in class_counts.items():
            print(f"  {class_name}: {count}")
        
    except Exception as e:
        logger.error(f"Failed to load detection results: {e}")
        raise


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Load YOLO detection results to database")
    parser.add_argument('--csv-file', type=str, default='results/medical_detection_results.csv',
                       help='Path to CSV file with detection results')
    parser.add_argument('--db-url', type=str, 
                       default='postgresql://postgres:password@localhost:5432/telegram_data',
                       help='Database connection URL')
    
    args = parser.parse_args()
    
    # Check if CSV file exists
    if not Path(args.csv_file).exists():
        print(f"❌ CSV file not found: {args.csv_file}")
        print("Please run the medical detection script first to generate results.")
        sys.exit(1)
    
    try:
        load_detection_results_to_db(args.csv_file, args.db_url)
        print("✅ Detection results loaded successfully!")
        print("You can now run dbt to create the fact table:")
        print("  dbt run --select fct_image_detections")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 