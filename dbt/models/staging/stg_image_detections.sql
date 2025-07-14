{{
  config(
    materialized='view',
    description='Staging model for YOLO image detection results'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw_data', 'image_detections') }}
),

cleaned AS (
  SELECT 
    image_file,
    class_name,
    confidence,
    bbox,
    bbox_normalized,
    CURRENT_TIMESTAMP as created_at
  FROM source
  WHERE confidence > 0.1  -- Filter out very low confidence detections
)

SELECT * FROM cleaned 