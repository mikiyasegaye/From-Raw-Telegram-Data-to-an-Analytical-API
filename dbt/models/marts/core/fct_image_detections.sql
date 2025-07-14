{{
  config(
    materialized='table',
    description='Fact table containing YOLO object detection results from Telegram images'
  )
}}

WITH detection_results AS (
  SELECT 
    image_file,
    class_name,
    confidence,
    bbox,
    bbox_normalized,
    created_at
  FROM {{ ref('stg_image_detections') }}
),

messages AS (
  SELECT 
    message_id,
    channel_name,
    message_date,
    has_media,
    file_path
  FROM {{ ref('fct_messages') }}
),

final AS (
  SELECT 
    -- Primary key
    ROW_NUMBER() OVER (ORDER BY dr.created_at, dr.image_file, dr.class_name) as detection_id,
    
    -- Foreign keys
    m.message_id,
    m.channel_name,
    
    -- Detection details
    dr.image_file,
    dr.class_name as detected_object_class,
    dr.confidence as confidence_score,
    
    -- Bounding box coordinates
    dr.bbox[1] as bbox_x1,
    dr.bbox[2] as bbox_y1, 
    dr.bbox[3] as bbox_x2,
    dr.bbox[4] as bbox_y2,
    
    -- Normalized coordinates
    dr.bbox_normalized[1] as bbox_norm_x1,
    dr.bbox_normalized[2] as bbox_norm_y1,
    dr.bbox_normalized[3] as bbox_norm_x2,
    dr.bbox_normalized[4] as bbox_norm_y2,
    
    -- Metadata
    m.message_date,
    dr.created_at as detection_timestamp,
    
    -- Flags
    CASE 
      WHEN dr.confidence >= 0.8 THEN 'high'
      WHEN dr.confidence >= 0.5 THEN 'medium'
      ELSE 'low'
    END as confidence_level
    
  FROM detection_results dr
  LEFT JOIN messages m 
    ON dr.image_file LIKE '%' || m.channel_name || '%'
    AND m.has_media = true
    AND m.file_path IS NOT NULL
)

SELECT * FROM final 