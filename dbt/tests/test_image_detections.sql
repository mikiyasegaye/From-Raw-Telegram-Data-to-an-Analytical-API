-- Test that detection_id is unique
SELECT detection_id, COUNT(*) as count
FROM {{ ref('fct_image_detections') }}
GROUP BY detection_id
HAVING COUNT(*) > 1

-- Test that confidence scores are within valid range (0-1)
SELECT *
FROM {{ ref('fct_image_detections') }}
WHERE confidence_score < 0 OR confidence_score > 1

-- Test that bounding box coordinates are valid
SELECT *
FROM {{ ref('fct_image_detections') }}
WHERE bbox_x1 >= bbox_x2 OR bbox_y1 >= bbox_y2

-- Test that normalized coordinates are within range (0-1)
SELECT *
FROM {{ ref('fct_image_detections') }}
WHERE bbox_norm_x1 < 0 OR bbox_norm_x1 > 1
   OR bbox_norm_y1 < 0 OR bbox_norm_y1 > 1
   OR bbox_norm_x2 < 0 OR bbox_norm_x2 > 1
   OR bbox_norm_y2 < 0 OR bbox_norm_y2 > 1 