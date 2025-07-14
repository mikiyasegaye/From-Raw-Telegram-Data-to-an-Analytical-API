#!/usr/bin/env python3
"""
Medical Products YOLO Inference Script

This script loads a trained YOLO model and runs inference on new images
to detect medical products from Telegram channel images.
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import List, Dict, Any
import cv2
import numpy as np
import pandas as pd
from ultralytics import YOLO
from app.utils.logger import get_logger

logger = get_logger(__name__)


class MedicalProductsDetector:
    """Medical products detector using trained YOLO model."""
    
    def __init__(self, model_path: str, confidence_threshold: float = 0.25):
        """
        Initialize the detector.
        
        Args:
            model_path: Path to the trained YOLO model
            confidence_threshold: Minimum confidence for detections
        """
        self.model_path = Path(model_path)
        self.confidence_threshold = confidence_threshold
        self.model = None
        self.classes = []
        
        # Load model and classes
        self._load_model()
    
    def _load_model(self):
        """Load the trained YOLO model and classes."""
        try:
            if not self.model_path.exists():
                raise FileNotFoundError(f"Model not found: {self.model_path}")
            
            logger.info(f"Loading model from: {self.model_path}")
            self.model = YOLO(str(self.model_path))
            
            # Load classes from model info or default
            model_info_path = self.model_path.parent / 'model_info.json'
            if model_info_path.exists():
                with open(model_info_path, 'r') as f:
                    model_info = json.load(f)
                self.classes = model_info.get('classes', [])
            else:
                # Default classes based on your dataset
                self.classes = ['gloves', 'medicine_strip', 'medicines', 'milk_powder', 'oximeter', 'syrings']
            
            logger.info(f"Model loaded successfully with {len(self.classes)} classes")
            logger.info(f"Classes: {self.classes}")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def detect_products(self, image_path: str) -> List[Dict[str, Any]]:
        """
        Detect medical products in an image.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            List of detection results
        """
        try:
            logger.info(f"Running detection on: {image_path}")
            
            # Run inference
            results = self.model(image_path, conf=self.confidence_threshold)
            
            detections = []
            for result in results:
                if result.boxes is not None:
                    boxes = result.boxes
                    for i in range(len(boxes)):
                        detection = {
                            'class_id': int(boxes.cls[i]),
                            'class_name': self.classes[int(boxes.cls[i])] if int(boxes.cls[i]) < len(self.classes) else f'class_{int(boxes.cls[i])}',
                            'confidence': float(boxes.conf[i]),
                            'bbox': boxes.xyxy[i].cpu().numpy().tolist(),  # [x1, y1, x2, y2]
                            'bbox_normalized': boxes.xyxyn[i].cpu().numpy().tolist(),  # normalized coordinates
                        }
                        detections.append(detection)
            
            logger.info(f"Found {len(detections)} detections")
            return detections
            
        except Exception as e:
            logger.error(f"Detection failed: {e}")
            return []
    
    def detect_batch(self, image_dir: str, output_dir: str = None) -> pd.DataFrame:
        """
        Detect medical products in a batch of images.
        
        Args:
            image_dir: Directory containing images
            output_dir: Directory to save annotated images (optional)
            
        Returns:
            DataFrame with detection results
        """
        image_dir = Path(image_dir)
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
        
        # Supported image extensions
        image_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.tiff']
        image_files = []
        for ext in image_extensions:
            image_files.extend(image_dir.glob(f'*{ext}'))
            image_files.extend(image_dir.glob(f'*{ext.upper()}'))
        
        logger.info(f"Found {len(image_files)} images in {image_dir}")
        
        all_results = []
        
        for image_file in image_files:
            logger.info(f"Processing: {image_file.name}")
            
            # Run detection
            detections = self.detect_products(str(image_file))
            
            # Save annotated image if output directory is specified
            if output_dir and detections:
                annotated_image = self._create_annotated_image(str(image_file), detections)
                output_path = output_dir / f"annotated_{image_file.name}"
                cv2.imwrite(str(output_path), annotated_image)
                logger.info(f"Saved annotated image: {output_path}")
            
            # Add results to list
            for detection in detections:
                result = {
                    'image_file': image_file.name,
                    'image_path': str(image_file),
                    **detection
                }
                all_results.append(result)
        
        # Create DataFrame
        results_df = pd.DataFrame(all_results)
        
        if not results_df.empty:
            logger.info(f"Detection summary:")
            logger.info(f"  Total images processed: {len(image_files)}")
            logger.info(f"  Total detections: {len(results_df)}")
            logger.info(f"  Detections per class:")
            class_counts = results_df['class_name'].value_counts()
            for class_name, count in class_counts.items():
                logger.info(f"    {class_name}: {count}")
        
        return results_df
    
    def _create_annotated_image(self, image_path: str, detections: List[Dict[str, Any]]) -> np.ndarray:
        """Create an annotated image with bounding boxes."""
        image = cv2.imread(image_path)
        
        for detection in detections:
            bbox = detection['bbox']
            class_name = detection['class_name']
            confidence = detection['confidence']
            
            # Draw bounding box
            x1, y1, x2, y2 = map(int, bbox)
            cv2.rectangle(image, (x1, y1), (x2, y2), (0, 255, 0), 2)
            
            # Add label
            label = f"{class_name}: {confidence:.2f}"
            cv2.putText(image, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
        
        return image
    
    def analyze_detections(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze detection results and generate insights.
        
        Args:
            results_df: DataFrame with detection results
            
        Returns:
            Dictionary with analysis results
        """
        if results_df.empty:
            return {}
        
        analysis = {
            'total_images': results_df['image_file'].nunique(),
            'total_detections': len(results_df),
            'detections_per_image': len(results_df) / results_df['image_file'].nunique(),
            'class_distribution': results_df['class_name'].value_counts().to_dict(),
            'confidence_stats': {
                'mean': results_df['confidence'].mean(),
                'std': results_df['confidence'].std(),
                'min': results_df['confidence'].min(),
                'max': results_df['confidence'].max()
            },
            'most_common_products': results_df['class_name'].value_counts().head(3).to_dict(),
            'high_confidence_detections': len(results_df[results_df['confidence'] > 0.8])
        }
        
        return analysis


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Medical Products YOLO Detection")
    parser.add_argument('--model', type=str, required=True,
                       help='Path to trained YOLO model')
    parser.add_argument('--input', type=str, required=True,
                       help='Input image or directory')
    parser.add_argument('--output', type=str, default=None,
                       help='Output directory for annotated images')
    parser.add_argument('--confidence', type=float, default=0.25,
                       help='Confidence threshold (default: 0.25)')
    parser.add_argument('--save-results', type=str, default=None,
                       help='Save results to CSV file')
    parser.add_argument('--analyze', action='store_true',
                       help='Generate analysis report')
    
    args = parser.parse_args()
    
    try:
        # Initialize detector
        detector = MedicalProductsDetector(args.model, args.confidence)
        
        input_path = Path(args.input)
        
        if input_path.is_file():
            # Single image detection
            detections = detector.detect_products(str(input_path))
            
            print(f"\n🔍 Detection Results for {input_path.name}:")
            for detection in detections:
                print(f"  {detection['class_name']}: {detection['confidence']:.3f}")
            
            # Save annotated image
            if args.output:
                output_dir = Path(args.output)
                output_dir.mkdir(parents=True, exist_ok=True)
                annotated_image = detector._create_annotated_image(str(input_path), detections)
                output_path = output_dir / f"annotated_{input_path.name}"
                cv2.imwrite(str(output_path), annotated_image)
                print(f"✅ Annotated image saved: {output_path}")
        
        elif input_path.is_dir():
            # Batch detection
            results_df = detector.detect_batch(str(input_path), args.output)
            
            if not results_df.empty:
                print(f"\n📊 Batch Detection Summary:")
                print(f"  Images processed: {results_df['image_file'].nunique()}")
                print(f"  Total detections: {len(results_df)}")
                print(f"  Average detections per image: {len(results_df)/results_df['image_file'].nunique():.2f}")
                
                # Save results
                if args.save_results:
                    results_df.to_csv(args.save_results, index=False)
                    print(f"✅ Results saved to: {args.save_results}")
                
                # Generate analysis
                if args.analyze:
                    analysis = detector.analyze_detections(results_df)
                    print(f"\n📈 Analysis Report:")
                    print(f"  Most common products:")
                    for product, count in analysis['most_common_products'].items():
                        print(f"    {product}: {count}")
                    print(f"  Average confidence: {analysis['confidence_stats']['mean']:.3f}")
                    print(f"  High confidence detections: {analysis['high_confidence_detections']}")
        
        else:
            print(f"❌ Input path does not exist: {input_path}")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Detection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 