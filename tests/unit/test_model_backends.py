# test_model_backends.py
import numpy as np
import cv2
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.model_loader import ModelFactory, ModelBackend


def run_model_backend_test(model_path: str, backend: ModelBackend, test_image_path: str):
    """Test a specific backend with diagnostic output"""
    
    print(f"\n{'='*80}")
    print(f"Testing {backend.value.upper()} Backend")
    print(f"{'='*80}")
    
    try:
        # Load model
        print(f"Loading model from: {model_path}")
        loader = ModelFactory.create_loader(
            model_path,
            backend=backend,
            confidence_threshold=0.5
        )
        
        loader.load_model()
        print(f"✅ Model loaded successfully")
        
        # Load test image
        print(f"\nLoading test image: {test_image_path}")
        image = cv2.imread(test_image_path)
        
        if image is None:
            print(f"❌ Could not load test image")
            return
        
        h, w = image.shape[:2]
        print(f"Image shape: {h}x{w}")
        
        # Run inference
        print(f"\n🔄 Running inference...")
        raw_output = loader.predict(image)
        
        print(f"Raw output shape: {raw_output.shape}")
        print(f"Raw output dtype: {raw_output.dtype}")
        print(f"Raw output range: [{raw_output.min():.4f}, {raw_output.max():.4f}]")
        
        # For debugging: show a sample of the output
        if len(raw_output.shape) == 3:
            print(f"\nOutput format analysis:")
            print(f"  Batch size: {raw_output.shape[0]}")
            print(f"  Dimension 2: {raw_output.shape[1]}")
            print(f"  Dimension 3: {raw_output.shape[2]}")
            
            # Sample first few predictions
            sample = raw_output[0, :, :5] if raw_output.shape[1] < raw_output.shape[2] else raw_output[0, :5, :]
            print(f"\nSample predictions (first 5):")
            print(sample)
        
        # Postprocess
        print(f"\n🔄 Postprocessing...")
        detections = loader.postprocess(
            raw_output,
            original_shape=(h, w),
            input_size=(640, 640),
            iou_threshold=0.45
        )
        
        print(f"\n✅ Postprocessing complete")
        print(f"Number of detections: {len(detections)}")
        
        if detections:
            print(f"\nDetections:")
            for i, det in enumerate(detections[:10]):  # Show first 10
                bbox = det['bbox']
                conf = det['confidence']
                cls_id = det['class_id']
                print(f"  [{i}] Class {cls_id}, Conf: {conf:.3f}, BBox: {bbox}")
        else:
            print(f"⚠️ No detections found")
            print(f"\nPossible reasons:")
            print(f"  1. Confidence threshold too high (current: {loader.confidence_threshold})")
            print(f"  2. Model output format mismatch")
            print(f"  3. No objects in the image")
            print(f"\nTry lowering confidence threshold or check model output format")
        
        # Annotate and save result
        if detections:
            annotated = image.copy()
            for det in detections:
                x1, y1, x2, y2 = det['bbox']
                conf = det['confidence']
                cls_id = det['class_id']
                
                # Draw box
                cv2.rectangle(annotated, (x1, y1), (x2, y2), (0, 255, 0), 2)
                
                # Draw label
                label = f"Class {cls_id}: {conf:.2f}"
                cv2.putText(annotated, label, (x1, y1 - 10),
                           cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
            
            # Save result
            output_path = f"test_output_{backend.value}.jpg"
            cv2.imwrite(output_path, annotated)
            print(f"\n💾 Annotated image saved to: {output_path}")
        
        print(f"\n✅ {backend.value.upper()} backend test PASSED")
        return True
        
    except Exception as e:
        print(f"\n❌ {backend.value.upper()} backend test FAILED")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main test function"""
    
    # Configuration
    test_configs = [
        # Format: (model_path, backend, test_image_path)
        ("models/people.pt", ModelBackend.PYTORCH, "test_image.jpg"),
        ("models/people.onnx", ModelBackend.ONNX, "test_image.jpg"),
        ("models/people_openvino", ModelBackend.OPENVINO, "test_image.jpg"),
        ("models/people.engine", ModelBackend.TENSORRT, "test_image.jpg"),
    ]
    
    results = {}
    
    for model_path, backend, test_image in test_configs:
        if not Path(model_path).exists():
            print(f"\n⚠️ Skipping {backend.value}: Model not found at {model_path}")
            continue
        
        if not Path(test_image).exists():
            print(f"\n⚠️ Test image not found: {test_image}")
            print(f"Please provide a test image or use a different path")
            continue
        
        success = run_model_backend_test(model_path, backend, test_image)
        results[backend.value] = success
    
    # Summary
    print(f"\n{'='*80}")
    print(f"TEST SUMMARY")
    print(f"{'='*80}")
    
    for backend, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{backend:15s}: {status}")
    
    print(f"{'='*80}\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test YOLO model backends")
    parser.add_argument("--model", help="Path to model file")
    parser.add_argument("--backend", choices=["pytorch", "onnx", "openvino", "tensorrt"],
                       help="Backend to use")
    parser.add_argument("--image", help="Path to test image")
    parser.add_argument("--conf", type=float, default=0.5,
                       help="Confidence threshold (default: 0.5)")
    
    args = parser.parse_args()
    
    if args.model and args.backend and args.image:
        # Single test mode
        backend = ModelBackend(args.backend)
        run_model_backend_test(args.model, backend, args.image)
    else:
        # Run all tests
        main()