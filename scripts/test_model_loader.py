# scripts/test_model_loader.py
"""
Quick test script to verify ONNX and OpenVINO models are working correctly
"""
import cv2
import numpy as np
import time
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.model_loader import ModelFactory, ModelBackend


def test_model(model_path: str, backend: ModelBackend, test_image: str = None):
    """
    Test a model with the given backend
    
    Args:
        model_path: Path to model file or directory
        backend: Backend to use (ONNX or OpenVINO)
        test_image: Optional path to test image
    """
    print(f"\n{'='*60}")
    print(f"Testing {model_path} with {backend.value}")
    print(f"{'='*60}")
    
    try:
        # Create model loader
        loader = ModelFactory.create_loader(
            model_path=model_path,
            backend=backend,
            confidence_threshold=0.5
        )
        
        # Load model
        print("📦 Loading model...")
        start = time.time()
        loader.load_model()
        load_time = time.time() - start
        print(f"✅ Model loaded in {load_time:.3f} seconds")
        
        # Create dummy input if no test image provided
        if test_image and Path(test_image).exists():
            print(f"📷 Loading test image: {test_image}")
            image = cv2.imread(test_image)
            if image is None:
                raise ValueError(f"Failed to load image: {test_image}")
        else:
            print("📷 Creating dummy test image (640x640x3)")
            image = np.random.randint(0, 255, (640, 640, 3), dtype=np.uint8)
        
        # Warmup
        print("🔥 Warming up (10 iterations)...")
        for _ in range(10):
            _ = loader.predict(image)
        
        # Benchmark
        print("⏱️  Benchmarking (100 iterations)...")
        iterations = 100
        start = time.time()
        
        for _ in range(iterations):
            outputs = loader.predict(image)
        
        elapsed = time.time() - start
        avg_time = elapsed / iterations
        fps = iterations / elapsed
        
        print(f"\n📊 Results:")
        print(f"   Average time: {avg_time*1000:.2f} ms/frame")
        print(f"   Throughput: {fps:.2f} FPS")
        print(f"   Output shape: {outputs.shape}")
        
        # Test postprocessing
        print("\n🔄 Testing postprocessing...")
        detections = loader.postprocess(
            outputs,
            original_shape=(image.shape[0], image.shape[1])
        )
        print(f"   Detections: {len(detections)}")
        
        if detections:
            print(f"   Sample detection: {detections[0]}")
        
        print(f"\n✅ Test passed for {backend.value}!")
        
        return {
            'backend': backend.value,
            'load_time': load_time,
            'avg_inference_time': avg_time,
            'fps': fps,
            'output_shape': outputs.shape,
            'num_detections': len(detections)
        }
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def compare_backends(onnx_path: str, openvino_path: str, test_image: str = None):
    """
    Compare ONNX and OpenVINO backends
    
    Args:
        onnx_path: Path to ONNX model
        openvino_path: Path to OpenVINO model directory
        test_image: Optional path to test image
    """
    results = {}
    
    # Test ONNX
    if Path(onnx_path).exists():
        onnx_result = test_model(onnx_path, ModelBackend.ONNX, test_image)
        if onnx_result:
            results['onnx'] = onnx_result
    else:
        print(f"⚠️  ONNX model not found: {onnx_path}")
    
    # Test OpenVINO
    if Path(openvino_path).exists():
        openvino_result = test_model(openvino_path, ModelBackend.OPENVINO, test_image)
        if openvino_result:
            results['openvino'] = openvino_result
    else:
        print(f"⚠️  OpenVINO model not found: {openvino_path}")
    
    # Print comparison
    if len(results) > 1:
        print(f"\n{'='*60}")
        print("📊 Comparison Summary")
        print(f"{'='*60}")
        
        print(f"\n{'Metric':<25} {'ONNX':<15} {'OpenVINO':<15} {'Speedup':<10}")
        print("-" * 70)
        
        if 'onnx' in results and 'openvino' in results:
            onnx = results['onnx']
            openvino = results['openvino']
            
            speedup = onnx['avg_inference_time'] / openvino['avg_inference_time']
            
            print(f"{'Load Time (s)':<25} {onnx['load_time']:<15.3f} {openvino['load_time']:<15.3f} {openvino['load_time']/onnx['load_time']:<10.2f}x")
            print(f"{'Inference Time (ms)':<25} {onnx['avg_inference_time']*1000:<15.2f} {openvino['avg_inference_time']*1000:<15.2f} {speedup:<10.2f}x")
            print(f"{'FPS':<25} {onnx['fps']:<15.2f} {openvino['fps']:<15.2f} {openvino['fps']/onnx['fps']:<10.2f}x")
            print(f"{'Detections':<25} {onnx['num_detections']:<15} {openvino['num_detections']:<15}")
            
            print(f"\n{'='*60}")
            if speedup > 1:
                print(f"🎉 OpenVINO is {speedup:.2f}x FASTER than ONNX!")
            else:
                print(f"⚠️  ONNX is {1/speedup:.2f}x faster than OpenVINO")
            print(f"{'='*60}")


def test_all_models(test_image: str = None):
    """Test all available models"""
    models_dir = Path("yolo_models")
    
    model_pairs = [
        ("car_detection.onnx", "car_detection_openvino"),
        ("plate_detection.onnx", "plate_detection_openvino"),
        ("character_detection.onnx", "character_detection_openvino")
    ]
    
    for onnx_name, openvino_name in model_pairs:
        onnx_path = models_dir / onnx_name
        openvino_path = models_dir / openvino_name
        
        print(f"\n\n{'#'*60}")
        print(f"# Testing {onnx_name.replace('.onnx', '')}")
        print(f"{'#'*60}")
        
        compare_backends(str(onnx_path), str(openvino_path), test_image)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test ONNX/OpenVINO model loaders")
    parser.add_argument("--model", help="Path to specific model to test")
    parser.add_argument("--backend", choices=["onnx", "openvino"], 
                       help="Backend to use (if testing single model)")
    parser.add_argument("--image", help="Path to test image (optional)")
    parser.add_argument("--all", action="store_true", help="Test all models")
    
    args = parser.parse_args()
    
    if args.all:
        test_all_models(args.image)
    elif args.model:
        backend = ModelBackend.ONNX if args.backend == "onnx" else ModelBackend.OPENVINO
        test_model(args.model, backend, args.image)
    else:
        # Default: test all models
        test_all_models(args.image)