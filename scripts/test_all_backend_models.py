#!/usr/bin/env python3
"""
Test all backends (PyTorch, ONNX, OpenVINO) to ensure they work correctly
"""

import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.model_loader import ModelBackend
from app.services.car_detection_service import CarDetectionService
from app.services.plate_detection_service import PlateDetectionService
from app.services.character_detection_service import CharacterDetectionService
import cv2
import numpy as np
import time


def create_test_image():
    """Create a dummy test image"""
    return np.random.randint(0, 255, (640, 640, 3), dtype=np.uint8)


def test_backend(backend: ModelBackend, test_image: np.ndarray):
    """
    Test a specific backend
    
    Args:
        backend: Backend to test
        test_image: Test image
    """
    print(f"\n{'='*60}")
    print(f"Testing {backend.value.upper()} Backend")
    print(f"{'='*60}\n")
    
    results = {}
    
    # Test Car Detection
    print(f"1️⃣  Testing Car Detection...")
    try:
        car_service = CarDetectionService(backend=backend)
        car_service.load_model()
        
        # Warmup
        for _ in range(5):
            _ = car_service.detect_cars(test_image)
        
        # Benchmark
        start = time.time()
        iterations = 50
        for _ in range(iterations):
            detections = car_service.detect_cars(test_image)
        elapsed = time.time() - start
        
        results['car'] = {
            'status': 'success',
            'fps': iterations / elapsed,
            'avg_time_ms': (elapsed / iterations) * 1000,
            'detections': len(detections) if detections else 0
        }
        
        print(f"   ✅ Success: {results['car']['fps']:.2f} FPS")
        
    except Exception as e:
        results['car'] = {'status': 'failed', 'error': str(e)}
        print(f"   ❌ Failed: {e}")
    
    # Test Plate Detection
    print(f"\n2️⃣  Testing Plate Detection...")
    try:
        plate_service = PlateDetectionService(backend=backend)
        plate_service.load_model()
        
        # Warmup
        for _ in range(5):
            _ = plate_service.detect_plates(test_image)
        
        # Benchmark
        start = time.time()
        iterations = 50
        for _ in range(iterations):
            detections = plate_service.detect_plates(test_image)
        elapsed = time.time() - start
        
        results['plate'] = {
            'status': 'success',
            'fps': iterations / elapsed,
            'avg_time_ms': (elapsed / iterations) * 1000,
            'detections': len(detections) if detections else 0
        }
        
        print(f"   ✅ Success: {results['plate']['fps']:.2f} FPS")
        
    except Exception as e:
        results['plate'] = {'status': 'failed', 'error': str(e)}
        print(f"   ❌ Failed: {e}")
    
    # Test Character Detection
    print(f"\n3️⃣  Testing Character Detection...")
    try:
        char_service = CharacterDetectionService(backend=backend)
        char_service.load_model()
        
        # Warmup
        for _ in range(5):
            _ = char_service.detect_characters(test_image)
        
        # Benchmark
        start = time.time()
        iterations = 50
        for _ in range(iterations):
            result = char_service.detect_characters(test_image)
        elapsed = time.time() - start
        
        results['character'] = {
            'status': 'success',
            'fps': iterations / elapsed,
            'avg_time_ms': (elapsed / iterations) * 1000,
            'chars_detected': result.get('char_count', 0)
        }
        
        print(f"   ✅ Success: {results['character']['fps']:.2f} FPS")
        
    except Exception as e:
        results['character'] = {'status': 'failed', 'error': str(e)}
        print(f"   ❌ Failed: {e}")
    
    return results


def print_summary(all_results: dict):
    """Print comparison summary"""
    print(f"\n\n{'='*80}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*80}\n")
    
    # Header
    print(f"{'Model':<20} {'Backend':<12} {'Status':<10} {'FPS':<12} {'Time (ms)':<12}")
    print("-" * 80)
    
    # Results
    for backend_name, results in all_results.items():
        for model_name, result in results.items():
            status = result.get('status', 'unknown')
            
            if status == 'success':
                fps = f"{result.get('fps', 0):.2f}"
                time_ms = f"{result.get('avg_time_ms', 0):.2f}"
                status_icon = "✅"
            else:
                fps = "N/A"
                time_ms = "N/A"
                status_icon = "❌"
            
            print(f"{model_name.title():<20} {backend_name:<12} {status_icon:<10} {fps:<12} {time_ms:<12}")
    
    print("\n" + "="*80)
    
    # Overall success rate
    total = 0
    success = 0
    
    for results in all_results.values():
        for result in results.values():
            total += 1
            if result.get('status') == 'success':
                success += 1
    
    print(f"\nOverall Success Rate: {success}/{total} ({success/total*100:.1f}%)")
    
    # Recommendations
    print(f"\n{'='*80}")
    print("RECOMMENDATIONS")
    print(f"{'='*80}\n")
    
    # Find fastest backend for each model
    for model_name in ['car', 'plate', 'character']:
        fastest_backend = None
        fastest_fps = 0
        
        for backend_name, results in all_results.items():
            if model_name in results and results[model_name].get('status') == 'success':
                fps = results[model_name].get('fps', 0)
                if fps > fastest_fps:
                    fastest_fps = fps
                    fastest_backend = backend_name
        
        if fastest_backend:
            print(f"🏆 {model_name.title()} Detection: Use {fastest_backend.upper()} ({fastest_fps:.2f} FPS)")
    
    print()


def main():
    """Main test runner"""
    print("🧪 Testing All Model Backends")
    print("="*80)
    
    # Create test image
    test_image = create_test_image()
    print(f"📷 Created test image: {test_image.shape}")
    
    # Test each backend
    backends_to_test = [
        ModelBackend.PYTORCH,
        ModelBackend.ONNX,
        ModelBackend.OPENVINO
    ]
    
    all_results = {}
    
    for backend in backends_to_test:
        try:
            results = test_backend(backend, test_image)
            all_results[backend.value] = results
        except Exception as e:
            print(f"\n❌ Fatal error testing {backend.value}: {e}")
            import traceback
            traceback.print_exc()
    
    # Print summary
    if all_results:
        print_summary(all_results)
    else:
        print("\n❌ No results to display - all tests failed!")
        return 1
    
    # Check if all succeeded
    all_success = all(
        result.get('status') == 'success'
        for results in all_results.values()
        for result in results.values()
    )
    
    if all_success:
        print("\n✅ All backends working perfectly!")
        return 0
    else:
        print("\n⚠️  Some backends have issues. See errors above.")
        return 1


if __name__ == "__main__":
    exit(main())