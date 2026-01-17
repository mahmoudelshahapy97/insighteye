#!/usr/bin/env python3
"""
Model Manager - All-in-one tool for managing YOLO models
"""

import argparse
from pathlib import Path
import sys


def check_models():
    """Check all models for dynamic shapes"""
    print("🔍 Checking OpenVINO models for dynamic shapes...\n")
    
    try:
        from openvino.runtime import Core
    except ImportError:
        print("❌ OpenVINO not installed!")
        print("   Install with: pip install openvino")
        return False
    
    models_dir = Path("yolo_models")
    openvino_dirs = [
        "car_detection_openvino",
        "plate_detection_openvino",
        "character_detection_openvino"
    ]
    
    all_ok = True
    
    for dir_name in openvino_dirs:
        model_path = models_dir / dir_name
        xml_files = list(model_path.glob("*.xml"))
        
        if not xml_files:
            print(f"⚠️  {dir_name}: Not found")
            all_ok = False
            continue
        
        try:
            core = Core()
            model = core.read_model(str(xml_files[0]))
            
            input_dynamic = model.input(0).shape.is_dynamic
            output_dynamic = model.output(0).shape.is_dynamic
            
            if input_dynamic or output_dynamic:
                print(f"❌ {dir_name}: HAS DYNAMIC SHAPES (needs fix)")
                all_ok = False
            else:
                print(f"✅ {dir_name}: Fixed shapes (OK)")
        
        except Exception as e:
            print(f"⚠️  {dir_name}: Error - {e}")
            all_ok = False
    
    print()
    return all_ok


def fix_models():
    """Fix all models by reconverting with fixed shapes"""
    print("🔧 Fixing OpenVINO models (converting with fixed shapes)...\n")
    
    try:
        from ultralytics import YOLO
    except ImportError:
        print("❌ Ultralytics not installed!")
        print("   Install with: pip install ultralytics")
        return False
    
    models_dir = Path("yolo_models")
    pt_models = [
        "car_detection.pt",
        "plate_detection.pt",
        "character_detection.pt"
    ]
    
    success_count = 0
    
    for model_name in pt_models:
        model_path = models_dir / model_name
        
        if not model_path.exists():
            print(f"⚠️  {model_name}: Not found, skipping")
            continue
        
        output_dir = models_dir / f"{model_path.stem}_openvino"
        
        try:
            print(f"📦 Converting {model_name}...")
            model = YOLO(str(model_path))
            
            model.export(
                format='openvino',
                imgsz=640,
                half=False,
                dynamic=False,  # CRITICAL: Fixed shapes!
                batch=1
            )
            
            print(f"✅ {model_name}: Success\n")
            success_count += 1
            
        except Exception as e:
            print(f"❌ {model_name}: Failed - {e}\n")
    
    print(f"{'='*60}")
    print(f"Converted {success_count}/{len(pt_models)} models successfully")
    print(f"{'='*60}\n")
    
    return success_count == len(pt_models)


def convert_to_onnx():
    """Convert all models to ONNX"""
    print("🔄 Converting models to ONNX...\n")
    
    try:
        from ultralytics import YOLO
    except ImportError:
        print("❌ Ultralytics not installed!")
        return False
    
    models_dir = Path("yolo_models")
    pt_models = [
        "car_detection.pt",
        "plate_detection.pt",
        "character_detection.pt"
    ]
    
    success_count = 0
    
    for model_name in pt_models:
        model_path = models_dir / model_name
        
        if not model_path.exists():
            print(f"⚠️  {model_name}: Not found")
            continue
        
        try:
            print(f"📦 Converting {model_name} to ONNX...")
            model = YOLO(str(model_path))
            
            model.export(
                format='onnx',
                imgsz=640,
                dynamic=False,
                simplify=True,
                opset=12
            )
            
            print(f"✅ {model_name}: Success\n")
            success_count += 1
            
        except Exception as e:
            print(f"❌ {model_name}: Failed - {e}\n")
    
    return success_count == len(pt_models)


def test_models(backend='openvino'):
    """Test model loading and inference"""
    print(f"🧪 Testing {backend.upper()} models...\n")
    
    # Add parent to path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    try:
        from app.core.model_loader import ModelFactory, ModelBackend
        import cv2
        import numpy as np
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    
    models_dir = Path("yolo_models")
    
    if backend == 'openvino':
        models = [
            ("car_detection_openvino", ModelBackend.OPENVINO),
            ("plate_detection_openvino", ModelBackend.OPENVINO),
            ("character_detection_openvino", ModelBackend.OPENVINO)
        ]
    else:  # onnx
        models = [
            ("car_detection.onnx", ModelBackend.ONNX),
            ("plate_detection.onnx", ModelBackend.ONNX),
            ("character_detection.onnx", ModelBackend.ONNX)
        ]
    
    # Create dummy image
    test_image = np.random.randint(0, 255, (640, 640, 3), dtype=np.uint8)
    
    all_ok = True
    
    for model_path, backend_type in models:
        full_path = models_dir / model_path
        
        if not full_path.exists():
            print(f"⚠️  {model_path}: Not found")
            all_ok = False
            continue
        
        try:
            print(f"Testing {model_path}...")
            
            loader = ModelFactory.create_loader(
                str(full_path),
                backend=backend_type,
                confidence_threshold=0.5
            )
            
            loader.load_model()
            output = loader.predict(test_image)
            
            print(f"✅ {model_path}: OK (output shape: {output.shape})\n")
            
        except Exception as e:
            print(f"❌ {model_path}: Failed - {e}\n")
            all_ok = False
    
    return all_ok


def show_status():
    """Show current model status"""
    print("📊 Current Model Status\n")
    print(f"{'='*60}")
    
    models_dir = Path("yolo_models")
    
    # Check PyTorch models
    print("\nPyTorch Models (.pt):")
    for pt in ["car_detection.pt", "plate_detection.pt", "character_detection.pt"]:
        path = models_dir / pt
        status = "✅ Exists" if path.exists() else "❌ Missing"
        print(f"  {pt:<30} {status}")
    
    # Check ONNX models
    print("\nONNX Models (.onnx):")
    for onnx in ["car_detection.onnx", "plate_detection.onnx", "character_detection.onnx"]:
        path = models_dir / onnx
        status = "✅ Exists" if path.exists() else "❌ Missing"
        print(f"  {onnx:<30} {status}")
    
    # Check OpenVINO models
    print("\nOpenVINO Models (directories):")
    for openvino in ["car_detection_openvino", "plate_detection_openvino", "character_detection_openvino"]:
        path = models_dir / openvino
        if path.exists():
            xml_exists = any(path.glob("*.xml"))
            bin_exists = any(path.glob("*.bin"))
            if xml_exists and bin_exists:
                status = "✅ Complete"
            else:
                status = "⚠️  Incomplete"
        else:
            status = "❌ Missing"
        print(f"  {openvino:<30} {status}")
    
    print(f"\n{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description="Model Manager - Manage YOLO models for OpenVINO/ONNX",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python model_manager.py status          # Show current status
  python model_manager.py check           # Check for dynamic shapes
  python model_manager.py fix             # Fix dynamic shape issues
  python model_manager.py onnx            # Convert to ONNX
  python model_manager.py test            # Test OpenVINO models
  python model_manager.py test --backend onnx  # Test ONNX models
        """
    )
    
    parser.add_argument(
        'command',
        choices=['status', 'check', 'fix', 'onnx', 'test'],
        help='Command to run'
    )
    
    parser.add_argument(
        '--backend',
        choices=['openvino', 'onnx'],
        default='openvino',
        help='Backend to use for testing (default: openvino)'
    )
    
    args = parser.parse_args()
    
    if args.command == 'status':
        show_status()
    
    elif args.command == 'check':
        result = check_models()
        if result:
            print("✅ All models have fixed shapes!")
        else:
            print("❌ Some models have issues. Run 'fix' command to resolve.")
            sys.exit(1)
    
    elif args.command == 'fix':
        result = fix_models()
        if result:
            print("✅ All models fixed!")
            print("\nNext steps:")
            print("1. Set MODEL_BACKEND=openvino in .env")
            print("2. Restart: docker-compose restart vehicle-plate-api")
        else:
            print("❌ Some models failed to convert.")
            sys.exit(1)
    
    elif args.command == 'onnx':
        result = convert_to_onnx()
        if result:
            print("✅ All models converted to ONNX!")
        else:
            print("❌ Some conversions failed.")
            sys.exit(1)
    
    elif args.command == 'test':
        result = test_models(args.backend)
        if result:
            print(f"✅ All {args.backend.upper()} models working!")
        else:
            print(f"❌ Some {args.backend.upper()} models failed.")
            sys.exit(1)


if __name__ == "__main__":
    main()