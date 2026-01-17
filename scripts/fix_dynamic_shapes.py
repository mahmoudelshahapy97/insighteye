#!/usr/bin/env python3
"""
Quick fix script to reconvert models with FIXED shapes (not dynamic)
This fixes the "get_shape was called on a descriptor::Tensor with dynamic shape" error
"""

from pathlib import Path
from ultralytics import YOLO

def fix_openvino_model(pt_model_path: str, output_dir: str = None):
    """
    Reconvert a PyTorch model to OpenVINO with FIXED shapes
    
    Args:
        pt_model_path: Path to .pt model
        output_dir: Output directory name
    """
    model_path = Path(pt_model_path)
    
    if not model_path.exists():
        print(f"❌ Model not found: {model_path}")
        return False
    
    if output_dir is None:
        output_dir = f"{model_path.stem}_openvino"
    
    print(f"\n{'='*60}")
    print(f"Fixing {model_path.name}")
    print(f"{'='*60}")
    
    try:
        # Load model
        print("📦 Loading PyTorch model...")
        model = YOLO(str(model_path))
        
        # Export with FIXED shapes
        print("🔄 Exporting to OpenVINO (fixed shapes)...")
        model.export(
            format='openvino',
            imgsz=640,         # Fixed size: 640x640
            half=False,        # FP32 precision
            dynamic=False,     # ⭐ CRITICAL: Fixed shapes only!
            batch=1            # Fixed batch size
        )
        
        print(f"✅ Success! Model exported to: {output_dir}/")
        print(f"   Files created:")
        
        output_path = Path(output_dir)
        if output_path.exists():
            for f in output_path.glob("*"):
                print(f"   - {f.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def fix_all_models():
    """Fix all models in yolo_models directory"""
    
    models_dir = Path("yolo_models")
    
    if not models_dir.exists():
        print(f"❌ Directory not found: {models_dir}")
        return
    
    pt_models = [
        "car_detection.pt",
        "plate_detection.pt", 
        "character_detection.pt"
    ]
    
    success_count = 0
    
    for model_name in pt_models:
        model_path = models_dir / model_name
        
        if not model_path.exists():
            print(f"⚠️  Skipping {model_name} (not found)")
            continue
        
        output_dir = models_dir / f"{model_path.stem}_openvino"
        
        if fix_openvino_model(str(model_path), str(output_dir)):
            success_count += 1
    
    print(f"\n{'='*60}")
    print(f"Summary: {success_count}/{len(pt_models)} models converted successfully")
    print(f"{'='*60}")
    
    if success_count == len(pt_models):
        print("\n✅ All models fixed! You can now use OpenVINO backend.")
        print("\nNext steps:")
        print("1. Make sure MODEL_BACKEND=openvino in your .env file")
        print("2. Restart your application")
        print("3. Test with: docker-compose restart vehicle-plate-api")
    else:
        print("\n⚠️  Some models failed to convert. Check errors above.")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Convert specific model
        model_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else None
        fix_openvino_model(model_path, output_dir)
    else:
        # Convert all models
        print("🔧 Converting all models to OpenVINO (fixed shapes)...\n")
        fix_all_models()