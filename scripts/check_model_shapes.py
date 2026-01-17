#!/usr/bin/env python3
"""
Check if OpenVINO models have dynamic or fixed shapes
"""

from pathlib import Path
from openvino.runtime import Core

def check_openvino_model(model_dir: str):
    """
    Check if an OpenVINO model has dynamic shapes
    
    Args:
        model_dir: Path to OpenVINO model directory
    """
    model_path = Path(model_dir)
    
    if not model_path.exists():
        print(f"❌ Model directory not found: {model_path}")
        return None
    
    # Find .xml file
    xml_files = list(model_path.glob("*.xml"))
    if not xml_files:
        print(f"❌ No .xml file found in {model_path}")
        return None
    
    xml_file = xml_files[0]
    
    print(f"\n{'='*60}")
    print(f"Checking: {model_path.name}")
    print(f"{'='*60}")
    
    try:
        # Initialize OpenVINO
        core = Core()
        
        # Read model
        model = core.read_model(str(xml_file))
        
        # Get input info
        input_layer = model.input(0)
        output_layer = model.output(0)
        
        # Check if shapes are dynamic
        input_shape = input_layer.shape
        output_shape = output_layer.shape
        
        input_is_dynamic = input_shape.is_dynamic
        output_is_dynamic = output_shape.is_dynamic
        
        print(f"Model file: {xml_file.name}")
        print(f"\nInput:")
        print(f"  Shape: {input_shape}")
        print(f"  Is Dynamic: {'❌ YES (PROBLEM!)' if input_is_dynamic else '✅ NO (GOOD)'}")
        
        print(f"\nOutput:")
        print(f"  Shape: {output_shape}")
        print(f"  Is Dynamic: {'❌ YES (PROBLEM!)' if output_is_dynamic else '✅ NO (GOOD)'}")
        
        # Overall status
        if input_is_dynamic or output_is_dynamic:
            print(f"\n❌ STATUS: DYNAMIC SHAPES DETECTED!")
            print(f"   This will cause the error you're seeing.")
            print(f"   Solution: Reconvert with fixed shapes using fix_dynamic_shapes.py")
            return False
        else:
            print(f"\n✅ STATUS: FIXED SHAPES - Model is OK!")
            return True
        
    except Exception as e:
        print(f"❌ Error checking model: {e}")
        import traceback
        traceback.print_exc()
        return None


def check_all_models():
    """Check all OpenVINO models in yolo_models directory"""
    
    models_dir = Path("yolo_models")
    
    if not models_dir.exists():
        print(f"❌ Directory not found: {models_dir}")
        return
    
    openvino_dirs = [
        "car_detection_openvino",
        "plate_detection_openvino",
        "character_detection_openvino"
    ]
    
    results = {}
    
    for dir_name in openvino_dirs:
        model_path = models_dir / dir_name
        result = check_openvino_model(str(model_path))
        results[dir_name] = result
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    
    ok_count = sum(1 for v in results.values() if v is True)
    problem_count = sum(1 for v in results.values() if v is False)
    error_count = sum(1 for v in results.values() if v is None)
    
    for name, result in results.items():
        if result is True:
            status = "✅ OK"
        elif result is False:
            status = "❌ DYNAMIC (NEEDS FIX)"
        else:
            status = "⚠️  ERROR"
        
        print(f"{name:<35} {status}")
    
    print(f"\n{'='*60}")
    print(f"Results: {ok_count} OK, {problem_count} need fixing, {error_count} errors")
    
    if problem_count > 0:
        print(f"\n❌ {problem_count} model(s) have dynamic shapes!")
        print(f"\n💡 To fix this issue, run:")
        print(f"   python fix_dynamic_shapes.py")
    elif ok_count == len(openvino_dirs):
        print(f"\n✅ All models are configured correctly!")
    
    return ok_count, problem_count, error_count


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Check specific model
        model_dir = sys.argv[1]
        check_openvino_model(model_dir)
    else:
        # Check all models
        print("🔍 Checking all OpenVINO models for dynamic shapes...\n")
        check_all_models()