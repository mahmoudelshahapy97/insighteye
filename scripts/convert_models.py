# scripts/convert_models.py
"""
Script to convert YOLO PyTorch models to ONNX and OpenVINO formats
"""
import argparse
from pathlib import Path
import subprocess


def convert_to_onnx(model_path: str, output_path: str = None):
    """
    Convert PyTorch model to ONNX format
    
    Args:
        model_path: Path to .pt model file
        output_path: Optional output path (default: same name with .onnx extension)
    """
    try:
        from ultralytics import YOLO
        
        # Load model
        model = YOLO(model_path)
        
        # Determine output path
        if output_path is None:
            output_path = str(Path(model_path).with_suffix('.onnx'))
        
        # Export to ONNX
        print(f"🔄 Converting {model_path} to ONNX...")
        model.export(
            format='onnx',
            imgsz=640,
            dynamic=False,  # Set to True for dynamic input sizes
            simplify=True,   # Simplify ONNX model
            opset=12         # ONNX opset version
        )
        
        print(f"✅ ONNX model saved to: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"❌ Error converting to ONNX: {e}")
        raise


def convert_to_openvino(onnx_path: str, output_dir: str = None):
    """
    Convert ONNX model to OpenVINO format
    
    Args:
        onnx_path: Path to .onnx model file
        output_dir: Output directory for OpenVINO model
    """
    try:
        # Determine output directory
        if output_dir is None:
            output_dir = str(Path(onnx_path).stem) + "_openvino"
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Convert using OpenVINO Model Optimizer
        print(f"🔄 Converting {onnx_path} to OpenVINO...")
        
        cmd = [
            "mo",
            "--input_model", onnx_path,
            "--output_dir", output_dir,
            "--data_type", "FP32",  # Use FP16 for better performance on supported hardware
            "--model_name", Path(onnx_path).stem
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Error: {result.stderr}")
            raise RuntimeError(f"Model Optimizer failed: {result.stderr}")
        
        print(f"✅ OpenVINO model saved to: {output_dir}")
        print(f"   Files: {Path(onnx_path).stem}.xml, {Path(onnx_path).stem}.bin")
        return output_dir
        
    except Exception as e:
        print(f"❌ Error converting to OpenVINO: {e}")
        raise


def convert_yolo_to_openvino_direct(model_path: str, output_dir: str = None):
    """
    Convert YOLO PyTorch model directly to OpenVINO (Ultralytics method)
    
    Args:
        model_path: Path to .pt model file
        output_dir: Output directory
    """
    try:
        from ultralytics import YOLO
        
        # Load model
        model = YOLO(model_path)
        
        # Determine output directory
        if output_dir is None:
            output_dir = str(Path(model_path).stem) + "_openvino"
        
        # Export directly to OpenVINO
        print(f"🔄 Converting {model_path} to OpenVINO (direct)...")
        model.export(
            format='openvino',
            imgsz=640,
            half=False  # Set to True for FP16 precision
        )
        
        print(f"✅ OpenVINO model saved to: {output_dir}")
        return output_dir
        
    except Exception as e:
        print(f"❌ Error converting to OpenVINO: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Convert YOLO models to ONNX/OpenVINO")
    parser.add_argument("model_path", help="Path to PyTorch .pt model file")
    parser.add_argument("--format", choices=["onnx", "openvino", "both"], 
                       default="both", help="Target format")
    parser.add_argument("--output-dir", help="Output directory (optional)")
    parser.add_argument("--direct", action="store_true",
                       help="Use direct OpenVINO conversion (faster)")
    
    args = parser.parse_args()
    
    model_path = Path(args.model_path)
    if not model_path.exists():
        print(f"❌ Error: Model file not found: {model_path}")
        return
    
    # Convert to ONNX
    onnx_path = None
    if args.format in ["onnx", "both"]:
        onnx_path = convert_to_onnx(str(model_path))
    
    # Convert to OpenVINO
    if args.format in ["openvino", "both"]:
        if args.direct:
            # Direct conversion (faster, recommended for YOLO)
            convert_yolo_to_openvino_direct(str(model_path), args.output_dir)
        else:
            # Two-step conversion (ONNX -> OpenVINO)
            if onnx_path is None:
                onnx_path = convert_to_onnx(str(model_path))
            convert_to_openvino(onnx_path, args.output_dir)
    
    print("\n✅ Conversion complete!")


def convert_all_models():
    """Convert all models in yolo_models directory"""
    models_dir = Path("yolo_models")
    
    models = [
        "car_detection.pt",
        "plate_detection.pt",
        "character_detection.pt"
    ]
    
    for model_name in models:
        model_path = models_dir / model_name
        if not model_path.exists():
            print(f"⚠️ Model not found: {model_path}")
            continue
        
        print(f"\n{'='*60}")
        print(f"Converting {model_name}")
        print(f"{'='*60}")
        
        try:
            # Convert to ONNX
            onnx_path = convert_to_onnx(str(model_path))
            
            # Convert to OpenVINO (direct method - faster)
            output_dir = models_dir / (model_path.stem + "_openvino")
            convert_yolo_to_openvino_direct(str(model_path), str(output_dir))
            
        except Exception as e:
            print(f"❌ Error converting {model_name}: {e}")
            continue
    
    print("\n" + "="*60)
    print("✅ All conversions complete!")
    print("="*60)


if __name__ == "__main__":
    # Uncomment to convert all models at once
    # convert_all_models()
    
    # Or use command line arguments
    main()