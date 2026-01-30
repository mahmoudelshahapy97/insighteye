# scripts/convert_models.py
"""
Script to convert YOLO PyTorch models to ONNX, OpenVINO, and TensorRT formats
"""
import argparse
from pathlib import Path
import subprocess
import sys


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


def convert_to_tensorrt(onnx_path: str, output_path: str = None, 
                       precision: str = "fp16", max_batch_size: int = 8,
                       workspace_size_mb: int = 2048):
    """
    Convert ONNX model to TensorRT format
    
    Args:
        onnx_path: Path to .onnx model file
        output_path: Output path for TensorRT engine (default: same name with .engine extension)
        precision: Precision mode (fp32, fp16, int8)
        max_batch_size: Maximum batch size
        workspace_size_mb: Workspace size in MB
    """
    try:
        import tensorrt as trt
        
        # Determine output path
        if output_path is None:
            output_path = str(Path(onnx_path).with_suffix('.engine'))
        
        print(f"🔄 Converting {onnx_path} to TensorRT ({precision})...")
        
        # Create logger
        TRT_LOGGER = trt.Logger(trt.Logger.WARNING)
        
        # Create builder
        builder = trt.Builder(TRT_LOGGER)
        
        # Create network
        network_flags = 1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH)
        network = builder.create_network(network_flags)
        
        # Parse ONNX
        parser = trt.OnnxParser(network, TRT_LOGGER)
        
        with open(onnx_path, 'rb') as model:
            if not parser.parse(model.read()):
                print("❌ Failed to parse ONNX model")
                for error in range(parser.num_errors):
                    print(parser.get_error(error))
                raise RuntimeError("ONNX parsing failed")
        
        # Create builder config
        config = builder.create_builder_config()
        
        # Set workspace size
        config.max_workspace_size = workspace_size_mb * (1 << 20)  # Convert MB to bytes
        
        # Set precision
        if precision == "fp16":
            if builder.platform_has_fast_fp16:
                config.set_flag(trt.BuilderFlag.FP16)
                print("   Using FP16 precision")
            else:
                print("   ⚠️ FP16 not supported, using FP32")
        elif precision == "int8":
            if builder.platform_has_fast_int8:
                config.set_flag(trt.BuilderFlag.INT8)
                print("   Using INT8 precision (requires calibration)")
                # Note: INT8 requires calibration data - implement if needed
            else:
                print("   ⚠️ INT8 not supported, using FP32")
        else:
            print("   Using FP32 precision")
        
        # Build engine
        print(f"   Building TensorRT engine (this may take a while)...")
        serialized_engine = builder.build_serialized_network(network, config)
        
        if serialized_engine is None:
            raise RuntimeError("Failed to build TensorRT engine")
        
        # Save engine
        with open(output_path, 'wb') as f:
            f.write(serialized_engine)
        
        print(f"✅ TensorRT engine saved to: {output_path}")
        print(f"   Precision: {precision.upper()}")
        print(f"   Max batch size: {max_batch_size}")
        print(f"   Workspace: {workspace_size_mb}MB")
        
        return output_path
        
    except ImportError:
        print("❌ TensorRT not installed")
        print("   Install with: pip install tensorrt")
        print("   Or use NVIDIA NGC TensorRT container")
        raise
    except Exception as e:
        print(f"❌ Error converting to TensorRT: {e}")
        raise


def convert_yolo_to_tensorrt_direct(model_path: str, output_path: str = None,
                                    precision: str = "fp16", max_batch_size: int = 8):
    """
    Convert YOLO PyTorch model directly to TensorRT (Ultralytics method)
    
    Args:
        model_path: Path to .pt model file
        output_path: Output path for engine
        precision: Precision mode (fp32, fp16, int8)
        max_batch_size: Maximum batch size
    """
    try:
        from ultralytics import YOLO
        
        # Load model
        model = YOLO(model_path)
        
        # Determine output path
        if output_path is None:
            output_path = str(Path(model_path).with_suffix('.engine'))
        
        # Set half precision flag
        half = (precision == "fp16")
        int8 = (precision == "int8")
        
        # Export to TensorRT
        print(f"🔄 Converting {model_path} to TensorRT (direct, {precision})...")
        model.export(
            format='engine',
            imgsz=640,
            half=half,
            int8=int8,
            batch=max_batch_size,
            workspace=4  # 4GB workspace
        )
        
        print(f"✅ TensorRT engine saved")
        return output_path
        
    except Exception as e:
        print(f"❌ Error converting to TensorRT: {e}")
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
    parser = argparse.ArgumentParser(description="Convert YOLO models to ONNX/OpenVINO/TensorRT")
    parser.add_argument("model_path", help="Path to PyTorch .pt model file")
    parser.add_argument("--format", choices=["onnx", "openvino", "tensorrt", "all"], 
                       default="all", help="Target format")
    parser.add_argument("--output-dir", help="Output directory (optional)")
    parser.add_argument("--direct", action="store_true",
                       help="Use direct conversion (faster, recommended)")
    parser.add_argument("--precision", choices=["fp32", "fp16", "int8"],
                       default="fp16", help="TensorRT precision mode")
    parser.add_argument("--batch-size", type=int, default=8,
                       help="Maximum batch size for TensorRT")
    parser.add_argument("--workspace", type=int, default=2048,
                       help="TensorRT workspace size in MB")
    
    args = parser.parse_args()
    
    model_path = Path(args.model_path)
    if not model_path.exists():
        print(f"❌ Error: Model file not found: {model_path}")
        return
    
    # Convert to ONNX (required for TensorRT if not using direct conversion)
    onnx_path = None
    if args.format in ["onnx", "tensorrt", "all"]:
        onnx_path = convert_to_onnx(str(model_path))
    
    # Convert to OpenVINO
    if args.format in ["openvino", "all"]:
        if args.direct:
            convert_yolo_to_openvino_direct(str(model_path), args.output_dir)
        else:
            if onnx_path is None:
                onnx_path = convert_to_onnx(str(model_path))
            convert_to_openvino(onnx_path, args.output_dir)
    
    # Convert to TensorRT
    if args.format in ["tensorrt", "all"]:
        if args.direct:
            convert_yolo_to_tensorrt_direct(
                str(model_path),
                precision=args.precision,
                max_batch_size=args.batch_size
            )
        else:
            if onnx_path is None:
                onnx_path = convert_to_onnx(str(model_path))
            convert_to_tensorrt(
                onnx_path,
                precision=args.precision,
                max_batch_size=args.batch_size,
                workspace_size_mb=args.workspace
            )
    
    print("\n✅ Conversion complete!")


def convert_all_models():
    """Convert all models in models directory to all formats"""
    models_dir = Path("models")
    
    models = [
        "fire.pt",
        "gender.pt",
        "people.pt"
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
            # # Convert to ONNX
            # print("\n--- ONNX Conversion ---")
            # onnx_path = convert_to_onnx(str(model_path))
            
            # # Convert to OpenVINO (direct method - faster)
            # print("\n--- OpenVINO Conversion ---")
            # output_dir = models_dir / (model_path.stem + "_openvino")
            # convert_yolo_to_openvino_direct(str(model_path), str(output_dir))
            
            # Convert to TensorRT (direct method - recommended)
            print("\n--- TensorRT Conversion ---")
            try:
                convert_yolo_to_tensorrt_direct(
                    str(model_path),
                    precision="fp16",
                    max_batch_size=8
                )
            except ImportError:
                print("⚠️ TensorRT not available, skipping TensorRT conversion")
            except Exception as e:
                print(f"⚠️ TensorRT conversion failed: {e}")
                print("   Trying two-step conversion (ONNX -> TensorRT)...")
                try:
                    convert_to_tensorrt(
                        onnx_path,
                        precision="fp16",
                        max_batch_size=8,
                        workspace_size_mb=2048
                    )
                except Exception as e2:
                    print(f"❌ Two-step conversion also failed: {e2}")
            
        except Exception as e:
            print(f"❌ Error converting {model_name}: {e}")
            continue
    
    print("\n" + "="*60)
    print("✅ All conversions complete!")
    print("="*60)
    print("\nGenerated files:")
    print("  ONNX:      models/*.onnx")
    print("  OpenVINO:  models/*_openvino/")
    print("  TensorRT:  models/*.engine")


if __name__ == "__main__":
    # Check if running with arguments
    if len(sys.argv) > 1:
        main()
    else:
        # Convert all models
        print("No arguments provided, converting all models...")
        convert_all_models()
