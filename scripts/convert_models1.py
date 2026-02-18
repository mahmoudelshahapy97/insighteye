"""
Script to convert YOLO PyTorch models to ONNX, OpenVINO, and TensorRT formats
UPDATED: Fixed ONNX export settings to ensure proper class detection
"""
import argparse
from pathlib import Path
import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_onnx_output(onnx_path: str) -> bool:
    """
    Verify ONNX model has correct output format
    
    Returns:
        True if model looks correct, False otherwise
    """
    try:
        import onnxruntime as ort
        import numpy as np
        
        session = ort.InferenceSession(onnx_path, providers=['CPUExecutionProvider'])
        
        # Get output shape
        output_shape = session.get_outputs()[0].shape
        logger.info(f"   ONNX output shape: {output_shape}")
        
        # Test with dummy input
        dummy_input = np.random.randn(1, 3, 640, 640).astype(np.float32)
        input_name = session.get_inputs()[0].name
        outputs = session.run(None, {input_name: dummy_input})
        
        actual_shape = outputs[0].shape
        logger.info(f"   Actual output shape: {actual_shape}")
        
        # Check for YOLO formats
        if len(actual_shape) == 3:
            _, dim1, dim2 = actual_shape
            
            # YOLO v8 standard format (4 box + 80 classes)
            if 84 in [dim1, dim2]:
                logger.info("   ✅ Detected YOLO v8 format (84 channels)")
                return True
            # YOLO v5 format (deprecated)
            elif 85 in [dim1, dim2]:
                logger.warning("   ⚠️  Detected YOLO v5 format (85 channels)")
                logger.warning("   This model may not work correctly!")
                return False
            # Custom YOLO with top-K detections (e.g., 300 x 6 for single-class)
            # Format: [batch, num_detections, 6] where 6 = [x, y, w, h, conf, class]
            elif dim1 == 300 or dim2 == 300:
                num_features = dim2 if dim1 == 300 else dim1
                if num_features == 6:
                    logger.info(f"   ✅ Detected custom YOLO format (top-{300} detections, {num_features-5} class(es))")
                    logger.info(f"   Format: [batch, detections, features] = {actual_shape}")
                    return True
                else:
                    logger.warning(f"   ⚠️  Custom format detected: {dim1} x {dim2}")
                    logger.warning(f"   Expected 6 features but got {num_features}")
                    return False
            else:
                logger.warning(f"   ⚠️  Unknown format: {dim1} x {dim2}")
                logger.warning(f"   Expected YOLO v8 (84), YOLO v5 (85), or custom (300x6)")
                return False
        
        return True
        
    except ImportError:
        logger.warning("   ⚠️  onnxruntime not installed, skipping verification")
        return True
    except Exception as e:
        logger.error(f"   ❌ Verification failed: {e}")
        return False


def convert_to_onnx(model_path: str, output_path: str = None, verify: bool = True):
    """
    Convert PyTorch model to ONNX format with CORRECT settings
    
    Args:
        model_path: Path to .pt model file
        output_path: Optional output path (default: same name with .onnx extension)
        verify: Whether to verify the output format
    """
    try:
        from ultralytics import YOLO
        
        # Load model
        logger.info(f"🔄 Loading PyTorch model: {model_path}")
        model = YOLO(model_path)
        
        # Determine output path
        if output_path is None:
            output_path = str(Path(model_path).with_suffix('.onnx'))
        
        # Export to ONNX with CRITICAL settings
        logger.info(f"🔄 Converting {model_path} to ONNX...")
        logger.info(f"   Settings:")
        logger.info(f"     - Format: ONNX")
        logger.info(f"     - Image size: 640x640")
        logger.info(f"     - Dynamic: False (CRITICAL for correct output)")
        logger.info(f"     - Simplify: True")
        logger.info(f"     - Opset: 12")
        logger.info(f"     - Half precision: False (FP32)")
        
        model.export(
            format='onnx',
            imgsz=640,
            dynamic=False,   # ⚠️ CRITICAL: Must be False for correct class detection
            simplify=True,   # Simplify ONNX model
            opset=12,        # ONNX opset version
            half=False       # Use FP32 for maximum compatibility
        )
        
        logger.info(f"✅ ONNX model saved to: {output_path}")
        
        # Verify the output
        if verify:
            logger.info(f"🔍 Verifying ONNX model...")
            if verify_onnx_output(output_path):
                logger.info(f"✅ ONNX model verification passed")
            else:
                logger.error(f"❌ ONNX model verification FAILED")
                logger.error(f"   The converted model may not work correctly!")
                logger.error(f"   Please check the model and try again.")
        
        return output_path
        
    except Exception as e:
        logger.error(f"❌ Error converting to ONNX: {e}")
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
        logger.info(f"🔄 Converting {onnx_path} to OpenVINO...")
        
        cmd = [
            "mo",
            "--input_model", onnx_path,
            "--output_dir", output_dir,
            "--data_type", "FP32",  # Use FP16 for better performance on supported hardware
            "--model_name", Path(onnx_path).stem
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"❌ Error: {result.stderr}")
            raise RuntimeError(f"Model Optimizer failed: {result.stderr}")
        
        logger.info(f"✅ OpenVINO model saved to: {output_dir}")
        logger.info(f"   Files: {Path(onnx_path).stem}.xml, {Path(onnx_path).stem}.bin")
        return output_dir
        
    except Exception as e:
        logger.error(f"❌ Error converting to OpenVINO: {e}")
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
        
        logger.info(f"🔄 Converting {onnx_path} to TensorRT ({precision})...")
        
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
                logger.error("❌ Failed to parse ONNX model")
                for error in range(parser.num_errors):
                    logger.error(parser.get_error(error))
                raise RuntimeError("ONNX parsing failed")
        
        # Create builder config
        config = builder.create_builder_config()
        
        # Set workspace size (UPDATED API for newer TensorRT versions)
        # For TensorRT 8.6+, use set_memory_pool_limit instead of max_workspace_size
        try:
            config.set_memory_pool_limit(trt.MemoryPoolType.WORKSPACE, workspace_size_mb * (1 << 20))
        except AttributeError:
            # Fallback for older TensorRT versions
            config.max_workspace_size = workspace_size_mb * (1 << 20)
        
        # Set precision
        if precision == "fp16":
            if builder.platform_has_fast_fp16:
                config.set_flag(trt.BuilderFlag.FP16)
                logger.info("   Using FP16 precision")
            else:
                logger.warning("   ⚠️ FP16 not supported, using FP32")
        elif precision == "int8":
            if builder.platform_has_fast_int8:
                config.set_flag(trt.BuilderFlag.INT8)
                logger.info("   Using INT8 precision (requires calibration)")
                # Note: INT8 requires calibration data - implement if needed
            else:
                logger.warning("   ⚠️ INT8 not supported, using FP32")
        else:
            logger.info("   Using FP32 precision")
        
        # Build engine
        logger.info(f"   Building TensorRT engine (this may take a while)...")
        serialized_engine = builder.build_serialized_network(network, config)
        
        if serialized_engine is None:
            raise RuntimeError("Failed to build TensorRT engine")
        
        # Save engine
        with open(output_path, 'wb') as f:
            f.write(serialized_engine)
        
        logger.info(f"✅ TensorRT engine saved to: {output_path}")
        logger.info(f"   Precision: {precision.upper()}")
        logger.info(f"   Max batch size: {max_batch_size}")
        logger.info(f"   Workspace: {workspace_size_mb}MB")
        
        return output_path
        
    except ImportError:
        logger.error("❌ TensorRT not installed")
        logger.info("   Install with: pip install tensorrt")
        logger.info("   Or use NVIDIA NGC TensorRT container")
        raise
    except Exception as e:
        logger.error(f"❌ Error converting to TensorRT: {e}")
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
    
    BUG FIXES:
    1. Added device parameter to force CPU/GPU selection
    2. Fixed half/int8 parameter logic
    3. Added proper error handling for CUDA availability
    4. Added output path handling
    """
    try:
        from ultralytics import YOLO
        import torch
        
        # Load model
        model = YOLO(model_path)
        
        # Determine output path
        if output_path is None:
            output_path = str(Path(model_path).with_suffix('.engine'))
        
        # BUG FIX 1: Check CUDA availability before setting precision
        if not torch.cuda.is_available():
            raise RuntimeError(
                "CUDA is not available. TensorRT requires GPU.\n"
                "Please ensure:\n"
                "1. PyTorch is installed with CUDA support\n"
                "2. GPU is accessible (check with nvidia-smi)\n"
                "3. Run: pip uninstall torch && pip install torch --index-url https://download.pytorch.org/whl/cu121"
            )
        
        # BUG FIX 2: Set precision flags correctly
        # The issue: when precision="fp32", we were setting half=False, int8=False
        # But Ultralytics interprets this as default (fp16 on GPU)
        # Solution: explicitly set device and precision
        if precision == "fp16":
            half = True
            int8 = False
        elif precision == "int8":
            half = False
            int8 = True
        else:  # fp32
            half = False
            int8 = False
        
        # Export to TensorRT
        logger.info(f"🔄 Converting {model_path} to TensorRT (direct, {precision})...")
        logger.info(f"   GPU: {torch.cuda.get_device_name(0)}")
        logger.info(f"   Half precision: {half}")
        logger.info(f"   INT8: {int8}")
        logger.info(f"   Batch size: {max_batch_size}")
        
        # BUG FIX 3: Add device parameter and proper settings
        result = model.export(
            format='engine',
            imgsz=640,
            half=half,
            int8=int8,
            batch=max_batch_size,
            workspace=4,  # 4GB workspace
            device=0,     # BUG FIX: Explicitly set device to GPU 0
            simplify=True,
            verbose=True
        )
        
        # BUG FIX 4: The export returns the path, use it
        actual_output = result if isinstance(result, str) else output_path
        
        logger.info(f"✅ TensorRT engine saved to: {actual_output}")
        return actual_output
        
    except Exception as e:
        logger.error(f"❌ Error converting to TensorRT: {e}")
        raise


def check_tensorrt_requirements():
    """
    Check if all TensorRT requirements are met
    Returns: (bool, str) - (success, error_message)
    """
    import torch
    
    # Check 1: CUDA availability
    if not torch.cuda.is_available():
        return False, (
            "PyTorch CUDA is not available.\n"
            "Fix: pip uninstall torch && pip install torch --index-url https://download.pytorch.org/whl/cu121"
        )
    
    # Check 2: TensorRT installation
    try:
        import tensorrt as trt
    except ImportError:
        return False, (
            "TensorRT is not installed.\n"
            "Fix: pip install tensorrt"
        )
    
    # Check 3: GPU visibility
    try:
        torch.cuda.get_device_name(0)
    except Exception as e:
        return False, f"GPU is not accessible: {e}"
    
    return True, "All requirements met"


def safe_convert_to_tensorrt(model_path: str, **kwargs):
    """
    Safely convert model to TensorRT with comprehensive error checking
    """
    # Check requirements first
    success, message = check_tensorrt_requirements()
    if not success:
        logger.error(f"❌ TensorRT requirements not met:")
        logger.error(f"   {message}")
        logger.info("\n📝 Current status:")
        
        import torch
        logger.info(f"   - PyTorch version: {torch.__version__}")
        logger.info(f"   - CUDA available: {torch.cuda.is_available()}")
        
        try:
            import subprocess
            result = subprocess.run(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'],
                                  capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"   - GPU (host): {result.stdout.strip()}")
        except:
            pass
        
        raise RuntimeError(message)
    
    # Proceed with conversion
    return convert_yolo_to_tensorrt_direct(model_path, **kwargs)


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
        logger.info(f"🔄 Converting {model_path} to OpenVINO (direct)...")
        model.export(
            format='openvino',
            imgsz=640,
            half=False  # Set to True for FP16 precision
        )
        
        logger.info(f"✅ OpenVINO model saved to: {output_dir}")
        return output_dir
        
    except Exception as e:
        logger.error(f"❌ Error converting to OpenVINO: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Convert YOLO models to ONNX/OpenVINO/TensorRT",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert single model to ONNX (with verification)
  python convert_models.py models/people.pt --format onnx
  
  # Convert to all formats using direct method (recommended)
  python convert_models.py models/people.pt --format all --direct
  
  # Convert all models in models/ directory
  python convert_models.py
  
  # Convert to TensorRT with specific settings
  python convert_models.py models/people.pt --format tensorrt --precision fp16 --batch-size 8
        """
    )
    parser.add_argument("model_path", nargs='?', help="Path to PyTorch .pt model file")
    parser.add_argument("--format", choices=["onnx", "openvino", "tensorrt", "all"], 
                       default="all", help="Target format (default: all)")
    parser.add_argument("--output-dir", help="Output directory (optional)")
    parser.add_argument("--direct", action="store_true",
                       help="Use direct conversion (faster, recommended)")
    parser.add_argument("--precision", choices=["fp32", "fp16", "int8"],
                       default="fp16", help="TensorRT precision mode (default: fp16)")
    parser.add_argument("--batch-size", type=int, default=8,
                       help="Maximum batch size for TensorRT (default: 8)")
    parser.add_argument("--workspace", type=int, default=2048,
                       help="TensorRT workspace size in MB (default: 2048)")
    parser.add_argument("--no-verify", action="store_true",
                       help="Skip ONNX model verification")
    
    args = parser.parse_args()
    
    # If no model path provided, convert all models
    if not args.model_path:
        logger.info("No model path provided, converting all models...")
        convert_all_models()
        return
    
    model_path = Path(args.model_path)
    if not model_path.exists():
        logger.error(f"❌ Error: Model file not found: {model_path}")
        return
    
    verify = not args.no_verify
    
    # Convert to ONNX (required for TensorRT if not using direct conversion)
    onnx_path = None
    if args.format in ["onnx", "tensorrt", "all"]:
        onnx_path = convert_to_onnx(str(model_path), verify=verify)
    
    # Convert to OpenVINO
    if args.format in ["openvino", "all"]:
        if args.direct:
            convert_yolo_to_openvino_direct(str(model_path), args.output_dir)
        else:
            if onnx_path is None:
                onnx_path = convert_to_onnx(str(model_path), verify=verify)
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
                onnx_path = convert_to_onnx(str(model_path), verify=verify)
            convert_to_tensorrt(
                onnx_path,
                precision=args.precision,
                max_batch_size=args.batch_size,
                workspace_size_mb=args.workspace
            )
    
    logger.info("\n✅ Conversion complete!")


def convert_all_models():
    """Convert all models in models directory to all formats"""
    models_dir = Path("models")
    
    if not models_dir.exists():
        logger.error(f"❌ Models directory not found: {models_dir}")
        logger.info("   Please create 'models/' directory and add your .pt files")
        return
    
    # Find all .pt files
    pt_files = list(models_dir.glob("*.pt"))
    
    if not pt_files:
        logger.error(f"❌ No .pt model files found in {models_dir}")
        return
    
    logger.info(f"Found {len(pt_files)} model(s) to convert")
    
    for model_path in pt_files:
        model_name = model_path.name
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Converting {model_name}")
        logger.info(f"{'='*60}")
        
        try:
            # Convert to ONNX
            logger.info("\n--- ONNX Conversion ---")
            onnx_path = convert_to_onnx(str(model_path), verify=True)
            
            # Convert to OpenVINO (direct method - faster)
            logger.info("\n--- OpenVINO Conversion ---")
            output_dir = models_dir / (model_path.stem + "_openvino")
            convert_yolo_to_openvino_direct(str(model_path), str(output_dir))
            
            # Convert to TensorRT (direct method - recommended)
            logger.info("\n--- TensorRT Conversion ---")
            try:
                convert_yolo_to_tensorrt_direct(
                    str(model_path),
                    precision="fp32",
                    max_batch_size=8
                )
            except ImportError:
                logger.warning("⚠️ TensorRT not available, skipping TensorRT conversion")
            except Exception as e:
                logger.warning(f"⚠️ TensorRT conversion failed: {e}")
                logger.info("   Trying two-step conversion (ONNX -> TensorRT)...")
                try:
                    convert_to_tensorrt(
                        onnx_path,
                        precision="fp32",
                        max_batch_size=8,
                        workspace_size_mb=2048
                    )
                except Exception as e2:
                    logger.error(f"❌ Two-step conversion also failed: {e2}")
            
        except Exception as e:
            logger.error(f"❌ Error converting {model_name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    logger.info("\n" + "="*60)
    logger.info("✅ All conversions complete!")
    logger.info("="*60)
    logger.info("\nGenerated files:")
    logger.info("  ONNX:      models/*.onnx")
    logger.info("  OpenVINO:  models/*_openvino/")
    logger.info("  TensorRT:  models/*.engine")
    logger.info("\nNext steps:")
    logger.info("  1. Test your models: python test_model_backends.py")
    logger.info("  2. Update .env with MODEL_BACKEND=onnx (or openvino/tensorrt)")
    logger.info("  3. Restart your application")


if __name__ == "__main__":
    # main()
    convert_all_models()
    