#!/usr/bin/env python3
"""
TensorRT Conversion Workaround for CUDA Error 35
This script uses PyTorch's CUDA context to initialize before TensorRT attempts to
"""
import torch
import os
import sys

def fix_cuda_context():
    """Initialize CUDA context before TensorRT tries to"""
    print("=" * 60)
    print("CUDA Error 35 Workaround - Pre-initializing CUDA")
    print("=" * 60)
    
    # Force CUDA initialization
    if torch.cuda.is_available():
        print(f"✓ CUDA available: {torch.cuda.get_device_name(0)}")
        
        # Create a dummy tensor on GPU to force CUDA context creation
        dummy = torch.zeros(1).cuda()
        print(f"✓ CUDA context initialized on device 0")
        
        # Synchronize to ensure context is fully initialized
        torch.cuda.synchronize()
        print(f"✓ CUDA context synchronized")
        
        # Get memory info to further ensure CUDA is working
        mem_allocated = torch.cuda.memory_allocated(0) / 1024**2
        mem_cached = torch.cuda.memory_reserved(0) / 1024**2
        print(f"✓ GPU Memory: {mem_allocated:.1f} MB allocated, {mem_cached:.1f} MB cached")
        
        return True
    else:
        print("✗ CUDA not available")
        return False


def convert_with_workaround(model_path: str, precision: str = "fp16"):
    """Convert model with CUDA context pre-initialized"""
    from ultralytics import YOLO
    
    print(f"\n{'='*60}")
    print(f"Converting {model_path}")
    print(f"{'='*60}\n")
    
    # Load model
    model = YOLO(model_path)
    
    # Set precision
    half = (precision == "fp16")
    int8 = (precision == "int8")
    
    print(f"Settings:")
    print(f"  - Precision: {precision.upper()}")
    print(f"  - Half: {half}")
    print(f"  - INT8: {int8}")
    print(f"  - Device: cuda:0")
    print()
    
    try:
        # Export with explicit settings
        result = model.export(
            format='engine',
            imgsz=640,
            half=half,
            int8=int8,
            batch=1,  # Use batch=1 to reduce memory pressure
            workspace=4,
            device=0,
            simplify=True,
            verbose=True
        )
        
        print(f"\n✅ Success! TensorRT engine created: {result}")
        return result
        
    except Exception as e:
        print(f"\n❌ Failed: {e}")
        return None


def main():
    """Main conversion function with workaround"""
    
    # Step 1: Pre-initialize CUDA context
    if not fix_cuda_context():
        print("\n❌ CUDA initialization failed. Cannot proceed.")
        sys.exit(1)
    
    # Step 2: Convert models
    models = ['models/gender.pt', 'models/people.pt', 'models/fire.pt']
    
    print("\n" + "="*60)
    print("Starting TensorRT conversions with CUDA context active")
    print("="*60)
    
    results = {}
    for model_path in models:
        result = convert_with_workaround(model_path, precision="fp16")
        model_name = os.path.basename(model_path)
        results[model_name] = result is not None
    
    # Summary
    print("\n" + "="*60)
    print("Conversion Summary")
    print("="*60)
    for model, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{model}: {status}")
    
    print("\n" + "="*60)
    
    # If all failed, provide alternative
    if not any(results.values()):
        print("\n⚠️  All TensorRT conversions failed.")
        print("\nRECOMMENDATION: Use ONNX backend instead")
        print("  1. Update .env: MODEL_BACKEND=onnx")
        print("  2. Restart: docker-compose restart app")
        print("  3. ONNX provides 2-3x speedup and works perfectly!")
    

if __name__ == "__main__":
    main()