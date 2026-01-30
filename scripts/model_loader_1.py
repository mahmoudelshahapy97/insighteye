# app/services/model_loader.py
import numpy as np
import cv2
from typing import List, Tuple, Optional, Dict
from pathlib import Path
from enum import Enum
import onnxruntime as ort


class ModelBackend(str, Enum):
    PYTORCH = "pytorch"
    ONNX = "onnx"
    OPENVINO = "openvino"
    TENSORRT = "tensorrt"


class BaseModelLoader:
    """Base class for model loading"""
    
    def __init__(self, model_path: str, confidence_threshold: float = 0.5):
        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        self.model = None
        self.is_loaded = False
    
    def load_model(self):
        raise NotImplementedError
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        raise NotImplementedError
    
    def preprocess(self, image: np.ndarray, input_size: Tuple[int, int] = (640, 640)) -> np.ndarray:
        """Standard YOLO preprocessing"""
        # Resize
        img_resized = cv2.resize(image, input_size)
        
        # Convert BGR to RGB
        img_rgb = cv2.cvtColor(img_resized, cv2.COLOR_BGR2RGB)
        
        # Normalize to [0, 1]
        img_normalized = img_rgb.astype(np.float32) / 255.0
        
        # Transpose to CHW format
        img_chw = img_normalized.transpose(2, 0, 1)
        
        # Add batch dimension
        img_batch = np.expand_dims(img_chw, axis=0)
        
        return img_batch
    
    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640)) -> List[Dict]:
        """Standard YOLO postprocessing"""
        raise NotImplementedError
    
    def unload_model(self):
        self.model = None
        self.is_loaded = False


class ONNXModelLoader(BaseModelLoader):
    """ONNX Runtime model loader"""
    
    def load_model(self):
        if not self.is_loaded:
            try:
                # Create ONNX Runtime session
                providers = ['CPUExecutionProvider']
                
                # Try to use CUDA if available
                if ort.get_device() == 'GPU':
                    providers = ['CUDAExecutionProvider', 'CPUExecutionProvider']
                
                self.model = ort.InferenceSession(
                    self.model_path,
                    providers=providers
                )
                
                self.input_name = self.model.get_inputs()[0].name
                self.output_names = [output.name for output in self.model.get_outputs()]
                
                self.is_loaded = True
                print(f"✅ ONNX model loaded: {self.model_path}")
                print(f"   Input: {self.input_name}")
                print(f"   Outputs: {self.output_names}")
                
            except Exception as e:
                print(f"❌ Error loading ONNX model: {e}")
                raise
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        """Run inference"""
        if not self.is_loaded:
            self.load_model()
        
        # Preprocess
        input_tensor = self.preprocess(image)
        
        # Run inference
        outputs = self.model.run(
            self.output_names,
            {self.input_name: input_tensor}
        )
        
        return outputs[0]
    
    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640),
                   iou_threshold: float = 0.45) -> List[Dict]:
        """
        Postprocess YOLO outputs
        
        Args:
            outputs: Model outputs (1, num_classes + 5, 8400) or (1, 8400, num_classes + 5)
            original_shape: Original image shape (H, W)
            input_size: Model input size
            iou_threshold: NMS IOU threshold
        
        Returns:
            List of detections with bbox, confidence, class_id
        """
        # Handle different output formats
        if len(outputs.shape) == 3:
            if outputs.shape[1] > outputs.shape[2]:
                # Format: (1, num_classes + 5, 8400) -> transpose to (1, 8400, num_classes + 5)
                outputs = outputs.transpose(0, 2, 1)
        
        # Remove batch dimension
        predictions = outputs[0]  # (8400, num_classes + 5)
        
        # Extract boxes, scores, and class predictions
        boxes = predictions[:, :4]  # (8400, 4) - x_center, y_center, width, height
        scores = predictions[:, 4:]  # (8400, num_classes)
        
        # Get class with highest score
        class_ids = np.argmax(scores, axis=1)
        confidences = np.max(scores, axis=1)
        
        # Filter by confidence
        mask = confidences > self.confidence_threshold
        boxes = boxes[mask]
        confidences = confidences[mask]
        class_ids = class_ids[mask]
        
        # Convert from center format to corner format
        boxes_xyxy = self._xywh_to_xyxy(boxes)
        
        # Scale boxes to original image size
        scale_x = original_shape[1] / input_size[0]
        scale_y = original_shape[0] / input_size[1]
        
        boxes_xyxy[:, [0, 2]] *= scale_x
        boxes_xyxy[:, [1, 3]] *= scale_y
        
        # Apply NMS
        indices = self._nms(boxes_xyxy, confidences, iou_threshold)
        
        # Prepare detections
        detections = []
        for idx in indices:
            x1, y1, x2, y2 = boxes_xyxy[idx]
            detections.append({
                'bbox': [int(x1), int(y1), int(x2), int(y2)],
                'confidence': float(confidences[idx]),
                'class_id': int(class_ids[idx])
            })
        
        return detections
    
    def _xywh_to_xyxy(self, boxes: np.ndarray) -> np.ndarray:
        """Convert boxes from (x_center, y_center, width, height) to (x1, y1, x2, y2)"""
        boxes_xyxy = np.copy(boxes)
        boxes_xyxy[:, 0] = boxes[:, 0] - boxes[:, 2] / 2  # x1
        boxes_xyxy[:, 1] = boxes[:, 1] - boxes[:, 3] / 2  # y1
        boxes_xyxy[:, 2] = boxes[:, 0] + boxes[:, 2] / 2  # x2
        boxes_xyxy[:, 3] = boxes[:, 1] + boxes[:, 3] / 2  # y2
        return boxes_xyxy
    
    def _nms(self, boxes: np.ndarray, scores: np.ndarray, iou_threshold: float) -> List[int]:
        """Non-Maximum Suppression"""
        x1 = boxes[:, 0]
        y1 = boxes[:, 1]
        x2 = boxes[:, 2]
        y2 = boxes[:, 3]
        
        areas = (x2 - x1) * (y2 - y1)
        order = scores.argsort()[::-1]
        
        keep = []
        while order.size > 0:
            i = order[0]
            keep.append(i)
            
            xx1 = np.maximum(x1[i], x1[order[1:]])
            yy1 = np.maximum(y1[i], y1[order[1:]])
            xx2 = np.minimum(x2[i], x2[order[1:]])
            yy2 = np.minimum(y2[i], y2[order[1:]])
            
            w = np.maximum(0.0, xx2 - xx1)
            h = np.maximum(0.0, yy2 - yy1)
            inter = w * h
            
            iou = inter / (areas[i] + areas[order[1:]] - inter)
            
            inds = np.where(iou <= iou_threshold)[0]
            order = order[inds + 1]
        
        return keep


class TensorRTModelLoader(BaseModelLoader):
    """TensorRT model loader for high-performance GPU inference"""
    
    def load_model(self):
        if not self.is_loaded:
            try:
                import tensorrt as trt
                import pycuda.driver as cuda
                import pycuda.autoinit  # Automatically initializes CUDA
                
                # Create TensorRT logger
                self.trt_logger = trt.Logger(trt.Logger.WARNING)
                
                # Load serialized engine
                model_path = Path(self.model_path)
                
                # Support both .engine and .trt extensions
                if model_path.is_file():
                    engine_file = model_path
                else:
                    # Try common extensions
                    for ext in ['.engine', '.trt']:
                        potential_file = model_path.with_suffix(ext)
                        if potential_file.exists():
                            engine_file = potential_file
                            break
                    else:
                        raise FileNotFoundError(f"No TensorRT engine file found for {model_path}")
                
                print(f"🔄 Loading TensorRT engine from {engine_file}")
                
                with open(engine_file, 'rb') as f:
                    runtime = trt.Runtime(self.trt_logger)
                    self.engine = runtime.deserialize_cuda_engine(f.read())
                
                if self.engine is None:
                    raise RuntimeError("Failed to deserialize TensorRT engine")
                
                # Create execution context
                self.context = self.engine.create_execution_context()
                
                # Allocate buffers
                self.inputs = []
                self.outputs = []
                self.bindings = []
                self.stream = cuda.Stream()
                
                for i in range(self.engine.num_bindings):
                    binding = self.engine[i]
                    size = trt.volume(self.engine.get_binding_shape(i))
                    dtype = trt.nptype(self.engine.get_binding_dtype(i))
                    
                    # Allocate host and device buffers
                    host_mem = cuda.pagelocked_empty(size, dtype)
                    device_mem = cuda.mem_alloc(host_mem.nbytes)
                    
                    self.bindings.append(int(device_mem))
                    
                    if self.engine.binding_is_input(i):
                        self.inputs.append({'host': host_mem, 'device': device_mem})
                        self.input_shape = self.engine.get_binding_shape(i)
                    else:
                        self.outputs.append({'host': host_mem, 'device': device_mem})
                        self.output_shape = self.engine.get_binding_shape(i)
                
                self.is_loaded = True
                print(f"✅ TensorRT model loaded: {engine_file}")
                print(f"   Input shape: {self.input_shape}")
                print(f"   Output shape: {self.output_shape}")
                print(f"   GPU: {cuda.Device(0).name()}")
                
            except ImportError as e:
                print(f"❌ TensorRT not installed: {e}")
                print("   Install with: pip install tensorrt pycuda")
                raise
            except Exception as e:
                print(f"❌ Error loading TensorRT model: {e}")
                raise
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        """Run inference using TensorRT"""
        if not self.is_loaded:
            self.load_model()
        
        import pycuda.driver as cuda
        
        # Preprocess
        input_tensor = self.preprocess(image)
        
        # Ensure correct shape
        input_tensor = input_tensor.astype(np.float32).ravel()
        
        # Copy input to device
        np.copyto(self.inputs[0]['host'], input_tensor)
        cuda.memcpy_htod_async(
            self.inputs[0]['device'],
            self.inputs[0]['host'],
            self.stream
        )
        
        # Run inference
        self.context.execute_async_v2(
            bindings=self.bindings,
            stream_handle=self.stream.handle
        )
        
        # Copy output from device
        cuda.memcpy_dtoh_async(
            self.outputs[0]['host'],
            self.outputs[0]['device'],
            self.stream
        )
        
        # Synchronize
        self.stream.synchronize()
        
        # Reshape output
        output = self.outputs[0]['host'].reshape(self.output_shape)
        
        return output
    
    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640),
                   iou_threshold: float = 0.45) -> List[Dict]:
        """Postprocess TensorRT outputs (same as ONNX)"""
        # Reuse ONNX postprocessing logic
        loader = ONNXModelLoader("", self.confidence_threshold)
        return loader.postprocess(outputs, original_shape, input_size, iou_threshold)
    
    def unload_model(self):
        """Cleanup TensorRT resources"""
        if hasattr(self, 'context'):
            del self.context
        if hasattr(self, 'engine'):
            del self.engine
        if hasattr(self, 'stream'):
            del self.stream
        self.model = None
        self.is_loaded = False


class OpenVINOModelLoader(BaseModelLoader):
    """OpenVINO model loader"""
    
    def load_model(self):
        if not self.is_loaded:
            try:
                from openvino.runtime import Core
                
                # Initialize OpenVINO
                self.core = Core()
                
                # Determine model path
                model_path = Path(self.model_path)
                if model_path.is_dir():
                    # Find .xml file in directory
                    xml_files = list(model_path.glob("*.xml"))
                    if not xml_files:
                        raise FileNotFoundError(f"No .xml file found in {model_path}")
                    model_file = xml_files[0]
                else:
                    model_file = model_path
                
                # Load model
                self.model = self.core.read_model(model=str(model_file))
                self.compiled_model = self.core.compile_model(
                    model=self.model,
                    device_name="CPU"  # or "GPU" if available
                )
                
                # Get input/output info
                self.input_layer = self.compiled_model.input(0)
                self.output_layer = self.compiled_model.output(0)
                
                # Get shapes safely (handle dynamic shapes)
                try:
                    input_shape = self.input_layer.shape
                    if input_shape.is_dynamic:
                        input_shape_str = str(input_shape)
                    else:
                        input_shape_str = str(list(input_shape))
                except:
                    input_shape_str = "dynamic"
                
                try:
                    output_shape = self.output_layer.shape
                    if output_shape.is_dynamic:
                        output_shape_str = str(output_shape)
                    else:
                        output_shape_str = str(list(output_shape))
                except:
                    output_shape_str = "dynamic"
                
                self.is_loaded = True
                print(f"✅ OpenVINO model loaded: {model_file}")
                print(f"   Input shape: {input_shape_str}")
                print(f"   Output shape: {output_shape_str}")
                
            except Exception as e:
                print(f"❌ Error loading OpenVINO model: {e}")
                raise
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        """Run inference"""
        if not self.is_loaded:
            self.load_model()
        
        # Preprocess
        input_tensor = self.preprocess(image)
        
        # Run inference
        result = self.compiled_model([input_tensor])
        outputs = result[self.output_layer]
        
        return outputs
    
    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640),
                   iou_threshold: float = 0.45) -> List[Dict]:
        """Postprocess outputs (same as ONNX)"""
        # Reuse ONNX postprocessing logic
        loader = ONNXModelLoader("", self.confidence_threshold)
        return loader.postprocess(outputs, original_shape, input_size, iou_threshold)


class PyTorchModelLoader(BaseModelLoader):
    """PyTorch/Ultralytics YOLO model loader"""
    
    def load_model(self):
        if not self.is_loaded:
            try:
                from ultralytics import YOLO
                import torch
                
                # Load YOLO model
                self.model = YOLO(self.model_path)
                
                # Set device
                device = 'cuda' if torch.cuda.is_available() else 'cpu'
                self.model.to(device)
                
                self.is_loaded = True
                print(f"✅ PyTorch model loaded: {self.model_path}")
                print(f"   Device: {device.upper()}")
                
            except Exception as e:
                print(f"❌ Error loading PyTorch model: {e}")
                raise
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        """Run inference using YOLO"""
        if not self.is_loaded:
            self.load_model()
        
        # Run YOLO inference
        results = self.model(image, conf=self.confidence_threshold, verbose=False)
        
        # Extract predictions in YOLO format
        # Convert to numpy array format: [batch, num_detections, 5+num_classes]
        if len(results) > 0:
            result = results[0]
            boxes = result.boxes
            
            if boxes is not None and len(boxes) > 0:
                # Get xyxy, conf, cls
                xyxy = boxes.xyxy.cpu().numpy()  # [N, 4]
                conf = boxes.conf.cpu().numpy()  # [N]
                cls = boxes.cls.cpu().numpy()    # [N]
                
                # Combine into format expected by postprocess
                # Format: [x1, y1, x2, y2, conf, cls]
                detections = np.column_stack([xyxy, conf, cls])  # [N, 6]
                
                # Add batch dimension and transpose to match YOLO output format
                # YOLO output format is typically [1, num_classes+5, num_detections]
                # But we'll convert in postprocess, so return as [1, N, 6]
                output = np.expand_dims(detections, axis=0)  # [1, N, 6]
                
                return output
        
        # Return empty predictions
        return np.zeros((1, 0, 6), dtype=np.float32)
    
    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640),
                   iou_threshold: float = 0.45) -> List[Dict]:
        """
        Postprocess PyTorch YOLO outputs
        
        Args:
            outputs: Model outputs in format [1, N, 6] where 6 = [x1,y1,x2,y2,conf,cls]
            original_shape: Original image shape (H, W)
            input_size: Model input size
            iou_threshold: NMS IOU threshold (not used, YOLO already applied NMS)
        
        Returns:
            List of detections with bbox, confidence, class_id
        """
        if outputs.shape[1] == 0:
            return []
        
        # Remove batch dimension
        detections_array = outputs[0]  # [N, 6]
        
        # Parse detections
        detections = []
        for det in detections_array:
            x1, y1, x2, y2, conf, cls = det
            
            detections.append({
                'bbox': [int(x1), int(y1), int(x2), int(y2)],
                'confidence': float(conf),
                'class_id': int(cls)
            })
        
        return detections


class ModelFactory:
    """Factory for creating model loaders"""
    
    @staticmethod
    def create_loader(model_path: str, backend: ModelBackend = None,
                     confidence_threshold: float = 0.5) -> BaseModelLoader:
        """
        Create appropriate model loader based on file extension or backend
        
        Args:
            model_path: Path to model file or directory
            backend: Force specific backend
            confidence_threshold: Detection confidence threshold
        
        Returns:
            Model loader instance
        """
        path = Path(model_path)
        
        # Auto-detect backend if not specified
        if backend is None:
            if path.suffix == '.onnx':
                backend = ModelBackend.ONNX
            elif path.suffix == '.pt':
                backend = ModelBackend.PYTORCH
            elif path.suffix in ['.engine', '.trt']:
                backend = ModelBackend.TENSORRT
            elif path.is_dir() or path.suffix == '.xml':
                backend = ModelBackend.OPENVINO
            else:
                raise ValueError(f"Cannot determine backend for {model_path}")
        
        # Create loader
        if backend == ModelBackend.ONNX:
            return ONNXModelLoader(model_path, confidence_threshold)
        elif backend == ModelBackend.OPENVINO:
            return OpenVINOModelLoader(model_path, confidence_threshold)
        elif backend == ModelBackend.PYTORCH:
            return PyTorchModelLoader(model_path, confidence_threshold)
        elif backend == ModelBackend.TENSORRT:
            return TensorRTModelLoader(model_path, confidence_threshold)
        else:
            raise ValueError(f"Unknown backend: {backend}")
