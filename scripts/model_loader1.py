# app/core/model_loader.py
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
                
                self.is_loaded = True
                print(f"✅ OpenVINO model loaded: {model_file}")
                print(f"   Input shape: {self.input_layer.shape}")
                print(f"   Output shape: {self.output_layer.shape}")
                
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
            # Fallback to PyTorch (using ultralytics)
            from ultralytics import YOLO
            # Wrap YOLO in a compatible interface
            raise NotImplementedError("PyTorch backend wrapper not implemented yet")
        else:
            raise ValueError(f"Unknown backend: {backend}")