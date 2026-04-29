# app/services/model_loader.py
import numpy as np
import cv2
from typing import List, Tuple, Optional, Dict
from pathlib import Path
from enum import Enum
import onnxruntime as ort
import logging

logger = logging.getLogger(__name__)


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
        self.use_gpu = True
    
    def load_model(self):
        raise NotImplementedError
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        raise NotImplementedError
         
    def preprocess(self, image: np.ndarray, input_size: Tuple[int, int] = (640, 640)) -> np.ndarray:
        """
        YOLO preprocessing with letterbox (maintains aspect ratio)
        Supports GPU acceleration via OpenCV CUDA
        """
        # Get original dimensions
        orig_h, orig_w = image.shape[:2]
        target_h, target_w = input_size
        
        # Calculate scale to fit image within target size (letterbox)
        scale = min(target_w / orig_w, target_h / orig_h)
        
        # Calculate new dimensions
        new_w = int(orig_w * scale)
        new_h = int(orig_h * scale)
        
        if not self.use_gpu:
            # CPU path
            resized = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
            padded = np.full((target_h, target_w, 3), 114, dtype=np.uint8)
            pad_x = (target_w - new_w) // 2
            pad_y = (target_h - new_h) // 2
            padded[pad_y:pad_y+new_h, pad_x:pad_x+new_w] = resized
            img_rgb = cv2.cvtColor(padded, cv2.COLOR_BGR2RGB)
            img_normalized = img_rgb.astype(np.float32) / 255.0
        else:
            try:
                # GPU path
                # Upload to GPU
                gpu_image = cv2.cuda_GpuMat()
                gpu_image.upload(image)
                
                # Resize on GPU
                gpu_resized = cv2.cuda.resize(
                    gpu_image, 
                    (new_w, new_h), 
                    interpolation=cv2.INTER_LINEAR
                )
                
                # Create padded image on GPU
                gpu_padded = cv2.cuda_GpuMat(target_h, target_w, cv2.CV_8UC3)
                gpu_padded.setTo((114, 114, 114))
                
                # Calculate padding offsets (center the image)
                pad_x = (target_w - new_w) // 2
                pad_y = (target_h - new_h) // 2
                
                # Copy resized image to center of padded image
                # Note: Direct ROI operations on GpuMat require downloading
                resized_cpu = gpu_resized.download()
                padded_cpu = gpu_padded.download()
                padded_cpu[pad_y:pad_y+new_h, pad_x:pad_x+new_w] = resized_cpu
                gpu_padded.upload(padded_cpu)
                
                # Convert BGR to RGB on GPU
                gpu_rgb = cv2.cuda.cvtColor(gpu_padded, cv2.COLOR_BGR2RGB)
                
                # Download for normalization (GPU doesn't have native division)
                img_rgb = gpu_rgb.download()
                img_normalized = img_rgb.astype(np.float32) / 255.0
                
            except Exception as e:
                logger.warning(f"GPU preprocessing failed, falling back to CPU: {e}")
                # CPU fallback
                resized = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
                padded = np.full((target_h, target_w, 3), 114, dtype=np.uint8)
                pad_x = (target_w - new_w) // 2
                pad_y = (target_h - new_h) // 2
                padded[pad_y:pad_y+new_h, pad_x:pad_x+new_w] = resized
                img_rgb = cv2.cvtColor(padded, cv2.COLOR_BGR2RGB)
                img_normalized = img_rgb.astype(np.float32) / 255.0
        
        # Transpose to CHW format
        img_chw = img_normalized.transpose(2, 0, 1)
        
        # Add batch dimension
        img_batch = np.expand_dims(img_chw, axis=0)
        
        # Store preprocessing info for postprocessing
        self._preprocess_info = {
            'scale': scale,
            'pad_x': pad_x,
            'pad_y': pad_y,
            'new_w': new_w,
            'new_h': new_h
        }
        
        return img_batch

    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640)) -> List[Dict]:
        """Standard YOLO postprocessing"""
        raise NotImplementedError
    
    def unload_model(self):
        self.model = None
        self.is_loaded = False


class ONNXModelLoader(BaseModelLoader):
    """ONNX Runtime model loader with support for multiple output formats"""
    
    def __init__(self, model_path: str, confidence_threshold: float = 0.5):
        super().__init__(model_path, confidence_threshold)
        self.output_format = None  # Will be detected on first run
    
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
                
                # Get output shape to determine model type
                output_shape = self.model.get_outputs()[0].shape
                logger.info(f"ONNX model output shape: {output_shape}")
                
                # Detect output format
                self._detect_output_format(output_shape)
                
                self.is_loaded = True
                print(f"✅ ONNX model loaded: {self.model_path}")
                print(f"   Input: {self.input_name}")
                print(f"   Outputs: {self.output_names}")
                print(f"   Output shape: {output_shape}")
                print(f"   Detected format: {self.output_format}")
                
            except Exception as e:
                print(f"❌ Error loading ONNX model: {e}")
                raise
    
    def _detect_output_format(self, shape):
        """
        Detect ONNX output format:
        - "yolov8_full": (1, 84, 8400) - Full YOLO v8 format
        - "yolov8_simplified": (1, 300, 6) - Simplified top-K format (dynamic=False)
        - "yolov5": (1, 25200, 85) - Legacy YOLO v5 format
        """
        if len(shape) != 3:
            self.output_format = "unknown"
            return
        
        _, dim1, dim2 = shape
        
        # YOLOv8 simplified format (what you get with dynamic=False)
        # Format: [batch, num_detections, 6] where 6 = [x1, y1, x2, y2, conf, class]
        if (dim1 <= 300 and dim2 == 6) or (dim1 == 6 and dim2 <= 300):
            self.output_format = "yolov8_simplified"
            logger.info("   Detected YOLOv8 SIMPLIFIED format (top-K detections)")
        
        # YOLOv8 full format
        # Format: [batch, 84, 8400] or [batch, 8400, 84]
        elif 84 in [dim1, dim2]:
            self.output_format = "yolov8_full"
            logger.info("   Detected YOLOv8 FULL format")
        
        # YOLOv5 format (legacy)
        elif 85 in [dim1, dim2]:
            self.output_format = "yolov5"
            logger.warning("   Detected YOLOv5 format (legacy)")
        
        else:
            self.output_format = "unknown"
            logger.warning(f"   Unknown format: {dim1} x {dim2}")
    
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
        Postprocess YOLO outputs - handles MULTIPLE formats
        
        Supports:
        1. YOLOv8 simplified: (1, 300, 6) - [x1, y1, x2, y2, conf, class]
        2. YOLOv8 full: (1, 84, 8400) - [x, y, w, h, class_probs...]
        3. YOLOv5: (1, 25200, 85) - [x, y, w, h, conf, class_probs...]
        """
        logger.debug(f"Raw output shape: {outputs.shape}")
        
        # Auto-detect format if not already detected
        if self.output_format is None:
            self._detect_output_format(outputs.shape)
        
        # Route to appropriate postprocessing
        if self.output_format == "yolov8_simplified":
            return self._postprocess_simplified(outputs, original_shape, input_size)
        elif self.output_format == "yolov8_full":
            return self._postprocess_full_yolov8(outputs, original_shape, input_size, iou_threshold)
        elif self.output_format == "yolov5":
            return self._postprocess_yolov5(outputs, original_shape, input_size, iou_threshold)
        else:
            logger.error(f"Unknown output format: {outputs.shape}")
            return []
    
    def _postprocess_simplified(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                               input_size: Tuple[int, int]) -> List[Dict]:
        """
        Postprocess YOLOv8 simplified format (dynamic=False export)
        
        Format: [batch, num_detections, 6]
        Where 6 = [x1, y1, x2, y2, confidence, class_id]
        
        CRITICAL: Coordinates are already in xyxy format and scaled to input size!
        """
        logger.debug(f"Using SIMPLIFIED postprocessing")
        
        # Remove batch dimension
        if len(outputs.shape) == 3:
            detections = outputs[0]  # Shape: (num_detections, 6)
        else:
            detections = outputs
        
        # Get preprocessing info
        preprocess_info = getattr(self, '_preprocess_info', None)
        if not preprocess_info:
            # Fallback to simple scaling (less accurate)
            logger.warning("No preprocessing info available, using simple scaling")
            scale = 1.0
            pad_x = 0
            pad_y = 0
        else:
            scale = preprocess_info['scale']
            pad_x = preprocess_info['pad_x']
            pad_y = preprocess_info['pad_y']
        
        results = []
        for det in detections:
            x1, y1, x2, y2, conf, cls = det
            
            # Filter by confidence
            if conf < self.confidence_threshold:
                continue
            
            # Convert coordinates back to original image space
            # 1. Remove padding
            x1 = x1 - pad_x
            y1 = y1 - pad_y
            x2 = x2 - pad_x
            y2 = y2 - pad_y
            
            # 2. Scale back to original size
            x1 = x1 / scale
            y1 = y1 / scale
            x2 = x2 / scale
            y2 = y2 / scale
            
            # 3. Clip to image boundaries
            orig_h, orig_w = original_shape
            x1 = max(0, min(x1, orig_w))
            y1 = max(0, min(y1, orig_h))
            x2 = max(0, min(x2, orig_w))
            y2 = max(0, min(y2, orig_h))
            
            # Validate box
            if x2 <= x1 or y2 <= y1:
                continue
            
            results.append({
                'bbox': [int(x1), int(y1), int(x2), int(y2)],
                'confidence': float(conf),
                'class_id': int(cls)
            })
        
        logger.info(f"Simplified format: {len(results)} detections")
        return results
    
    def _postprocess_full_yolov8(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                                 input_size: Tuple[int, int], iou_threshold: float) -> List[Dict]:
        """
        Postprocess YOLOv8 full format
        
        Format: [batch, 84, 8400] or [batch, 8400, 84]
        Where 84 = 4 (box coords) + 80 (class scores)
        Box coords are [x_center, y_center, width, height]
        """
        logger.debug(f"Using FULL YOLOv8 postprocessing")
        
        # Transpose if needed
        if len(outputs.shape) == 3:
            batch_size, dim1, dim2 = outputs.shape
            if dim1 < dim2:  # (1, 84, 8400) -> (1, 8400, 84)
                outputs = outputs.transpose(0, 2, 1)
        
        # Remove batch dimension
        predictions = outputs[0]  # Shape: (8400, 84)
        
        # Split into boxes and class scores
        boxes = predictions[:, :4]  # [x_center, y_center, w, h]
        class_scores = predictions[:, 4:]  # [class0_prob, ..., class79_prob]
        
        # Get best class for each detection
        class_ids = np.argmax(class_scores, axis=1)
        confidences = np.max(class_scores, axis=1)
        
        # Filter by confidence
        mask = confidences > self.confidence_threshold
        boxes = boxes[mask]
        confidences = confidences[mask]
        class_ids = class_ids[mask]
        
        if len(boxes) == 0:
            return []
        
        # Convert from center format to corner format
        boxes_xyxy = self._xywh_to_xyxy(boxes)
        
        # Get preprocessing info for accurate coordinate transformation
        preprocess_info = getattr(self, '_preprocess_info', None)
        if not preprocess_info:
            logger.warning("No preprocessing info, using fallback scaling")
            scale_x = original_shape[1] / input_size[0]
            scale_y = original_shape[0] / input_size[1]
            boxes_xyxy[:, [0, 2]] *= scale_x
            boxes_xyxy[:, [1, 3]] *= scale_y
        else:
            # Accurate transformation using letterbox info
            scale = preprocess_info['scale']
            pad_x = preprocess_info['pad_x']
            pad_y = preprocess_info['pad_y']
            
            # Remove padding
            boxes_xyxy[:, [0, 2]] -= pad_x
            boxes_xyxy[:, [1, 3]] -= pad_y
            
            # Scale back to original size
            boxes_xyxy /= scale
        
        # Clip to image boundaries
        orig_h, orig_w = original_shape
        boxes_xyxy[:, [0, 2]] = np.clip(boxes_xyxy[:, [0, 2]], 0, orig_w)
        boxes_xyxy[:, [1, 3]] = np.clip(boxes_xyxy[:, [1, 3]], 0, orig_h)
        
        # Apply NMS
        indices = self._nms(boxes_xyxy, confidences, iou_threshold)
        
        # Build results
        results = []
        for idx in indices:
            x1, y1, x2, y2 = boxes_xyxy[idx]
            
            if x2 <= x1 or y2 <= y1:
                continue
            
            results.append({
                'bbox': [int(x1), int(y1), int(x2), int(y2)],
                'confidence': float(confidences[idx]),
                'class_id': int(class_ids[idx])
            })
        
        logger.info(f"Full YOLOv8 format: {len(results)} detections")
        return results
    
    def _postprocess_yolov5(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                           input_size: Tuple[int, int], iou_threshold: float) -> List[Dict]:
        """
        Postprocess YOLOv5 format (legacy)
        
        Format: [batch, 25200, 85]
        Where 85 = 4 (box) + 1 (objectness) + 80 (class scores)
        """
        logger.debug(f"Using YOLOv5 postprocessing")
        
        predictions = outputs[0]  # Remove batch dimension
        
        # Split components
        boxes = predictions[:, :4]  # [x_center, y_center, w, h]
        objectness = predictions[:, 4]
        class_scores = predictions[:, 5:]
        
        # Combine objectness with class scores
        confidences = objectness[:, np.newaxis] * class_scores
        class_ids = np.argmax(confidences, axis=1)
        confidences = np.max(confidences, axis=1)
        
        # Filter by confidence
        mask = confidences > self.confidence_threshold
        boxes = boxes[mask]
        confidences = confidences[mask]
        class_ids = class_ids[mask]
        
        if len(boxes) == 0:
            return []
        
        # Convert and scale boxes (same as YOLOv8 full)
        boxes_xyxy = self._xywh_to_xyxy(boxes)
        
        # Transform coordinates
        preprocess_info = getattr(self, '_preprocess_info', None)
        if preprocess_info:
            scale = preprocess_info['scale']
            pad_x = preprocess_info['pad_x']
            pad_y = preprocess_info['pad_y']
            
            boxes_xyxy[:, [0, 2]] -= pad_x
            boxes_xyxy[:, [1, 3]] -= pad_y
            boxes_xyxy /= scale
        
        # Clip and NMS
        orig_h, orig_w = original_shape
        boxes_xyxy[:, [0, 2]] = np.clip(boxes_xyxy[:, [0, 2]], 0, orig_w)
        boxes_xyxy[:, [1, 3]] = np.clip(boxes_xyxy[:, [1, 3]], 0, orig_h)
        
        indices = self._nms(boxes_xyxy, confidences, iou_threshold)
        
        results = []
        for idx in indices:
            x1, y1, x2, y2 = boxes_xyxy[idx]
            if x2 > x1 and y2 > y1:
                results.append({
                    'bbox': [int(x1), int(y1), int(x2), int(y2)],
                    'confidence': float(confidences[idx]),
                    'class_id': int(class_ids[idx])
                })
        
        logger.info(f"YOLOv5 format: {len(results)} detections")
        return results
    
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
    """TensorRT model loader via Ultralytics"""
    
    def load_model(self):
        if not self.is_loaded:
            try:
                from ultralytics import YOLO
                
                self.model = YOLO(self.model_path)
                self.is_loaded = True
                print(f"✅ TensorRT model loaded: {self.model_path}")
                
            except Exception as e:
                print(f"❌ Error loading TensorRT model: {e}")
                raise
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        if not self.is_loaded:
            self.load_model()
        
        results = self.model(image, conf=self.confidence_threshold, verbose=False)
        
        if len(results) > 0:
            result = results[0]
            boxes = result.boxes
            if boxes is not None and len(boxes) > 0:
                xyxy = boxes.xyxy.cpu().numpy()
                conf = boxes.conf.cpu().numpy()
                cls = boxes.cls.cpu().numpy()
                detections = np.column_stack([xyxy, conf, cls])
                return np.expand_dims(detections, axis=0)
        
        return np.zeros((1, 0, 6), dtype=np.float32)
    
    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640),
                   iou_threshold: float = 0.45) -> List[Dict]:
        # Outputs are already in xyxy format from Ultralytics
        if outputs.shape[1] == 0:
            return []
        
        results = []
        for det in outputs[0]:
            x1, y1, x2, y2, conf, cls = det
            results.append({
                'bbox': [int(x1), int(y1), int(x2), int(y2)],
                'confidence': float(conf),
                'class_id': int(cls)
            })
        return results


class OpenVINOModelLoader(BaseModelLoader):
    """OpenVINO model loader"""
    
    def load_model(self):
        if not self.is_loaded:
            try:
                from openvino.runtime import Core
                
                self.core = Core()
                
                model_path = Path(self.model_path)
                if model_path.is_dir():
                    xml_files = list(model_path.glob("*.xml"))
                    if not xml_files:
                        raise FileNotFoundError(f"No .xml file found in {model_path}")
                    model_file = xml_files[0]
                else:
                    model_file = model_path
                
                self.model = self.core.read_model(model=str(model_file))
                self.compiled_model = self.core.compile_model(model=self.model, device_name="CPU")
                
                self.input_layer = self.compiled_model.input(0)
                self.output_layer = self.compiled_model.output(0)
                
                self.is_loaded = True
                print(f"✅ OpenVINO model loaded: {model_file}")
                
            except Exception as e:
                print(f"❌ Error loading OpenVINO model: {e}")
                raise
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        if not self.is_loaded:
            self.load_model()
        
        input_tensor = self.preprocess(image)
        result = self.compiled_model([input_tensor])
        outputs = result[self.output_layer]
        
        return outputs
    
    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640),
                   iou_threshold: float = 0.45) -> List[Dict]:
        # Use ONNX postprocessing (it handles multiple formats)
        loader = ONNXModelLoader("", self.confidence_threshold)
        loader._preprocess_info = getattr(self, '_preprocess_info', None)
        return loader.postprocess(outputs, original_shape, input_size, iou_threshold)


class PyTorchModelLoader(BaseModelLoader):
    """PyTorch/Ultralytics YOLO model loader"""
    
    def load_model(self):
        if not self.is_loaded:
            try:
                from ultralytics import YOLO
                import torch
                
                self.model = YOLO(self.model_path)
                device = 'cuda' if torch.cuda.is_available() else 'cpu'
                self.model.to(device)
                
                self.is_loaded = True
                print(f"✅ PyTorch model loaded: {self.model_path}")
                print(f"   Device: {device.upper()}")
                
            except Exception as e:
                print(f"❌ Error loading PyTorch model: {e}")
                raise
    
    def predict(self, image: np.ndarray) -> np.ndarray:
        if not self.is_loaded:
            self.load_model()
        
        results = self.model(image, conf=self.confidence_threshold, verbose=False)
        
        if len(results) > 0:
            result = results[0]
            boxes = result.boxes
            
            if boxes is not None and len(boxes) > 0:
                xyxy = boxes.xyxy.cpu().numpy()
                conf = boxes.conf.cpu().numpy()
                cls = boxes.cls.cpu().numpy()
                
                detections = np.column_stack([xyxy, conf, cls])
                output = np.expand_dims(detections, axis=0)
                return output
        
        return np.zeros((1, 0, 6), dtype=np.float32)
    
    def postprocess(self, outputs: np.ndarray, original_shape: Tuple[int, int],
                   input_size: Tuple[int, int] = (640, 640),
                   iou_threshold: float = 0.45) -> List[Dict]:
        if outputs.shape[1] == 0:
            return []
        
        detections_array = outputs[0]
        
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
        """Create appropriate model loader based on file extension or backend"""
        path = Path(model_path)
        
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
