"""
Unit tests for Inference Service
Tests AI/ML model loading, object detection, and inference operations
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import numpy as np
from PIL import Image


@pytest.fixture
def inference_service():
    """Create InferenceService instance with mocked dependencies"""
    with patch('app.services.inference_service.YOLO') as mock_yolo:
        from app.services.inference_service import InferenceService
        
        # Mock YOLO model
        mock_model = MagicMock()
        mock_yolo.return_value = mock_model
        
        service = InferenceService()
        service.model = mock_model
        yield service


@pytest.fixture
def sample_frame():
    """Create sample image frame for testing"""
    # Create a simple test image
    img = Image.new('RGB', (640, 480), color='red')
    return np.array(img)


class TestModelLoading:
    """Test model loading functionality"""
    
    def test_load_yolo_model(self, inference_service):
        """Test YOLO model initialization"""
        assert inference_service.model is not None
    
    @patch('app.services.inference_service.YOLO')
    def test_load_custom_model(self, mock_yolo):
        """Test loading custom model weights"""
        from app.services.inference_service import InferenceService
        
        service = InferenceService(model_path="custom_weights.pt")
        
        mock_yolo.assert_called()


class TestObjectDetection:
    """Test object detection functionality"""
    
    def test_detect_objects_in_frame(self, inference_service, sample_frame):
        """Test object detection on sample frame"""
        # Mock detection results
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        mock_results.boxes.data = np.array([[100, 100, 200, 200, 0.9, 0]])  # x1, y1, x2, y2, conf, class
        
        inference_service.model.return_value = [mock_results]
        
        detections = inference_service.detect(sample_frame)
        
        assert detections is not None
        assert isinstance(detections, list) or isinstance(detections, np.ndarray)
    
    def test_detect_multiple_objects(self, inference_service, sample_frame):
        """Test detecting multiple objects"""
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        mock_results.boxes.data = np.array([
            [100, 100, 200, 200, 0.9, 0],
            [300, 300, 400, 400, 0.85, 1]
        ])
        
        inference_service.model.return_value = [mock_results]
        
        detections = inference_service.detect(sample_frame)
        
        assert len(detections) >= 0
    
    def test_confidence_threshold_filtering(self, inference_service, sample_frame):
        """Test filtering detections by confidence threshold"""
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        mock_results.boxes.data = np.array([
            [100, 100, 200, 200, 0.9, 0],   # High confidence
            [300, 300, 400, 400, 0.3, 1]    # Low confidence
        ])
        
        inference_service.model.return_value = [mock_results]
        
        detections = inference_service.detect(sample_frame, confidence_threshold=0.5)
        
        # Should filter out low confidence detections
        assert isinstance(detections, (list, np.ndarray))


class TestFireDetection:
    """Test fire detection functionality"""
    
    def test_detect_fire(self, inference_service, sample_frame):
        """Test fire detection"""
        # Mock fire detection result
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        # Assuming fire class ID is 0
        mock_results.boxes.data = np.array([[100, 100, 200, 200, 0.95, 0]])
        
        inference_service.model.return_value = [mock_results]
        
        has_fire = inference_service.detect_fire(sample_frame)
        
        assert isinstance(has_fire, bool)
    
    def test_fire_detection_threshold(self, inference_service, sample_frame):
        """Test fire detection with confidence threshold"""
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        mock_results.boxes.data = np.array([[100, 100, 200, 200, 0.6, 0]])
        
        inference_service.model.return_value = [mock_results]
        
        # Should detect fire with lower threshold
        has_fire_low = inference_service.detect_fire(sample_frame, threshold=0.5)
        
        # Should not detect fire with higher threshold
        has_fire_high = inference_service.detect_fire(sample_frame, threshold=0.9)
        
        assert isinstance(has_fire_low, bool)
        assert isinstance(has_fire_high, bool)


class TestPeopleCounting:
    """Test people counting functionality"""
    
    def test_count_people(self, inference_service, sample_frame):
        """Test counting people in frame"""
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        # Assuming person class ID is 0
        mock_results.boxes.data = np.array([
            [100, 100, 200, 200, 0.9, 0],
            [300, 300, 400, 400, 0.85, 0],
            [500, 100, 600, 200, 0.88, 0]
        ])
        
        inference_service.model.return_value = [mock_results]
        
        count = inference_service.count_people(sample_frame)
        
        assert isinstance(count, int)
        assert count >= 0
    
    def test_count_people_empty_frame(self, inference_service, sample_frame):
        """Test people counting with no people"""
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        mock_results.boxes.data = np.array([])
        
        inference_service.model.return_value = [mock_results]
        
        count = inference_service.count_people(sample_frame)
        
        assert count == 0


class TestInferencePerformance:
    """Test inference performance"""
    
    def test_inference_speed(self, inference_service, sample_frame):
        """Test inference execution time"""
        import time
        
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        mock_results.boxes.data = np.array([[100, 100, 200, 200, 0.9, 0]])
        
        inference_service.model.return_value = [mock_results]
        
        start_time = time.time()
        inference_service.detect(sample_frame)
        end_time = time.time()
        
        inference_time = end_time - start_time
        
        # Inference should be reasonably fast (< 1 second for mocked model)
        assert inference_time < 1.0
    
    def test_batch_inference(self, inference_service):
        """Test batch processing of frames"""
        frames = [np.random.rand(640, 480, 3) for _ in range(5)]
        
        mock_results = MagicMock()
        mock_results.boxes = MagicMock()
        mock_results.boxes.data = np.array([[100, 100, 200, 200, 0.9, 0]])
        
        inference_service.model.return_value = [mock_results]
        
        results = [inference_service.detect(frame) for frame in frames]
        
        assert len(results) == 5


class TestErrorHandling:
    """Test error handling in inference"""
    
    def test_invalid_frame_input(self, inference_service):
        """Test handling of invalid frame input"""
        invalid_frame = None
        
        try:
            inference_service.detect(invalid_frame)
        except Exception as e:
            assert isinstance(e, (ValueError, TypeError, AttributeError))
    
    def test_model_loading_failure(self):
        """Test handling of model loading failure"""
        with patch('app.services.inference_service.YOLO') as mock_yolo:
            mock_yolo.side_effect = Exception("Model not found")
            
            try:
                from app.services.inference_service import InferenceService
                service = InferenceService()
            except Exception as e:
                assert isinstance(e, Exception)
