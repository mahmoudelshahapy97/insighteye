
# app/services/stream_processing_service.py (CRITICAL CHANGES)
import torch
import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Dict, Optional
# ========================================
# 1️⃣ CONFIGURE PYTORCH FOR CPU (ADD AT TOP)
# ========================================
torch.set_num_threads(4)  # Limit per-model threads
torch.set_num_interop_threads(2)
torch.set_grad_enabled(False)  # Disable gradients for inference

class StreamProcessingService:
    def init(self):
        self.db_manager = db_manager
        # ========================================
        # 2️⃣ TIME-BASED DETECTION TRACKING
        # ========================================
        self.last_detection_time: Dict[str, float] = {}  # stream_id -> timestamp
        self.detection_results_cache: Dict[str, Dict] = {}  # Cached results
        
        # Configurable intervals (seconds)
        self.PEOPLE_DETECT_INTERVAL = 2.0    # Every 2 seconds
        self.GENDER_DETECT_INTERVAL = 10.0   # Every 10 seconds  
        self.FIRE_DETECT_INTERVAL = 5.0      # Every 5 seconds
        
        # ========================================
        # 3️⃣ OPTIMIZED MODEL LOADING
        # ========================================
        self._initialize_models_optimized()

    def _initialize_models_optimized(self):
        """Load models with CPU optimizations"""
        people_model_path = config.get("people_model_path", "yolov8n.pt")
        gender_model_path = config.get("gender_model_path", "gender.pt")
        fire_model_path = config.get("fire_model_path", "fire.pt")
        
        try:
            # People model (critical)
            self.people_model = YOLO(people_model_path)
            self.people_model.fuse()  # Fuse conv+bn layers
            
            # Gender model (optional)
            if os.path.exists(gender_model_path):
                self.gender_model = YOLO(gender_model_path)
                self.gender_model.fuse()
            else:
                self.gender_model = None
                logger.warning("Gender model disabled")
            
            # Fire model (optional)
            if os.path.exists(fire_model_path):
                self.fire_model = YOLO(fire_model_path)
                self.fire_model.fuse()
            else:
                self.fire_model = None
                logger.warning("Fire model disabled")
            
            logger.info("✅ Models loaded with CPU optimizations")
            
        except Exception as e:
            logger.error(f"Model initialization failed: {e}")
            raise

    # ========================================
    # 4️⃣ TIME-BASED DETECTION (REPLACES FRAME-BASED)
    # ========================================
    def should_run_detection(self, stream_id: str, detection_type: str) -> bool:
        """Check if enough time has passed for this detection type"""
        now = time.time()
        key = f"{stream_id}:{detection_type}"
        last_time = self.last_detection_time.get(key, 0)
        
        intervals = {
            'people': self.PEOPLE_DETECT_INTERVAL,
            'gender': self.GENDER_DETECT_INTERVAL,
            'fire': self.FIRE_DETECT_INTERVAL
        }
        
        interval = intervals.get(detection_type, 2.0)
        
        if now - last_time >= interval:
            self.last_detection_time[key] = now
            return True
        return False

    def get_cached_results(self, stream_id: str) -> Dict:
        """Get cached detection results"""
        return self.detection_results_cache.get(stream_id, {
            'person_count': 0,
            'male_count': 0,
            'female_count': 0,
            'fire_status': 'no detection',
            'timestamp': time.time()
        })

    # ========================================
    # 5️⃣ OPTIMIZED DETECTION WITH CACHING
    # ========================================
    def detect_objects_with_threshold(
        self,
        frame: np.ndarray,
        conf_threshold: float = 0.5,
        threshold_settings: Dict[str, Any] = None,
        stream_id_str: str = None
    ) -> Tuple[np.ndarray, int, bool, int, int, str]:
        """Optimized detection with time-based sampling and caching"""
        
        if frame is None or frame.size == 0:
            cached = self.get_cached_results(stream_id_str)
            return frame, cached['person_count'], False, \
                cached['male_count'], cached['female_count'], cached['fire_status']
        
        # ========================================
        # 6️⃣ CHECK IF DETECTION NEEDED
        # ========================================
        run_people = self.should_run_detection(stream_id_str, 'people')
        run_gender = self.should_run_detection(stream_id_str, 'gender')
        run_fire = self.should_run_detection(stream_id_str, 'fire')
        
        # If no detection needed, return cached
        if not (run_people or run_gender or run_fire):
            cached = self.get_cached_results(stream_id_str)
            annotated = frame.copy()
            self._draw_cached_annotations(annotated, cached)
            return annotated, cached['person_count'], False, \
                cached['male_count'], cached['female_count'], cached['fire_status']
        
        # ========================================
        # 7️⃣ OPTIMIZE INPUT SIZE (CRITICAL FOR CPU)
        # ========================================
        # Resize to max 512 for CPU efficiency
        h, w = frame.shape[:2]
        max_dim = 512  # Lower than 640 for CPU
        scale = 1.0
        
        if h > max_dim or w > max_dim:
            scale = max_dim / max(h, w)
            new_w = int(w * scale) & ~1  # Even dimensions
            new_h = int(h * scale) & ~1
            input_frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)
        else:
            input_frame = frame
        
        # ========================================
        # 8️⃣ RUN DETECTIONS (TIME-GATED)
        # ========================================
        person_count = 0
        male_count = 0
        female_count = 0
        fire_status = 'no detection'
        
        # People detection
        if run_people:
            try:
                results = self.people_model.predict(
                    source=input_frame,
                    conf=conf_threshold,
                    classes=[0],  # Person class only
                    verbose=False,
                    imgsz=512,  # Fixed size for consistency
                    half=False,  # CPU doesn't support FP16
                    device='cpu'
                )
                
                if results and len(results) > 0 and results[0].boxes is not None:
                    person_count = len(results[0].boxes)
            except Exception as e:
                logger.error(f"People detection error: {e}")
        
        # Gender detection (only if people detected AND interval passed)
        if run_gender and person_count > 0 and self.gender_model:
            try:
                gender_results = self.gender_model.predict(
                    source=input_frame,
                    conf=0.5,
                    verbose=False,
                    imgsz=512,
                    device='cpu'
                )
                
                if gender_results and gender_results[0].boxes:
                    male_count = sum(1 for box in gender_results[0].boxes 
                                if int(box.cls[0]) == 1)
                    female_count = sum(1 for box in gender_results[0].boxes 
                                    if int(box.cls[0]) == 0)
            except Exception as e:
                logger.error(f"Gender detection error: {e}")
        
        # Fire detection (time-gated)
        if run_fire and self.fire_model:
            try:
                fire_results = self.fire_model.predict(
                    source=input_frame,
                    conf=0.8,
                    verbose=False,
                    imgsz=512,
                    device='cpu'
                )
                
                if fire_results and fire_results[0].boxes:
                    classes = [int(box.cls[0]) for box in fire_results[0].boxes]
                    if 0 in classes:
                        fire_status = "fire"
                    elif 1 in classes:
                        fire_status = "smoke"
            except Exception as e:
                logger.error(f"Fire detection error: {e}")
        
        # ========================================
        # 9️⃣ UPDATE CACHE
        # ========================================
        self.detection_results_cache[stream_id_str] = {
            'person_count': person_count,
            'male_count': male_count,
            'female_count': female_count,
            'fire_status': fire_status,
            'timestamp': time.time()
        }
        
        # Annotate frame
        annotated = input_frame.copy()
        self._draw_annotations(annotated, person_count, male_count, 
                            female_count, fire_status)
        
        # Scale back if needed
        if scale != 1.0:
            annotated = cv2.resize(annotated, (w, h), interpolation=cv2.INTER_LINEAR)
        
        # Check thresholds
        alert_triggered = self._check_thresholds(person_count, threshold_settings)
        
        return annotated, person_count, alert_triggered, male_count, female_count, fire_status

    def _draw_annotations(self, frame, person_count, male_count, female_count, fire_status):
        """Draw detection results on frame"""
        # People count
        cv2.putText(frame, f"People: {person_count} | M: {male_count} | F: {female_count}",
                (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        
        # Fire warning
        if fire_status != "no detection":
            cv2.putText(frame, f"ALERT: {fire_status.upper()}",
                    (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)

    def _check_thresholds(self, count, settings):
        """Check if count exceeds thresholds"""
        if not settings or not settings.get("alert_enabled"):
            return False
        
        greater = settings.get("greater_than")
        less = settings.get("less_than")
        
        if greater is not None and count > greater:
            return True
        if less is not None and count < less:
            return True
        return False
