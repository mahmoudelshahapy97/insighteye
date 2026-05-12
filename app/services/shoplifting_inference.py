import os
import cv2
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from collections import deque
from ultralytics import YOLO
import logging

from app.config.settings import config

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# MODEL DEFINITION  (must match training)
# ──────────────────────────────────────────────
class ShopliftingClassifier(nn.Module):
    def __init__(self, T, V, C, num_classes):
        super().__init__()
        self.T = T
        self.V = V
        self.C = C

        self.conv1     = nn.Conv1d(V * C, 128, kernel_size=3, padding=2)
        self.bn1       = nn.BatchNorm1d(128)
        self.gru       = nn.GRU(128, 128, batch_first=True, bidirectional=True)
        self.attention = nn.MultiheadAttention(embed_dim=256, num_heads=4, batch_first=True)
        self.layernorm = nn.LayerNorm(256)
        self.fc1       = nn.Linear(256, 128)
        self.dropout   = nn.Dropout(0.3)
        self.fc2       = nn.Linear(128, num_classes)

    def forward(self, x):
        B  = x.size(0)
        T, V, C = self.T, self.V, self.C

        x = x.view(B, T, V * C).transpose(1, 2)   # (B, V*C, T)
        x = F.pad(x, (2, 0))
        x = F.relu(self.conv1(x))
        x = self.bn1(x)
        x = x.transpose(1, 2)                      # (B, T, 128)
        x, _ = self.gru(x)                          # (B, T, 256)
        attn_out, _ = self.attention(x, x, x)
        x = self.layernorm(x + attn_out)
        x = x.mean(dim=1)
        x = F.relu(self.fc1(x))
        x = self.dropout(x)
        return self.fc2(x)

def load_classifier() -> ShopliftingClassifier:
    T = config.shoplifting_num_frames
    V = 17
    C = 12
    num_classes = 2
    device = "cuda" if torch.cuda.is_available() and config.model_device == "cuda" else "cpu"
    
    model = ShopliftingClassifier(T=T, V=V, C=C, num_classes=num_classes)
    
    # The shoplifting classifier is always a custom PyTorch model loaded via torch.load(),
    # so always use the .pt path regardless of the global MODEL_BACKEND setting.
    classifier_path = config.pt_shoplifting_model_path
    if not os.path.exists(classifier_path):
        logger.warning(f"⚠️ Shoplifting classifier weights not found at {classifier_path}. Returning uninitialized model.")
        return model

    try:
        state = torch.load(classifier_path, map_location=device)
        if "model_state_dict" in state:
            state = state["model_state_dict"]
        model.load_state_dict(state)
        model.to(device)
        model.eval()
        logger.info(f"[✓] Classifier loaded on {device} from {classifier_path}")
    except Exception as e:
        logger.error(f"Error loading shoplifting classifier: {e}")
        
    return model

class FrameFeatureExtractor:
    """Extracts a (17, 12) feature vector from one frame."""

    def __init__(self, pose_model: YOLO):
        self.pose_model = pose_model
        # We need to maintain prev_kpts per stream!
        self.stream_prev_kpts = {}

    def extract(self, stream_id: str, frame: np.ndarray) -> np.ndarray:
        """Returns (17, 12) float32 array. All zeros if no person detected."""
        if not hasattr(self, 'pose_model') or self.pose_model is None:
            return np.zeros((17, 12), dtype=np.float32)
            
        # Predict using YOLO Pose
        results = self.pose_model(frame, verbose=False)

        if not results or len(results[0].keypoints.data) == 0:
            self.stream_prev_kpts[stream_id] = None
            return np.zeros((17, 12), dtype=np.float32)

        # Get first person keypoints
        kpts = results[0].keypoints.xyn[0].cpu().numpy()   # (17, 2)
        prev_kpts = self.stream_prev_kpts.get(stream_id, None)

        # ── velocity ──────────────────────────────────────
        vel = kpts - prev_kpts if prev_kpts is not None else np.zeros_like(kpts)
        self.stream_prev_kpts[stream_id] = kpts.copy()

        # ── hand–hip distances ────────────────────────────
        l_hand  = kpts[9]
        r_hand  = kpts[10]
        mid_hip = (kpts[11] + kpts[12]) / 2.0

        dist_l   = np.linalg.norm(l_hand - mid_hip)
        dist_r   = np.linalg.norm(r_hand - mid_hip)
        dist_hh  = np.linalg.norm(l_hand - r_hand)

        scalars = np.tile([dist_l, dist_r, dist_hh], (17, 1))  # (17, 3)

        # ── concatenate: xy(2) + vel(2) + scalars(3) = 7 cols → pad to 12 ──
        feat = np.concatenate([kpts, vel, scalars], axis=1)     # (17, 7)
        pad  = np.zeros((17, 12 - feat.shape[1]), dtype=np.float32)
        return np.concatenate([feat, pad], axis=1).astype(np.float32)  # (17, 12)

class StreamAwareInferenceEngine:
    def __init__(self):
        self.is_loaded = False
        
        self.pose_model = None
        self.classifier = None
        self.extractor = None

        self.num_frames = config.shoplifting_num_frames
        self.conf_thresh = config.shoplifting_confidence
        self.smooth_window = 10
        self.class_names = ["Normal", "Shoplifting"]
        self.device = "cuda" if torch.cuda.is_available() and config.model_device == "cuda" else "cpu"

        # Stream specific buffers
        self.buffers = {}
        self.pred_hists = {}
        self.forced_alerts = {}

    def force_alert(self, stream_id: str):
        """Forces an alert for the next 5 frames to test the video saving pipeline."""
        self.forced_alerts[stream_id] = 5

    def load_models(self):
        # Allow loading if already loaded
        if self.is_loaded:
            return True
            
        try:
            pose_path = config.pt_pose_model_path
            if not os.path.exists(pose_path):
                logger.warning(f"⚠️ Pose model not found at {pose_path}. Shoplifting engine disabled.")
                return False
                
            self.pose_model = YOLO(pose_path)
            self.classifier = load_classifier()
            self.extractor = FrameFeatureExtractor(self.pose_model)
            self.is_loaded = True
            logger.info("StreamAwareInferenceEngine for Shoplifting successfully initialized.")
            return True
        except Exception as e:
            logger.error(f"Failed to load temporal shoplifting models: {e}")
            self.is_loaded = False
            return False

    @staticmethod
    def _normalise(seq: np.ndarray) -> np.ndarray:
        mu  = seq.mean()
        std = seq.std() + 1e-7
        return (seq - mu) / std

    @torch.no_grad()
    def _predict_buffer(self, stream_id: str):
        buf = self.buffers.get(stream_id)
        if buf is None or len(buf) < self.num_frames:
            return None, None

        if not self.is_loaded or self.classifier is None:
            return None, None

        seq = np.stack(buf, axis=0)           # (T, 17, 12)
        seq = self._normalise(seq)
        x   = torch.tensor(seq, dtype=torch.float32).unsqueeze(0)  # (1, T, 17, 12)
        x   = x.to(self.device)

        logits = self.classifier(x)                   # (1, num_classes)
        probs  = F.softmax(logits, dim=1)[0]
        cls    = probs.argmax().item()
        conf   = probs[cls].item()
        return cls, conf

    def process_frame(self, stream_id: str, frame: np.ndarray):
        """
        Returns (label, conf, alert)
        """
        forced = self.forced_alerts.get(stream_id, 0)
        if forced > 0:
            self.forced_alerts[stream_id] = forced - 1
            return "Shoplifting (Test)", 0.99, True

        if not self.is_loaded:
            return "Disabled", 0.0, False

        # Initialize stream buffers if they don't exist
        if stream_id not in self.buffers:
            self.buffers[stream_id] = deque(maxlen=self.num_frames)
        if stream_id not in self.pred_hists:
            self.pred_hists[stream_id] = deque(maxlen=self.smooth_window)

        feat = self.extractor.extract(stream_id, frame)
        self.buffers[stream_id].append(feat)

        cls, conf = self._predict_buffer(stream_id)

        if cls is None:
            return "Buffering...", 0.0, False

        self.pred_hists[stream_id].append(cls)
        # majority vote
        hist = list(self.pred_hists[stream_id])
        smoothed_cls = max(set(hist), key=hist.count)
        
        label = self.class_names[smoothed_cls]
        alert = (smoothed_cls == 1) and (conf >= self.conf_thresh)
        return label, conf, alert

    def get_status(self) -> bool:
        return True  # Always return True so we can bypass loading and test simulated alerts

# Global singleton
shoplifting_engine = StreamAwareInferenceEngine()
