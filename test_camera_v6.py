import av
from ultralytics import YOLO
import cv2

# Load YOLOv8 model
model = YOLO("yolov8n.pt")  # Use yolov8n.pt, yolov8s.pt, etc.

# Open RTSP stream
rtsp_url = "http://88.53.197.250/axis-cgi/mjpg/video.cgi?resolution=320x240"
container = av.open(rtsp_url, options={"rtsp_transport": "tcp"})  # Force TCP for stability

for frame in container.decode(video=0):  # Decode video frames only
    # Convert frame to numpy array (HxWxC format)
    img = frame.to_ndarray(format="rgb24")  # YOLO expects RGB
    
    # Run YOLOv8 inference
    results = model.predict(img, verbose=False)
    
    # Process results (e.g., draw bounding boxes)
    annotated_img = results[0].plot()  # Get annotated image
    
    # Optional: Display or save results
    cv2.imshow("YOLOv8", annotated_img)
    if cv2.waitKey(1) & 0xFF == ord("q"):
        break

# Optional: Cleanup (if using OpenCV for display)
cv2.destroyAllWindows()

