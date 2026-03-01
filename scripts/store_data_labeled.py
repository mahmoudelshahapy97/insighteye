import cv2
import time
import os
from datetime import datetime
from ultralytics import YOLO

# --- CONFIGURATION ---
save_folder = "images_labeled"
max_images = 2000
interval_seconds = 120  # 2 Minutes
model_path = "models/people.pt" # Path to your custom model
conf_threshold = 0.4            # Only save labels with confidence > 40%

# List of your cameras
rtsp_urls = [
    "rtsp://airtsp:A12rEyeTsp@10.1.20.50:554",
    "rtsp://airtsp:A12rEyeTsp@10.1.20.55:554",
    "rtsp://airtsp:A12rEyeTsp@10.1.20.41:554"
]

# Create folder if it doesn't exist
if not os.path.exists(save_folder):
    os.makedirs(save_folder)

# --- LOAD YOLO MODEL ---
print(f"Loading model from {model_path}...")
try:
    model = YOLO(model_path)
except Exception as e:
    print(f"Error loading model: {e}")
    exit()

def extract_ip(url):
    """Extracts IP address from the RTSP string for naming."""
    try:
        return url.split('@')[1].split(':')[0]
    except:
        return "unknown_ip"

total_saved_images = 0

print(f"Starting capture loop. Target: {max_images} images. Interval: {interval_seconds}s")

while total_saved_images < max_images:
    cycle_start = time.time()
    
    print(f"\n--- Starting Capture Cycle (Total Saved: {total_saved_images}) ---")

    for url in rtsp_urls:
        if total_saved_images >= max_images:
            break

        ip_address = extract_ip(url)
        print(f"Connecting to {ip_address}...")

        # Initialize Capture
        cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
        
        # Specific options for snapshot stability
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1) 
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000)

        if cap.isOpened():
            # Clear buffer slightly
            ret, frame = cap.read()
            
            if ret and frame is not None:
                # --- 1. RUN DETECTION ---
                # Run inference on the frame
                results = model(frame, verbose=False, conf=conf_threshold)

                # --- 2. PREPARE FILENAMES ---
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name = f"{ip_address}_{timestamp}"
                
                img_filename = f"{base_name}.jpg"
                txt_filename = f"{base_name}.txt"
                
                img_path = os.path.join(save_folder, img_filename)
                txt_path = os.path.join(save_folder, txt_filename)

                # --- 3. SAVE IMAGE ---
                cv2.imwrite(img_path, frame)

                # --- 4. SAVE LABELS (YOLO FORMAT) ---
                # YOLO Format: class_id center_x center_y width height (normalized 0-1)
                with open(txt_path, "w") as f:
                    # results[0] contains detections for the first (and only) image
                    for result in results:
                        boxes = result.boxes
                        for box in boxes:
                            # Get class ID
                            cls = int(box.cls[0])
                            
                            # Get normalized coordinates (xywhn)
                            # x_center, y_center, width, height (all between 0 and 1)
                            x, y, w, h = box.xywhn[0].tolist()
                            
                            # Write to file
                            f.write(f"{cls} {x:.6f} {y:.6f} {w:.6f} {h:.6f}\n")

                print(f"  [SUCCESS] Saved: {img_filename} and {txt_filename}")
                total_saved_images += 1
            else:
                print(f"  [ERROR] Connected but failed to retrieve frame from {ip_address}")
        else:
            print(f"  [ERROR] Could not connect to {ip_address}")

        # Release camera immediately
        cap.release()

    if total_saved_images >= max_images:
        print("\nTarget image count reached. Exiting.")
        break

    # Sleep logic
    elapsed = time.time() - cycle_start
    sleep_time = interval_seconds - elapsed
    
    if sleep_time > 0:
        print(f"Cycle complete. Sleeping for {sleep_time:.1f} seconds...")
        time.sleep(sleep_time)
    else:
        print("Cycle took longer than interval, starting next immediately.")