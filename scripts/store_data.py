import cv2
import time
import os
from datetime import datetime

# --- CONFIGURATION ---
save_folder = "images_test"
max_images = 2000
interval_seconds = 120  # 2 Minutes

# List of your cameras
rtsp_urls = [
    "rtsp://airtsp:A12rEyeTsp@10.1.20.50:554",
    "rtsp://airtsp:A12rEyeTsp@10.1.20.55:554",
    "rtsp://airtsp:A12rEyeTsp@10.1.20.41:554"
]

# Create folder if it doesn't exist
if not os.path.exists(save_folder):
    os.makedirs(save_folder)

def extract_ip(url):
    """Extracts IP address from the RTSP string for naming."""
    try:
        # Splits by '@' then takes the part after, then splits by ':' and takes the first part
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
        
        # specific options to make connection faster/robust for snapshots
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1) 
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000)

        if cap.isOpened():
            # Sometimes the first frame is grey/corrupt in RTSP, read a couple to clear buffer
            # We grab one, verify it, then use it.
            ret, frame = cap.read()
            
            if ret and frame is not None:
                # Create timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{ip_address}_{timestamp}.jpg"
                file_path = os.path.join(save_folder, filename)

                # Save Image
                cv2.imwrite(file_path, frame)
                print(f"  [SUCCESS] Saved: {filename}")
                total_saved_images += 1
            else:
                print(f"  [ERROR] Connected but failed to retrieve frame from {ip_address}")
        else:
            print(f"  [ERROR] Could not connect to {ip_address}")

        # Always release the camera immediately to save resources
        cap.release()

    if total_saved_images >= max_images:
        print("\nTarget image count reached. Exiting.")
        break

    # Calculate how much time passed during the capture process
    elapsed = time.time() - cycle_start
    sleep_time = interval_seconds - elapsed
    
    if sleep_time > 0:
        print(f"Cycle complete. Sleeping for {sleep_time:.1f} seconds...")
        time.sleep(sleep_time)
    else:
        print("Cycle took longer than interval, starting next immediately.")