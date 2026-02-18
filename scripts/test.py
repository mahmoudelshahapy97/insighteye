import cv2
import time
# import os
# os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"

rtsp_url = "rtsp://airtsp:A12rEyeTsp@10.1.20.50:554"
# rtsp://airtsp:A12rEyeTsp@10.1.20.50:554
# rtsp://airtsp:A12rEyeTsp@10.1.20.55:554
# rtsp://airtsp:A12rEyeTsp@10.1.20.41:554


cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
cap.set(cv2.CAP_PROP_BUFFERSIZE, 3)
cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 10000)
cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 10000)

if not cap.isOpened():
    print("Failed to connect to stream")
    exit()

print("Connected successfully")

frame_count = 0
start_time = time.time()
retry_count = 0
max_retries = 10

while True:
    ret, frame = cap.read()

    if not ret:
        retry_count += 1
        print(f"Failed to read frame, retrying... ({retry_count}/{max_retries})")
        time.sleep(2)

        if retry_count >= max_retries:
            print("Too many failures, reconnecting...")
            cap.release()
            time.sleep(3)
            cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 3)
            retry_count = 0
        continue

    retry_count = 0
    frame_count += 1
    elapsed = time.time() - start_time
    fps = frame_count / elapsed

    print(f"Frame: {frame_count} | Time: {elapsed:.2f}s | FPS: {fps:.2f} | Shape: {frame.shape}")

cap.release()
