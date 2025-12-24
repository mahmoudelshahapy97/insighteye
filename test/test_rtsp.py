import cv2
import time

def test_rtsp_stream(rtsp_url, timeout=5):
    cap = cv2.VideoCapture(rtsp_url)
    if not cap.isOpened():
        print("❌ Cannot open RTSP stream")
        return False

    start = time.time()
    while time.time() - start < timeout:
        ret, frame = cap.read()
        if ret:
            print("✅ Frame received!")
            cap.release()
            return True
        time.sleep(0.1)

    print("❌ No frame received within timeout")
    cap.release()
    return False

if __name__ == "__main__":
    url = "rtsp://admin:12345678@41.178.2.61:554/ch12/0"
    test_rtsp_stream(url)
