# import cv2
# import time

# camera_url = "http://88.53.197.250/axis-cgi/mjpg/video.cgi?resolution=320x240"

# print(f"Attempting to open video stream: {camera_url}")

# # Try opening the capture
# cap = cv2.VideoCapture(camera_url)

# # Check if the camera opened successfully
# if not cap.isOpened():
#     print("Error: Could not open video stream.")
#     # Optional: Print OpenCV build info to check FFmpeg support
#     print("\nOpenCV build information:")
#     print(cv2.getBuildInformation())
# else:
#     print("Video stream opened successfully.")

#     # Try reading a few frames
#     for i in range(5): # Try reading 5 frames
#         ret, frame = cap.read()
#         if not ret:
#             print(f"Error: Could not read frame {i+1}.")
#             # This is the key failure point in your Streamlit app's loop
#             break
#         else:
#             print(f"Successfully read frame {i+1}. Frame shape: {frame.shape}")
#         time.sleep(0.1) # Add a small delay

#     # Release the capture object
#     cap.release()
#     print("Video stream released.")


import cv2
import logging
import time
import sys

def test_camera_stream(camera_url, frames_to_read=10, debug=True):
    """
    Comprehensive test for camera stream connectivity
    
    Args:
        camera_url: URL to the camera stream
        frames_to_read: Number of frames to try reading
        debug: Whether to print debug information including OpenCV build
        
    Returns:
        success: Boolean indicating overall success
        error_message: Error message if any, None otherwise
        stats: Dictionary with test statistics
    """
    print(f"Testing camera stream: {camera_url}")
    success = False
    error_message = None
    stats = {
        "frames_read": 0,
        "connection_time": 0,
        "average_read_time": 0
    }
    
    if debug:
        print("\nOpenCV version:", cv2.__version__)
        
        # Check if OpenCV is built with required components
        build_info = cv2.getBuildInformation()
        has_ffmpeg = "FFMPEG" in build_info and "YES" in build_info.split("FFMPEG")[1].split("\n")[0]
        has_gstreamer = "GStreamer" in build_info and "YES" in build_info.split("GStreamer")[1].split("\n")[0]
        
        print(f"OpenCV build with FFMPEG: {'YES' if has_ffmpeg else 'NO'}")
        print(f"OpenCV build with GStreamer: {'YES' if has_gstreamer else 'NO'}")
    
    # Test with different backends
    backends = [
        (cv2.CAP_ANY, "Default"),
        (cv2.CAP_FFMPEG, "FFMPEG"),
        (cv2.CAP_GSTREAMER if hasattr(cv2, 'CAP_GSTREAMER') else cv2.CAP_ANY, "GStreamer")
    ]
    
    successful_backend = None
    
    for backend, backend_name in backends:
        print(f"\nTrying backend: {backend_name}")
        
        try:
            # Start timer for connection
            start_time = time.time()
            
            # Open connection with specific backend
            cap = cv2.VideoCapture(camera_url, backend)
            
            # Set timeout properties if available
            cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000)  # 5 second timeout
            if hasattr(cv2, 'CAP_PROP_READ_TIMEOUT_MSEC'):
                cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000)  # 5 second read timeout
            
            # Check if opened
            if not cap.isOpened():
                print(f"Could not open camera with {backend_name} backend")
                if cap:
                    cap.release()
                continue
                
            connection_time = time.time() - start_time
            print(f"Successfully connected using {backend_name} backend in {connection_time:.2f} seconds")
            
            # Try reading frames
            read_times = []
            for i in range(frames_to_read):
                frame_start = time.time()
                ret, frame = cap.read()
                read_time = time.time() - frame_start
                read_times.append(read_time)
                
                if not ret or frame is None:
                    print(f"Failed to read frame {i+1} with {backend_name} backend")
                    break
                else:
                    print(f"Successfully read frame {i+1} with {backend_name} backend")
                    print(f"Frame shape: {frame.shape}, read time: {read_time:.4f} seconds")
                    stats["frames_read"] += 1
            
            # Calculate stats for this successful connection
            if stats["frames_read"] > 0:
                stats["connection_time"] = connection_time
                stats["average_read_time"] = sum(read_times) / len(read_times)
                successful_backend = backend_name
                success = True
                
            # Clean up
            cap.release()
            
            # If we successfully read all frames, break the loop
            if stats["frames_read"] == frames_to_read:
                break
                
        except Exception as e:
            print(f"Error with {backend_name} backend: {str(e)}")
            error_message = f"Error with {backend_name} backend: {str(e)}"
    
    # Final report
    print("\n----- Camera Test Results -----")
    if success:
        print(f"✅ Successfully connected to camera with {successful_backend} backend")
        print(f"✅ Read {stats['frames_read']}/{frames_to_read} frames")
        print(f"⏱️ Connection time: {stats['connection_time']:.2f} seconds")
        print(f"⏱️ Average frame read time: {stats['average_read_time']:.4f} seconds")
    else:
        print("❌ Failed to connect to camera with any backend")
        if error_message:
            print(f"❌ Last error: {error_message}")
    
    return success, error_message, stats

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Get camera URL from command line argument, or use default
    camera_url = sys.argv[1] if len(sys.argv) > 1 else "http://88.53.197.250/axis-cgi/mjpg/video.cgi?resolution=320x240"
    
    # Run the test
    test_camera_stream(camera_url)
    