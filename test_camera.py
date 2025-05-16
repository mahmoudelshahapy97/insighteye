
"""
Camera Streaming Alternatives to OpenCV
These examples demonstrate different libraries for accessing camera streams
"""

import sys
import matplotlib.pyplot as plt
import time
import os
import cv2
import numpy as np
import subprocess
import tempfile
import shutil
import threading

# Example 1: Using GStreamer Python bindings directly
def gstreamer_camera_stream(camera_url, frames_to_read=10):
    """
    Access camera stream using GStreamer Python bindings
    """
    import gi
    gi.require_version('Gst', '1.0')
    from gi.repository import Gst, GLib
    import numpy as np
    import time
    
    # Initialize GStreamer
    Gst.init(None)
    
    # Create GStreamer pipeline
    if camera_url.startswith('rtsp://'):
        # RTSP pipeline
        pipeline_str = (
            f'rtspsrc location={camera_url} latency=0 ! '
            'rtph264depay ! h264parse ! avdec_h264 ! '
            'videoconvert ! video/x-raw,format=RGB ! '
            'appsink name=sink emit-signals=True sync=False'
        )
    elif camera_url.startswith('http://'):
        # HTTP pipeline for MJPEG streams
        pipeline_str = (
            f'souphttpsrc location={camera_url} ! '
            'jpegdec ! videoconvert ! video/x-raw,format=RGB ! '
            'appsink name=sink emit-signals=True sync=False'
        )
    else:
        # Try a generic pipeline
        pipeline_str = (
            f'uridecodebin uri={camera_url} ! '
            'videoconvert ! video/x-raw,format=RGB ! '
            'appsink name=sink emit-signals=True sync=False'
        )
    
    print(f"Using GStreamer pipeline: {pipeline_str}")
    pipeline = Gst.parse_launch(pipeline_str)
    
    # Get the sink element
    appsink = pipeline.get_by_name('sink')
    
    # Define callback for new samples
    frames = []
    
    def on_new_sample(sink):
        sample = sink.emit("pull-sample")
        if sample:
            buffer = sample.get_buffer()
            caps = sample.get_caps()
            structure = caps.get_structure(0)
            width = structure.get_value("width")
            height = structure.get_value("height")
            
            # Get frame data
            success, map_info = buffer.map(Gst.MapFlags.READ)
            if success:
                # Create numpy array from buffer data
                frame = np.ndarray(
                    shape=(height, width, 3),
                    dtype=np.uint8,
                    buffer=map_info.data
                )
                frames.append(frame)
                buffer.unmap(map_info)
            
            return Gst.FlowReturn.OK
        return Gst.FlowReturn.ERROR
    
    # Connect the callback
    appsink.connect("new-sample", on_new_sample)
    
    # Start playing
    pipeline.set_state(Gst.State.PLAYING)
    
    # Create a GLib main loop to process events
    loop = GLib.MainLoop()
    
    # Create a thread to run the loop
    import threading
    thread = threading.Thread(target=loop.run)
    thread.daemon = True
    thread.start()
    
    # Wait for frames
    start_time = time.time()
    timeout = 10  # 10 seconds timeout
    
    while len(frames) < frames_to_read and time.time() - start_time < timeout:
        time.sleep(0.1)
        print(f"Received {len(frames)}/{frames_to_read} frames")
    
    # Stop the pipeline
    pipeline.set_state(Gst.State.NULL)
    
    # Try to stop the loop
    if loop.is_running():
        loop.quit()
    
    return frames

# Example 2: Using VLC Python bindings
def vlc_camera_stream(camera_url, frames_to_read=10):
    """
    Access camera stream using VLC Python bindings
    """
    import vlc
    import time
    import numpy as np
    from PIL import Image
    import ctypes
    import tempfile
    import os
    
    # Create a VLC instance
    instance = vlc.Instance()
    
    # Create a media player
    player = instance.media_player_new()
    
    # Create a media from the camera URL
    media = instance.media_new(camera_url)
    
    # Set the media to the player
    player.set_media(media)
    
    # Set up a temporary directory for snapshots
    temp_dir = tempfile.mkdtemp()
    frames = []
    
    # Start playing
    player.play()
    
    # Wait for player to start
    time.sleep(2)
    
    # Take frames
    for i in range(frames_to_read):
        snapshot_path = os.path.join(temp_dir, f"frame_{i}.png")
        
        # Take snapshot
        player.video_take_snapshot(0, snapshot_path, 0, 0)
        
        # Wait for file to be created
        time.sleep(0.5)
        
        # Check if file exists
        if os.path.exists(snapshot_path):
            # Load the image
            img = Image.open(snapshot_path)
            frame = np.array(img)
            frames.append(frame)
            print(f"Captured frame {i+1}, shape: {frame.shape}")
        else:
            print(f"Failed to capture frame {i+1}")
    
    # Stop the player
    player.stop()
    
    # Clean up temp files
    for i in range(frames_to_read):
        try:
            os.remove(os.path.join(temp_dir, f"frame_{i}.png"))
        except:
            pass
    os.rmdir(temp_dir)
    
    return frames

# Example 3: Using FFmpeg Python wrapper
def ffmpeg_camera_stream(camera_url, frames_to_read=10, output_folder="frames"):
    """
    Access camera stream using FFmpeg Python wrapper
    """
    import subprocess
    import os
    import numpy as np
    from PIL import Image
    
    # Ensure output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Build FFmpeg command
    ffmpeg_cmd = [
        'ffmpeg',
        '-i', camera_url,
        '-frames:v', str(frames_to_read),
        '-vsync', '0',
        f'{output_folder}/frame_%03d.jpg'
    ]
    
    print(f"Running FFmpeg command: {' '.join(ffmpeg_cmd)}")
    
    # Run FFmpeg
    try:
        process = subprocess.run(
            ffmpeg_cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            timeout=20  # 20 seconds timeout
        )
        
        print(f"FFmpeg return code: {process.returncode}")
        if process.returncode != 0:
            print(f"FFmpeg error: {process.stderr.decode()}")
            return []
        
    except subprocess.TimeoutExpired:
        print("FFmpeg process timed out")
        return []
    
    # Load captured frames
    frames = []
    for i in range(1, frames_to_read + 1):
        frame_path = f"{output_folder}/frame_{i:03d}.jpg"
        if os.path.exists(frame_path):
            img = Image.open(frame_path)
            frame = np.array(img)
            frames.append(frame)
            print(f"Loaded frame {i}, shape: {frame.shape}")
        else:
            print(f"Missing frame at {frame_path}")
    
    return frames

# Example 4: Using imageio
def imageio_camera_stream(camera_url, frames_to_read=10):
    """
    Access camera stream using imageio
    """
    import imageio.v3 as iio
    import time
    
    try:
        # Open the stream
        print(f"Opening camera stream with imageio: {camera_url}")
        frames = []
        
        # For RTSP streams
        if camera_url.startswith('rtsp://'):
            # Use FFmpeg plugin
            with iio.imopen(camera_url, 'r', plugin='pyav') as file:
                for i, frame in enumerate(file):
                    if i >= frames_to_read:
                        break
                    frames.append(frame)
                    print(f"Read frame {i+1}, shape: {frame.shape}")
        else:
            # For HTTP MJPEG streams, read directly
            for i in range(frames_to_read):
                frame = iio.imread(camera_url)
                frames.append(frame)
                print(f"Read frame {i+1}, shape: {frame.shape}")
                time.sleep(0.1)  # Small delay between frames
                
        return frames
    
    except Exception as e:
        print(f"Error with imageio: {str(e)}")
        return []

# Example 5: Simple combined tool that tries multiple methods
def camera_stream_test(camera_url, frames_to_read=5):
    """
    Try multiple methods to connect to a camera stream and return frames
    """
    print(f"Testing camera stream: {camera_url}")
    
    methods = [
        ("imageio", test_imageio),
        ("GStreamer", test_gstreamer),
        ("VLC", test_vlc),
        ("FFmpeg", test_ffmpeg),
        ("OpenCV", test_opencv),
    ]
    
    for method_name, method_func in methods:
        print(f"\n--- Testing {method_name} ---")
        try:
            success, frames = method_func(camera_url, frames_to_read)
            if success:
                print(f"✅ {method_name} successfully captured {len(frames)} frames")
                return method_name, frames
            else:
                print(f"❌ {method_name} failed")
        except Exception as e:
            print(f"❌ {method_name} failed with error: {str(e)}")
    
    print("\n❌ All methods failed")
    return None, []

# Helper test functions
def test_opencv(camera_url, frames_to_read):
    import cv2
    frames = []
    cap = cv2.VideoCapture(camera_url)
    
    if not cap.isOpened():
        return False, []
    
    for _ in range(frames_to_read):
        ret, frame = cap.read()
        if not ret:
            break
        frames.append(frame)
    
    cap.release()
    return len(frames) > 0, frames

def test_gstreamer(camera_url, frames_to_read):
    try:
        frames = gstreamer_camera_stream(camera_url, frames_to_read)
        return len(frames) > 0, frames
    except Exception:
        return False, []

def test_vlc(camera_url, frames_to_read):
    try:
        frames = vlc_camera_stream(camera_url, frames_to_read)
        return len(frames) > 0, frames
    except Exception:
        return False, []

def test_ffmpeg(camera_url, frames_to_read):
    import tempfile
    temp_dir = tempfile.mkdtemp()
    try:
        frames = ffmpeg_camera_stream(camera_url, frames_to_read, temp_dir)
        return len(frames) > 0, frames
    except Exception:
        return False, []
    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

def test_imageio(camera_url, frames_to_read):
    try:
        frames = imageio_camera_stream(camera_url, frames_to_read)
        return len(frames) > 0, frames
    except Exception:
        return False, []

# Example usage
if __name__ == "__main__":
    
    
    camera_url = sys.argv[1] if len(sys.argv) > 1 else "http://88.53.197.250/axis-cgi/mjpg/video.cgi?resolution=320x240"
    
    # Try all methods
    method_name, frames = camera_stream_test(camera_url)
    
    if frames:
        print(f"\nSuccessfully captured frames using {method_name}")
        # Display the first frame
        plt.imshow(frames[0])
        plt.title(f"First frame from {camera_url} using {method_name}")
        plt.show()
    else:
        print("\nFailed to capture any frames from the camera stream")

