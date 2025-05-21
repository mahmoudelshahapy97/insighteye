#####################################
# import gi
# gi.require_version('Gst', '1.0')
# from gi.repository import Gst, GLib
import vlc
import ctypes
import imageio.v3 as iio
import streamlit as st
import os
import cv2
import numpy as np
import subprocess
import tempfile
import shutil
import threading
from PIL import Image
import sys
import matplotlib.pyplot as plt
import time

st.set_page_config(page_title="Camera Stream Viewer", layout="wide")
st.title("Camera Stream Viewer")
st.write("Try different libraries to access camera streams")

# Example 1: Using GStreamer Python bindings directly
def gstreamer_camera_stream(camera_url, frames_to_read=10):
    """
    Access camera stream using GStreamer Python bindings
    """
    try:
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
        
        st.text(f"Using GStreamer pipeline: {pipeline_str}")
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
        thread = threading.Thread(target=loop.run)
        thread.daemon = True
        thread.start()
        
        # Wait for frames
        start_time = time.time()
        timeout = 10  # 10 seconds timeout
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        while len(frames) < frames_to_read and time.time() - start_time < timeout:
            time.sleep(0.1)
            progress = min(len(frames) / frames_to_read, 1.0)
            progress_bar.progress(progress)
            status_text.text(f"Received {len(frames)}/{frames_to_read} frames")
        
        # Stop the pipeline
        pipeline.set_state(Gst.State.NULL)
        
        # Try to stop the loop
        if loop.is_running():
            loop.quit()
        
        return frames
    except Exception as e:
        st.error(f"GStreamer error: {str(e)}")
        return []

# Example 2: Using VLC Python bindings
def vlc_camera_stream(camera_url, frames_to_read=10):
    """
    Access camera stream using VLC Python bindings
    """
    try:
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
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(frames_to_read):
            snapshot_path = os.path.join(temp_dir, f"frame_{i}.png")
            
            # Take snapshot
            player.video_take_snapshot(0, snapshot_path, 0, 0)
            
            # Wait for file to be created
            time.sleep(0.5)
            
            # Update progress
            progress_bar.progress((i + 1) / frames_to_read)
            
            # Check if file exists
            if os.path.exists(snapshot_path):
                # Load the image
                img = Image.open(snapshot_path)
                frame = np.array(img)
                frames.append(frame)
                status_text.text(f"Captured frame {i+1}, shape: {frame.shape}")
            else:
                status_text.text(f"Failed to capture frame {i+1}")
        
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
    except Exception as e:
        st.error(f"VLC error: {str(e)}")
        return []

# Example 3: Using FFmpeg Python wrapper
def ffmpeg_camera_stream(camera_url, frames_to_read=10):
    """
    Access camera stream using FFmpeg Python wrapper
    """
    try:
        import subprocess
        import os
        import numpy as np
        from PIL import Image
        
        # Create temporary output folder
        output_folder = tempfile.mkdtemp()
        
        # Build FFmpeg command
        ffmpeg_cmd = [
            'ffmpeg',
            '-i', camera_url,
            '-frames:v', str(frames_to_read),
            '-vsync', '0',
            f'{output_folder}/frame_%03d.jpg'
        ]
        
        st.text(f"Running FFmpeg command: {' '.join(ffmpeg_cmd)}")
        
        # Run FFmpeg
        try:
            process = subprocess.run(
                ffmpeg_cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                timeout=20  # 20 seconds timeout
            )
            
            if process.returncode != 0:
                st.error(f"FFmpeg error: {process.stderr.decode()}")
                return []
            
        except subprocess.TimeoutExpired:
            st.error("FFmpeg process timed out")
            return []
        
        # Load captured frames
        frames = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(1, frames_to_read + 1):
            frame_path = f"{output_folder}/frame_{i:03d}.jpg"
            if os.path.exists(frame_path):
                img = Image.open(frame_path)
                frame = np.array(img)
                frames.append(frame)
                progress_bar.progress(i / frames_to_read)
                status_text.text(f"Loaded frame {i}, shape: {frame.shape}")
            else:
                status_text.text(f"Missing frame at {frame_path}")
        
        # Clean up
        shutil.rmtree(output_folder, ignore_errors=True)
        
        return frames
    except Exception as e:
        st.error(f"FFmpeg error: {str(e)}")
        return []

# Example 4: Using imageio
def imageio_camera_stream(camera_url, frames_to_read=10):
    """
    Access camera stream using imageio
    """
    try:
        import imageio.v3 as iio
        import time
        
        # Open the stream
        st.text(f"Opening camera stream with imageio: {camera_url}")
        frames = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # For RTSP streams
        if camera_url.startswith('rtsp://'):
            # Use FFmpeg plugin
            with iio.imopen(camera_url, 'r', plugin='pyav') as file:
                for i, frame in enumerate(file):
                    if i >= frames_to_read:
                        break
                    frames.append(frame)
                    progress_bar.progress((i + 1) / frames_to_read)
                    status_text.text(f"Read frame {i+1}, shape: {frame.shape}")
        else:
            # For HTTP MJPEG streams, read directly
            for i in range(frames_to_read):
                frame = iio.imread(camera_url)
                frames.append(frame)
                progress_bar.progress((i + 1) / frames_to_read)
                status_text.text(f"Read frame {i+1}, shape: {frame.shape}")
                time.sleep(0.1)  # Small delay between frames
                
        return frames
    
    except Exception as e:
        st.error(f"Error with imageio: {str(e)}")
        return []

# Example 5: Using OpenCV
def opencv_camera_stream(camera_url, frames_to_read=10):
    """
    Access camera stream using OpenCV
    """
    try:
        import cv2
        
        frames = []
        cap = cv2.VideoCapture(camera_url)
        
        if not cap.isOpened():
            st.error("Could not open video stream with OpenCV")
            return []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(frames_to_read):
            ret, frame = cap.read()
            if not ret:
                break
            # Convert BGR to RGB
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            frames.append(frame)
            progress_bar.progress((i + 1) / frames_to_read)
            status_text.text(f"Read frame {i+1}, shape: {frame.shape}")
        
        cap.release()
        return frames
    
    except Exception as e:
        st.error(f"Error with OpenCV: {str(e)}")
        return []

# Helper functions for testing methods
def test_opencv(camera_url, frames_to_read):
    with st.spinner("Testing OpenCV..."):
        frames = opencv_camera_stream(camera_url, frames_to_read)
        return len(frames) > 0, frames

def test_gstreamer(camera_url, frames_to_read):
    with st.spinner("Testing GStreamer..."):
        try:
            frames = gstreamer_camera_stream(camera_url, frames_to_read)
            return len(frames) > 0, frames
        except Exception as e:
            st.error(f"GStreamer error: {str(e)}")
            return False, []

def test_vlc(camera_url, frames_to_read):
    with st.spinner("Testing VLC..."):
        try:
            frames = vlc_camera_stream(camera_url, frames_to_read)
            return len(frames) > 0, frames
        except Exception as e:
            st.error(f"VLC error: {str(e)}")
            return False, []

def test_ffmpeg(camera_url, frames_to_read):
    with st.spinner("Testing FFmpeg..."):
        try:
            frames = ffmpeg_camera_stream(camera_url, frames_to_read)
            return len(frames) > 0, frames
        except Exception as e:
            st.error(f"FFmpeg error: {str(e)}")
            return False, []

def test_imageio(camera_url, frames_to_read):
    with st.spinner("Testing imageio..."):
        try:
            frames = imageio_camera_stream(camera_url, frames_to_read)
            return len(frames) > 0, frames
        except Exception as e:
            st.error(f"imageio error: {str(e)}")
            return False, []

# Combined tool that tries multiple methods
def camera_stream_test(camera_url, frames_to_read=5):
    """
    Try multiple methods to connect to a camera stream and return frames
    """
    st.subheader(f"Testing camera stream: {camera_url}")
    
    # Define the methods to try
    methods = []
    
    if use_imageio:
        methods.append(("imageio", test_imageio))
    if use_gstreamer:
        methods.append(("GStreamer", test_gstreamer))
    if use_vlc:
        methods.append(("VLC", test_vlc))
    if use_ffmpeg:
        methods.append(("FFmpeg", test_ffmpeg))
    if use_opencv:
        methods.append(("OpenCV", test_opencv))
    
    for method_name, method_func in methods:
        st.write(f"--- Testing {method_name} ---")
        try:
            success, frames = method_func(camera_url, frames_to_read)
            if success:
                st.success(f"{method_name} successfully captured {len(frames)} frames")
                return method_name, frames
            else:
                st.error(f"{method_name} failed")
        except Exception as e:
            st.error(f"{method_name} failed with error: {str(e)}")
    
    st.error("All methods failed")
    return None, []

# Main app UI
st.sidebar.header("Settings")

# Camera URL input
default_camera_url = "http://88.53.197.250/axis-cgi/mjpg/video.cgi?resolution=320x240"
camera_url = st.sidebar.text_input("Camera URL", value=default_camera_url)

# Number of frames to capture
frames_to_read = st.sidebar.slider("Number of frames to capture", 1, 30, 5)

# Method selection
st.sidebar.subheader("Methods to try")
use_imageio = st.sidebar.checkbox("imageio", value=True)
use_gstreamer = st.sidebar.checkbox("GStreamer", value=True)
use_vlc = st.sidebar.checkbox("VLC", value=True)
use_ffmpeg = st.sidebar.checkbox("FFmpeg", value=True)
use_opencv = st.sidebar.checkbox("OpenCV", value=True)

# Specific method selection
st.sidebar.subheader("Or try a specific method")
specific_method = st.sidebar.radio(
    "Select a specific method",
    ("None", "imageio", "GStreamer", "VLC", "FFmpeg", "OpenCV")
)

# Button to start capture
if st.sidebar.button("Start Capture"):
    if specific_method == "None":
        # Try all selected methods
        method_name, frames = camera_stream_test(camera_url, frames_to_read)
    else:
        # Try specific method
        st.subheader(f"Testing {specific_method} method")
        
        if specific_method == "imageio":
            success, frames = test_imageio(camera_url, frames_to_read)
        elif specific_method == "GStreamer":
            success, frames = test_gstreamer(camera_url, frames_to_read)
        elif specific_method == "VLC":
            success, frames = test_vlc(camera_url, frames_to_read)
        elif specific_method == "FFmpeg":
            success, frames = test_ffmpeg(camera_url, frames_to_read)
        elif specific_method == "OpenCV":
            success, frames = test_opencv(camera_url, frames_to_read)
        
        method_name = specific_method if success else None
    
    # Display results
    if method_name and len(frames) > 0:
        st.success(f"Successfully captured {len(frames)} frames using {method_name}")
        
        # Create columns for displaying frames
        cols = st.columns(min(3, len(frames)))
        
        # Display the first frames
        for i, frame in enumerate(frames[:6]):  # Show up to 6 frames
            col_idx = i % len(cols)
            with cols[col_idx]:
                st.image(frame, caption=f"Frame {i+1}", use_column_width=True)
                
        # Show video animation
        if len(frames) > 1 and st.button("Play as animation"):
            st.subheader("Video Animation")
            # Convert frames to format expected by st.image
            animation = st.empty()
            
            # Loop through frames a few times
            for _ in range(3):
                for frame in frames:
                    animation.image(frame)
                    time.sleep(0.2)
    else:
        st.error("Failed to capture any frames from the camera stream")

# Add requirements section
st.sidebar.markdown("---")
st.sidebar.subheader("Requirements")
requirements = """
- streamlit
- opencv-python
- numpy
- matplotlib
- pillow
- imageio
- imageio-ffmpeg
"""
st.sidebar.code(requirements)

# Add instructions for installing optional dependencies
st.sidebar.markdown("Optional dependencies:")
st.sidebar.code("""
# For GStreamer
pip install PyGObject

# For VLC
pip install python-vlc
""")

# Footer
st.sidebar.markdown("---")
st.sidebar.info("Camera Stream Viewer App")

