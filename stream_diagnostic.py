#!/usr/bin/env python3
import cv2
import logging
import sys
import os
import tempfile
import subprocess
import time
import argparse
from datetime import datetime

def check_ffmpeg_installed():
    """Check if ffmpeg is installed and available"""
    try:
        subprocess.run(["ffmpeg", "-version"], 
                       stdout=subprocess.PIPE, 
                       stderr=subprocess.PIPE, 
                       check=True)
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        return False

def test_stream_with_ffmpeg(stream_url, output_file=None, duration=5):
    """Test stream with ffmpeg directly"""
    if not output_file:
        fd, output_file = tempfile.mkstemp(suffix='.mp4')
        os.close(fd)
    
    print(f"Testing stream with ffmpeg: {stream_url}")
    print(f"Recording {duration} seconds to {output_file}")
    
    cmd = [
        "ffmpeg",
        "-y",  # Overwrite output file
        "-v", "info",  # Verbose output
        "-fflags", "nobuffer",  # Reduce buffering
        "-flags", "low_delay",  # Low delay mode
        "-strict", "experimental",  # Allow experimental codecs
        "-timeout", "5000000",  # 5 second timeout (in microseconds)
        "-i", stream_url,  # Input stream
        "-t", str(duration),  # Duration in seconds
        "-c:v", "copy",  # Copy video stream without re-encoding
        output_file  # Output file
    ]
    
    try:
        process = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Check if file was created and has size > 0
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            print(f"✅ Successfully recorded stream to {output_file}")
            print(f"   File size: {os.path.getsize(output_file)} bytes")
            return True, output_file
        else:
            print(f"❌ Failed to record stream - empty or missing output file")
            print(f"FFmpeg stderr: {process.stderr}")
            return False, None
    except Exception as e:
        print(f"❌ Error testing stream with ffmpeg: {e}")
        return False, None

def test_stream_with_opencv(stream_url, frames_to_test=30, save_frames=False):
    """Test stream with OpenCV"""
    print(f"Testing stream with OpenCV: {stream_url}")
    
    # List of connection methods to try
    connection_methods = [
        {"name": "Default", "backend": cv2.CAP_ANY, "url": stream_url},
        {"name": "FFMPEG", "backend": cv2.CAP_FFMPEG, "url": stream_url},
        {"name": "FFMPEG with options", "backend": cv2.CAP_FFMPEG, 
         "url": f"{stream_url}{'&' if '?' in stream_url else '?'}fflags=nobuffer&flags=low_delay&strict=experimental&c:v=mjpeg"}
    ]
    
    # Try each connection method
    for method in connection_methods:
        print(f"\nTrying connection method: {method['name']}")
        cap = None
        try:
            # Open capture
            cap = cv2.VideoCapture(method['url'], method['backend'])
            
            # Try to set minimal buffer size
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            
            # Check if opened
            if not cap.isOpened():
                print(f"❌ Failed to open stream with {method['name']}")
                continue
            
            # Try reading frames
            frames_read = 0
            start_time = time.time()
            
            # Create directory for frames if saving
            frames_dir = None
            if save_frames:
                frames_dir = f"frames_{int(time.time())}"
                os.makedirs(frames_dir, exist_ok=True)
                print(f"Saving frames to directory: {frames_dir}")
            
            # Read frames
            for i in range(frames_to_test):
                ret, frame = cap.read()
                if not ret or frame is None:
                    print(f"❌ Failed to read frame {i+1}")
                    break
                
                frames_read += 1
                
                # Save frame if requested
                if save_frames and frames_dir:
                    frame_file = os.path.join(frames_dir, f"frame_{i+1:04d}.jpg")
                    cv2.imwrite(frame_file, frame)
                
                # Print progress
                if (i+1) % 5 == 0:
                    print(f"✅ Read {i+1}/{frames_to_test} frames")
            
            # Calculate FPS
            elapsed = time.time() - start_time
            fps = frames_read / elapsed if elapsed > 0 else 0
            
            print(f"\nResults for {method['name']}:")
            print(f"✅ Successfully read {frames_read}/{frames_to_test} frames")
            print(f"⏱️ Elapsed time: {elapsed:.2f} seconds")
            print(f"🔄 FPS: {fps:.2f}")
            
            if frames_read == frames_to_test:
                print(f"✅ Successfully read all {frames_to_test} frames with {method['name']}")
                
                # Get camera properties
                width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                fps = cap.get(cv2.CAP_PROP_FPS)
                
                print(f"📊 Stream properties:")
                print(f"   Resolution: {width}x{height}")
                print(f"   FPS: {fps}")
                
                # Close capture
                if cap:
                    cap.release()
                
                return True, method
            
        except Exception as e:
            print(f"❌ Error with {method['name']}: {e}")
        finally:
            # Close capture
            if cap:
                cap.release()
    
    print("\n❌ Failed to read frames with any connection method")
    return False, None

def run_diagnostic(url, save_frames=False, use_ffmpeg=True):
    """Run comprehensive diagnostic on stream URL"""
    print(f"🔍 Running comprehensive diagnostic on: {url}")
    print(f"⏱️ Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔧 OpenCV version: {cv2.__version__}")
    
    # Test with OpenCV
    print("\n=== Testing with OpenCV ===")
    opencv_success, opencv_method = test_stream_with_opencv(url, save_frames=save_frames)
    
    # Test with FFmpeg if available and requested
    ffmpeg_success = False
    if use_ffmpeg:
        if check_ffmpeg_installed():
            print("\n=== Testing with FFmpeg ===")
            ffmpeg_success, output_file = test_stream_with_ffmpeg(url)
        else:
            print("\n❌ FFmpeg not installed - skipping FFmpeg test")
    
    # Print summary
    print("\n=== Diagnostic Summary ===")
    print(f"Stream URL: {url}")
    print(f"OpenCV test: {'✅ PASS' if opencv_success else '❌ FAIL'}")
    if opencv_success:
        print(f"   Best method: {opencv_method['name']}")
        print(f"   URL used: {opencv_method['url']}")
    
    if use_ffmpeg:
        if check_ffmpeg_installed():
            print(f"FFmpeg test: {'✅ PASS' if ffmpeg_success else '❌ FAIL'}")
        else:
            print("FFmpeg test: ⚠️ SKIPPED (not installed)")
    
    # Provide recommendations
    print("\n=== Recommendations ===")
    if opencv_success:
        print("✅ OpenCV can successfully connect to the stream")
        print(f"   Use the following approach:")
        print(f"   - Backend: {opencv_method['name']}")
        print(f"   - URL: {opencv_method['url']}")
    elif ffmpeg_success:
        print("⚠️ FFmpeg can connect but OpenCV cannot")
        print("   Try using FFmpeg for preprocessing or as a backend")
    else:
        print("❌ Could not connect to stream with any method")
        print("   Recommendations:")
        print("   - Check that the URL is correct")
        print("   - Verify network connectivity to the camera")
        print("   - Check camera is powered on and streaming")
        print("   - Try accessing the camera directly via browser")
        print("   - Check if authentication is required")
    
    print(f"\n⏱️ End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Diagnose camera stream issues")
    parser.add_argument("url", help="Camera stream URL")
    parser.add_argument("--save-frames", action="store_true", help="Save frames to disk")
    parser.add_argument("--no-ffmpeg", action="store_true", help="Skip FFmpeg tests")
    
    args = parser.parse_args()
    
    run_diagnostic(args.url, save_frames=args.save_frames, use_ffmpeg=not args.no_ffmpeg)
    