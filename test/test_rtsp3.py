#!/usr/bin/env python3
"""
RTSP Stream Diagnostic Tool
Run this to test your RTSP connection: python rtsp_diagnostic.py
"""

import cv2
import socket
import subprocess
import time
import sys
import os
from urllib.parse import urlparse

# Your RTSP URL
RTSP_URL = "rtsp://admin:12345678@41.178.2.61:554/ch12/0"

def print_header(text):
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}\n")

def test_1_parse_url():
    """Test 1: Parse RTSP URL"""
    print_header("Test 1: Parsing RTSP URL")
    
    try:
        parsed = urlparse(RTSP_URL)
        print(f"✓ Scheme: {parsed.scheme}")
        print(f"✓ Host: {parsed.hostname}")
        print(f"✓ Port: {parsed.port or 554}")
        print(f"✓ Path: {parsed.path}")
        print(f"✓ Username: {parsed.username}")
        print(f"✓ Password: {'*' * len(parsed.password) if parsed.password else 'None'}")
        return True, parsed.hostname, parsed.port or 554
    except Exception as e:
        print(f"✗ Failed to parse URL: {e}")
        return False, None, None

def test_2_network_connectivity(host, port):
    """Test 2: Check network connectivity"""
    print_header(f"Test 2: Network Connectivity to {host}:{port}")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✓ Successfully connected to {host}:{port}")
            return True
        else:
            print(f"✗ Cannot connect to {host}:{port} (error code: {result})")
            print("  Possible issues:")
            print("  - Camera is offline or unreachable")
            print("  - Firewall blocking connection")
            print("  - Wrong IP address or port")
            return False
    except socket.timeout:
        print(f"✗ Connection timeout after 5 seconds")
        return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False

def test_3_ping(host):
    """Test 3: Ping the host"""
    print_header(f"Test 3: Ping {host}")
    
    try:
        # Different ping command for Windows vs Unix
        param = '-n' if sys.platform.startswith('win') else '-c'
        command = ['ping', param, '3', host]
        
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print(f"✓ Host {host} is reachable")
            print(result.stdout)
            return True
        else:
            print(f"✗ Host {host} is not reachable")
            print(result.stderr)
            return False
    except subprocess.TimeoutExpired:
        print(f"✗ Ping timeout")
        return False
    except Exception as e:
        print(f"✗ Ping error: {e}")
        return False

def test_4_ffprobe():
    """Test 4: Test with ffprobe"""
    print_header("Test 4: Testing with ffprobe")
    
    try:
        # Hide credentials in display
        display_url = RTSP_URL.replace(
            RTSP_URL.split('@')[0].split('//')[1],
            '***:***'
        )
        print(f"Testing: {display_url}")
        
        cmd = [
            'ffprobe',
            '-v', 'error',
            '-rtsp_transport', 'tcp',
            '-timeout', '15000000',  # 15 seconds
            '-i', RTSP_URL,
            '-show_entries', 'stream=codec_type,codec_name,width,height',
            '-of', 'default=noprint_wrappers=1'
        ]
        
        print("Running ffprobe (this may take 15-30 seconds)...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✓ Stream is valid!")
            print("\nStream information:")
            print(result.stdout)
            return True
        else:
            print("✗ ffprobe failed")
            print("\nError output:")
            print(result.stderr)
            return False
            
    except FileNotFoundError:
        print("✗ ffprobe not found. Install ffmpeg:")
        print("  Ubuntu/Debian: sudo apt-get install ffmpeg")
        print("  macOS: brew install ffmpeg")
        print("  Windows: Download from ffmpeg.org")
        return False
    except subprocess.TimeoutExpired:
        print("✗ ffprobe timeout after 30 seconds")
        print("  This usually means:")
        print("  - Camera is not responding")
        print("  - Wrong credentials")
        print("  - Network issues")
        return False
    except Exception as e:
        print(f"✗ ffprobe error: {e}")
        return False

def test_5_opencv_tcp():
    """Test 5: OpenCV with TCP transport"""
    print_header("Test 5: Testing with OpenCV (TCP transport)")
    
    try:
        # Set FFmpeg options for TCP
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
            'rtsp_transport;tcp|'
            'timeout;30000000|'
            'stimeout;5000000'
        )
        
        print("Opening video capture with TCP transport...")
        print("(This may take 30-60 seconds)")
        
        cap = cv2.VideoCapture(RTSP_URL, cv2.CAP_FFMPEG)
        
        if not cap.isOpened():
            print("✗ Failed to open stream")
            return False
        
        print("✓ Stream opened, attempting to read frame...")
        
        # Try to read a frame
        ret, frame = cap.read()
        
        if ret and frame is not None:
            print(f"✓ Successfully read frame!")
            print(f"  Frame size: {frame.shape[1]}x{frame.shape[0]}")
            print(f"  Channels: {frame.shape[2]}")
            
            # Try to save a test frame
            cv2.imwrite('rtsp_test_frame.jpg', frame)
            print(f"  Test frame saved as: rtsp_test_frame.jpg")
            
            cap.release()
            return True
        else:
            print("✗ Failed to read frame")
            cap.release()
            return False
            
    except Exception as e:
        print(f"✗ OpenCV error: {e}")
        return False

def test_6_opencv_udp():
    """Test 6: OpenCV with UDP transport (fallback)"""
    print_header("Test 6: Testing with OpenCV (UDP transport)")
    
    try:
        # Set FFmpeg options for UDP
        os.environ['OPENCV_FFMPEG_CAPTURE_OPTIONS'] = (
            'rtsp_transport;udp|'
            'timeout;30000000'
        )
        
        print("Opening video capture with UDP transport...")
        
        cap = cv2.VideoCapture(RTSP_URL, cv2.CAP_FFMPEG)
        
        if not cap.isOpened():
            print("✗ Failed to open stream")
            return False
        
        print("✓ Stream opened, attempting to read frame...")
        
        ret, frame = cap.read()
        
        if ret and frame is not None:
            print(f"✓ Successfully read frame with UDP!")
            print(f"  Frame size: {frame.shape[1]}x{frame.shape[0]}")
            cap.release()
            return True
        else:
            print("✗ Failed to read frame")
            cap.release()
            return False
            
    except Exception as e:
        print(f"✗ OpenCV error: {e}")
        return False

def main():
    print_header("RTSP Stream Diagnostic Tool")
    print(f"Testing RTSP URL: {RTSP_URL.split('@')[0]}@***")
    
    results = {}
    
    # Test 1: Parse URL
    success, host, port = test_1_parse_url()
    results['parse_url'] = success
    
    if not success:
        print("\n❌ Cannot proceed without valid URL")
        return
    
    # Test 2: Network connectivity
    results['network'] = test_2_network_connectivity(host, port)
    
    # Test 3: Ping
    results['ping'] = test_3_ping(host)
    
    # Test 4: ffprobe
    results['ffprobe'] = test_4_ffprobe()
    
    # Test 5: OpenCV TCP
    results['opencv_tcp'] = test_5_opencv_tcp()
    
    # Test 6: OpenCV UDP (only if TCP failed)
    if not results['opencv_tcp']:
        results['opencv_udp'] = test_6_opencv_udp()
    
    # Summary
    print_header("Diagnostic Summary")
    
    for test_name, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{test_name:20s}: {status}")
    
    # Recommendations
    print_header("Recommendations")
    
    if not results['network']:
        print("❌ CRITICAL: Cannot reach camera")
        print("   1. Check camera IP address and port")
        print("   2. Verify camera is powered on and connected")
        print("   3. Check firewall settings")
        print("   4. Try accessing camera from same network")
    elif not results.get('ffprobe', False) and not results.get('opencv_tcp', False):
        print("❌ CRITICAL: Can reach camera but stream fails")
        print("   1. Verify RTSP credentials (username/password)")
        print("   2. Check RTSP path (/ch12/0)")
        print("   3. Verify RTSP service is enabled on camera")
        print("   4. Check if camera allows multiple connections")
        print("   5. Try accessing stream with VLC or another player")
    elif results.get('opencv_tcp', False):
        print("✅ Stream is working!")
        print("   Your application should be able to connect")
        print("   Use TCP transport for best reliability")
    elif results.get('opencv_udp', False):
        print("⚠️  Stream works with UDP but not TCP")
        print("   UDP may be less reliable")
        print("   Consider checking camera's TCP settings")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()