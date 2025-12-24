# config.yaml - Main configuration file
streams:
  output_folder: "output"
  skip_frames: 100
  reconnect_delay: 5
  timeout: 10
  buffer_size: 30
  jpeg_quality: 95
  max_reconnect_attempts: -1  # -1 for unlimited
  health_check_interval: 60
  
logging:
  log_dir: "logs"
  max_bytes: 10485760  # 10MB
  backup_count: 5
  level: "INFO"

monitoring:
  status_interval: 60  # seconds
  metrics_enabled: true
  
cameras:
  - name: "port5511_ch01"
    url: "rtsp://admin:12345678@41.178.2.61:5511/ch01/0"
    enabled: true
  - name: "port5511_ch02"
    url: "rtsp://admin:12345678@41.178.2.61:5511/ch02/0"
    enabled: true
  # Add all other cameras...

---
# docker-compose.yml
version: '3.8'

services:
  rtsp-streamer:
    build: .
    container_name: rtsp-stream-reader
    restart: unless-stopped
    volumes:
      - ./output:/app/output
      - ./logs:/app/logs
      - ./config.yaml:/app/config.yaml:ro
    environment:
      - TZ=Africa/Cairo
      - PYTHONUNBUFFERED=1
    network_mode: "host"
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    deploy:
      resources:
        limits:
          cpus: '4.0'
          memory: 4G
        reservations:
          cpus: '2.0'
          memory: 2G

---
# Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY rtsp_stream_reader.py .
COPY config.yaml .

# Create directories
RUN mkdir -p /app/output /app/logs

# Run as non-root user
RUN useradd -m -u 1000 streamer && \
    chown -R streamer:streamer /app
USER streamer

CMD ["python", "rtsp_stream_reader.py"]

---
# requirements.txt
opencv-python-headless==4.8.1.78
numpy==1.24.3
PyYAML==6.0.1

---
# .env.example
# Copy to .env and fill in your values
RTSP_USERNAME=admin
RTSP_PASSWORD=12345678
RTSP_IP=41.178.2.61
TZ=Africa/Cairo

---
# systemd service file: /etc/systemd/system/rtsp-stream.service
[Unit]
Description=RTSP Stream Reader Service
After=network.target

[Service]
Type=simple
User=rtsp-user
WorkingDirectory=/opt/rtsp-stream
ExecStart=/usr/bin/python3 /opt/rtsp-stream/rtsp_stream_reader.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/rtsp-stream/output.log
StandardError=append:/var/log/rtsp-stream/error.log

# Resource limits
LimitNOFILE=65536
MemoryLimit=4G
CPUQuota=400%

[Install]
WantedBy=multi-user.target

---
# Monitoring script: monitor.sh
#!/bin/bash

CONTAINER_NAME="rtsp-stream-reader"
ALERT_EMAIL="admin@example.com"
LOG_FILE="/var/log/rtsp-monitor.log"

check_container() {
    if ! docker ps | grep -q $CONTAINER_NAME; then
        echo "[$(date)] Container $CONTAINER_NAME is not running. Restarting..." >> $LOG_FILE
        docker-compose up -d
        # Send alert
        echo "RTSP Stream container restarted at $(date)" | mail -s "RTSP Alert" $ALERT_EMAIL
    fi
}

check_disk_space() {
    USAGE=$(df -h /path/to/output | tail -1 | awk '{print $5}' | sed 's/%//')
    if [ $USAGE -gt 85 ]; then
        echo "[$(date)] Disk usage is $USAGE%. Cleaning old files..." >> $LOG_FILE
        find /path/to/output -name "*.jpg" -mtime +7 -delete
    fi
}

check_container
check_disk_space

---
# Cleanup script: cleanup.sh
#!/bin/bash

# Delete files older than 7 days
RETENTION_DAYS=7
OUTPUT_DIR="./output"

echo "Cleaning up files older than $RETENTION_DAYS days..."
find $OUTPUT_DIR -name "*.jpg" -mtime +$RETENTION_DAYS -delete
find $OUTPUT_DIR -name "*.json" -mtime +$RETENTION_DAYS -delete

# Compress old logs
find ./logs -name "*.log.*" -mtime +1 -exec gzip {} \;
find ./logs -name "*.log.*.gz" -mtime +30 -delete

echo "Cleanup complete"