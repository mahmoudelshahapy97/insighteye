# We use the devel image to ensure all headers/libs are present
FROM nvidia/cuda:12.2.2-devel-ubuntu22.04

WORKDIR /app

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# ===============================
# System Dependencies
# ===============================
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git pkg-config \
    libjpeg-dev libpng-dev libtiff-dev \
    libavcodec-dev libavformat-dev libswscale-dev \
    libgtk-3-dev libcanberra-gtk3-dev \
    libxvidcore-dev libx264-dev \
    libatlas-base-dev gfortran \
    python3-dev python3-pip python3-numpy \
    libgl1 libglib2.0-0 ffmpeg curl \
    && rm -rf /var/lib/apt/lists/*

# ===============================
# UV and Python Dependencies
# ===============================
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv

COPY requirements-base.txt requirements-gpu.txt ./

# Install requirements step-by-step to reduce peak disk space usage during extraction
RUN uv pip install --system --no-cache -r requirements-base.txt
RUN uv pip install --system --no-cache -r requirements-gpu.txt
# RUN uv pip install --system --no-cache nvidia-cudnn-cu12

# ===============================
# OpenCV with CUDA
# ===============================
RUN uv pip install --system --no-cache \
    https://github.com/cudawarped/opencv-python-cuda-wheels/releases/download/4.10.0.84/opencv_contrib_python-4.10.0.84-cp37-abi3-linux_x86_64.whl

# ===============================
# Library Path Configuration
# ===============================
# This tells the system where to find libcudnn.so.9 (from pip) and CUDA libs
ENV LD_LIBRARY_PATH=/usr/local/lib/python3.10/dist-packages/nvidia/cudnn/lib:/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:$LD_LIBRARY_PATH

RUN ldconfig

# Copy application code
COPY . .

EXPOSE 8001

# IMPORTANT: We removed the 'RUN python3 -c "import cv2"' line.
# It will always fail during BUILD because there is no GPU driver access.
# It will work at RUNTIME when you use --gpus all.

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]