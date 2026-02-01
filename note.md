python - <<'EOF'
import torch
print(torch.__version__)
print("CUDA available:", torch.cuda.is_available())
print("CUDA version:", torch.version.cuda)
print("GPU:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "None")
EOF


python - <<'EOF'
import torch

device = "cuda" if torch.cuda.is_available() else "cpu"
print("Using device:", device)

watch -n 1 nvidia-smi

docker run --rm --gpus all nvidia/cuda:12.1.0-base-ubuntu22.04 nvidia-smi

sudo journalctl -n 100 -f

sudo apt update
sudo apt install -y nvidia-driver-535

sudo reboot

curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
 | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit.gpg

curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
 | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit.gpg] https://#g' \
 | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt update
sudo apt install -y nvidia-container-toolkit

sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker


sudo apt install nvidia-cuda-toolkit
nvcc --version


docker run --gpus all -p 8000:8000 your_image_name



Download * TensorRT 8.6 GA for Ubuntu 22.04 and CUDA 12.0 and 12.1 DEB local repo Package
sudo dpkg -i nv-tensorrt-local-repo-ubuntu2204-8.6.1-cuda-12.0_1.0-1_amd64.deb 
sudo cp /var/nv-tensorrt-local-repo-ubuntu2204-8.6.1-cuda-12.0/nv-tensorrt-local-42B2FC56-keyring.gpg /usr/share/keyrings/
sudo apt-get update
sudo apt-get install tensorrt



from ultralytics import YOLO
import torch

print('torch:', torch.__version__, torch.version.cuda)

models = ['models/gender.pt', 'models/people.pt', 'models/fire.pt']

for path in models:
    print(f'\n--- Converting {path} ---')
    model = YOLO(path)
    result = model.export(
        format='engine',
        imgsz=640,
        half=True,
        batch=1,
        device=0,
    )
    print(f'✅ Saved: {result}')




xxd models/people.engine | head -3
echo '---'
python -c \"
from app.services.model_loader import ModelFactory
loader = ModelFactory.create_loader('models/people.engine')
loader.load_model()
print('✅ people.engine loaded successfully')


docker exec -it insighteye_app bash -c "
file models/people.engine
xxd models/people.engine | head -5
md5sum models/people.engine

# Check if ultralytics saved it somewhere else
find /app -name '*.engine' -newer /app/models/people.pt -ls
find /tmp -name '*.engine' -ls 2>/dev/null
find /root -name '*.engine' -ls 2>/dev/null
"