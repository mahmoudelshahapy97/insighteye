#!/bin/bash
set -e

# Variables
APP_REPO="https://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@github.com/mahmoudelshahawy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"
DOCKER_COMPOSE_VERSION="v2.27.0"

export DEBIAN_FRONTEND=noninteractive

echo "Installing Dependencies..."
sudo apt-get update
sudo apt-get install -y docker.io docker-compose git git-lfs nginx

sudo systemctl enable --now docker

echo "Cloning Repo..."
cd /home/ubuntu
sudo -u ubuntu git lfs install

if [ -d "$APP_DIR" ]; then
    cd $APP_DIR && sudo -u ubuntu git pull || true
else
    sudo -u ubuntu git clone $APP_REPO $APP_DIR
    cd $APP_DIR
fi

sudo -u ubuntu git lfs pull
sudo chown -R ubuntu:ubuntu $APP_DIR

echo "Starting Docker..."
cd $APP_DIR
sudo docker-compose down -v || true
sudo docker-compose up -d --build

echo "Configuring Nginx..."
sudo bash -c 'cat >/etc/nginx/sites-available/insighteye <<EOF
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
EOF'

sudo rm -f /etc/nginx/sites-enabled/default
sudo ln -s /etc/nginx/sites-available/insighteye /etc/nginx/sites-enabled/

sudo nginx -t && sudo systemctl restart nginx

echo "✅ Deployment Completed!"
