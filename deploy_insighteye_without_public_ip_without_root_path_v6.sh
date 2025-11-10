#!/bin/bash
set -e

# Variables
APP_REPO="https://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@github.com/mahmoudelshahawy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"

# Update & Install
echo "📦 Installing dependencies..."
sudo apt-get update
sudo apt-get install -y docker.io docker-compose git git-lfs nginx

# Configure Docker
sudo systemctl enable --now docker
sudo usermod -aG docker ubuntu

# Setup Repository
echo "📥 Cloning repository..."
cd /home/ubuntu
sudo git lfs install

if [ -d "$APP_DIR" ]; then
    cd $APP_DIR && sudo -u ubuntu git pull
else
    sudo -u ubuntu git clone $APP_REPO $APP_DIR
    cd $APP_DIR
fi

sudo -u ubuntu git lfs pull
sudo chown -R ubuntu:ubuntu $APP_DIR

# Start Docker Services
echo "🚀 Starting Docker services..."
sudo docker-compose down -v
sudo docker-compose up -d --build

# Configure Nginx
echo "⚙️ Configuring Nginx..."
sudo cat > /etc/nginx/sites-available/insighteye <<'EOF'
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
EOF

sudo rm -f /etc/nginx/sites-enabled/default
sudo ln -sf /etc/nginx/sites-available/insighteye /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl restart nginx

echo "✅ Setup complete!"