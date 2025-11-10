#!/bin/bash
set -e

# This script runs as root automatically in EC2 User Data

# -------------------------------
# Variables
# -------------------------------
APP_REPO="https://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@github.com/mahmoudelshahawy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"
DOCKER_COMPOSE_VERSION="v2.27.0"
LOG_FILE="/var/log/user-data.log"

# -------------------------------
# Update & Install Dependencies
# -------------------------------
echo "📦 Installing dependencies..."
export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install -y docker.io curl git git-lfs nginx ca-certificates gnupg lsb-release
# sudo apt-get install -y docker-compose-plugin
sudo systemctl enable --now docker

# -------------------------------
# Install Docker Compose (standalone)
# -------------------------------
echo "🔧 Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" \
    -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# -------------------------------
# Enable Docker
# -------------------------------
echo "🐳 Configuring Docker..."
# sudo systemctl start docker
sudo usermod -aG docker ubuntu

# Wait for Docker to be ready
sleep 5

# Verify Docker is running
if ! systemctl is-active --quiet docker; then
    echo "❌ Docker failed to start"
    exit 1
fi

echo "✅ Docker is running"


# Verify installation
docker --version
docker-compose --version || /usr/local/bin/docker-compose --version

echo "✅ Docker Compose installed"

# -------------------------------
# Git Clone + LFS
# -------------------------------
echo "📥 Setting up repository..."
cd /home/ubuntu

git lfs install

if [ -d "$APP_DIR" ]; then
    echo "Repository exists, pulling latest changes..."
    cd $APP_DIR
    sudo -u ubuntu git pull || true
else
    echo "Cloning repository..."
    sudo -u ubuntu git clone $APP_REPO $APP_DIR
    chmod -R 755 $APP_DIR
    cd $APP_DIR
fi

sudo -u ubuntu git lfs pull

# -------------------------------
# Fix permissions
# -------------------------------
chown -R ubuntu:ubuntu $APP_DIR

# -------------------------------
# Fix nginx PID dir permissions
# -------------------------------
echo "🔐 Setting nginx permissions..."
mkdir -p /run/nginx
chown -R www-data:www-data /run/nginx

# -------------------------------
# Start Docker Services
# -------------------------------
echo "🚀 Starting Docker services..."
cd $APP_DIR

docker-compose down -v
docker-compose up -d --build
docker ps

echo "✅ Docker services started"

# -------------------------------
# Nginx Configuration
# -------------------------------
echo "⚙️  Configuring Nginx..."
sudo systemctl enable nginx
NGINX_CONF="/etc/nginx/sites-available/insighteye"

cat > $NGINX_CONF <<'EOF'
server {
    listen 80;
    server_name _;

    # Health check endpoint
    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        access_log off;
    }

    # Proxy all other requests
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        
        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
}
EOF

# Remove default site
rm -f /etc/nginx/sites-enabled/default

# Enable our site
ln -sf $NGINX_CONF /etc/nginx/sites-enabled/

sudo nginx -t
sudo systemctl restart nginx

# Test and restart nginx
if nginx -t; then
    systemctl daemon-reload
    systemctl restart nginx
    systemctl enable nginx
    
    if systemctl is-active --quiet nginx; then
        echo "✅ Nginx is running"
    else
        echo "❌ Nginx failed to start"
        systemctl status nginx --no-pager
        exit 1
    fi
else
    echo "❌ Nginx configuration test failed"
    nginx -t
    exit 1
fi
