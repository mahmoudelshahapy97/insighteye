#!/bin/bash
set -e

# -------------------------------
# Variables
# -------------------------------
APP_REPO="https://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@github.com/mahmoudelshahapy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"
DOCKER_COMPOSE_VERSION="v2.27.0"

# -------------------------------
# Update & Install Dependencies
# -------------------------------
apt update && apt upgrade -y
apt install -y docker.io curl git git-lfs nginx
apt install -y docker-compose-plugin

# -------------------------------
# Enable Docker
# -------------------------------
systemctl enable --now docker
usermod -aG docker ubuntu

# -------------------------------
# Install Docker Compose
# -------------------------------
curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" \
    -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# -------------------------------
# Git Clone + LFS
# -------------------------------
git lfs install
if [ ! -d "$APP_DIR" ]; then
    git clone $APP_REPO $APP_DIR
fi
cd $APP_DIR
git pull || true
git lfs pull

# -------------------------------
# Start Docker Services
# -------------------------------
docker-compose down -v || true
docker-compose up -d --build
sleep 10

# -------------------------------
# Fix nginx PID dir permissions
# -------------------------------
mkdir -p /run/nginx
chown -R www-data:www-data /run/nginx

# -------------------------------
# Nginx Configuration
# -------------------------------
NGINX_CONF="/etc/nginx/sites-available/insighteye"
cat > $NGINX_CONF <<'EOF'
server {
    listen 80;
    server_name _;

    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        access_log off;
    }

    location = / {
        proxy_pass http://127.0.0.1:8000/docs;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
EOF

rm -f /etc/nginx/sites-enabled/default
ln -sf $NGINX_CONF /etc/nginx/sites-enabled/

nginx -t
systemctl daemon-reload
systemctl restart nginx
systemctl status nginx --no-pager

echo "✅ Deployment completed successfully!"
echo "📌 Test locally:   curl http://127.0.0.1/health"
echo "🌍 Access from browser: http://<EC2-PUBLIC-IP>/"
