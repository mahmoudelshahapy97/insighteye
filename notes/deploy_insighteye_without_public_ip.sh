#!/bin/bash
set -e

# -------------------------------
# Variables
# -------------------------------
APP_REPO="https://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@github.com/mahmoudelshahapy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"
DOCKER_COMPOSE_VERSION="v2.27.0"

# -------------------------------
# Update & install dependencies
# -------------------------------
apt update && apt upgrade -y
apt install -y docker.io curl git git-lfs nginx

# -------------------------------
# Enable Docker
# -------------------------------
systemctl enable --now docker
usermod -aG docker ubuntu

# -------------------------------
# Install Docker Compose
# -------------------------------
curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# -------------------------------
# Git LFS setup & clone repo
# -------------------------------
git lfs install
if [ ! -d "$APP_DIR" ]; then
    git clone $APP_REPO $APP_DIR
fi
cd $APP_DIR
git lfs pull

# -------------------------------
# Start Docker services
# -------------------------------
docker-compose down -v || true
docker-compose up -d --build

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 15

# -------------------------------
# Nginx setup - CRITICAL FIX
# -------------------------------
NGINX_CONF="/etc/nginx/sites-available/insighteye"
cat > $NGINX_CONF <<'EOL'
server {
    listen 80;
    server_name _;

    # Root path for ALB health checks
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

    # Optional: Keep your custom path if needed
    location /insighteye/ {
        proxy_pass http://127.0.0.1:8000/;
        
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
EOL

# Enable site and remove default
rm -f /etc/nginx/sites-enabled/default
ln -sf $NGINX_CONF /etc/nginx/sites-enabled/
nginx -t
systemctl enable --now nginx
systemctl restart nginx

# -------------------------------
# Verify everything is working
# -------------------------------
echo "🔍 Checking service status..."

# Check Docker containers
docker ps

# Check if backend is responding
echo "🔍 Testing backend (port 8000)..."
for i in {1..5}; do
    if curl -f -s http://127.0.0.1:8000/health > /dev/null 2>&1; then
        echo "✅ Backend is healthy on port 8000"
        break
    else
        echo "⏳ Attempt $i/5: Backend not ready yet..."
        sleep 5
    fi
done

# Check if Nginx is working
echo "🔍 Testing Nginx (port 80)..."
for i in {1..5}; do
    if curl -f -s http://127.0.0.1:80/ > /dev/null 2>&1; then
        echo "✅ Nginx is healthy on port 80"
        break
    else
        echo "⏳ Attempt $i/5: Nginx not ready yet..."
        sleep 5
    fi
done

# Final health check
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:80/)
if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 307 ]; then
    echo "✅ Instance is healthy! HTTP code: $HTTP_CODE"
else
    echo "⚠️ Warning: Unexpected HTTP code: $HTTP_CODE"
    echo "Checking logs..."
    docker-compose logs --tail=50
    nginx -T
fi

echo ""
echo "🎉 Deployment completed!"
echo "📍 Local health check: http://127.0.0.1:80/"
echo "📍 ALB will check: http://<instance-ip>:80/"