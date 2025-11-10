#!/bin/bash
set -e

# Logging
exec > >(tee /var/log/user-data.log)
exec 2>&1

echo "========================================="
echo "Starting InsightEye Deployment"
echo "Time: $(date)"
echo "========================================="

# -------------------------------
# Update & install dependencies
# -------------------------------
echo "Installing dependencies..."
apt update && apt upgrade -y
apt install -y docker.io curl git git-lfs nginx

# -------------------------------
# Enable Docker
# -------------------------------
echo "Configuring Docker..."
systemctl enable --now docker
usermod -aG docker ubuntu

# -------------------------------
# Install Docker Compose
# -------------------------------
echo "Installing Docker Compose..."
DOCKER_COMPOSE_VERSION="v2.27.0"
curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# -------------------------------
# Git LFS setup & clone repo
# -------------------------------
echo "Cloning repository..."
APP_REPO="https://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@github.com/mahmoudelshahapy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"

git lfs install
if [ ! -d "$APP_DIR" ]; then
    git clone $APP_REPO $APP_DIR
fi
cd $APP_DIR
git lfs pull

# -------------------------------
# Start Docker services
# -------------------------------
echo "Starting Docker containers..."
cd $APP_DIR
docker-compose down -v || true

# Build can take time, so let's monitor it
echo "Building and starting containers (this may take 5-10 minutes)..."
docker-compose up -d --build

# Wait for containers to be running and backend to respond
echo "Waiting for backend to be ready..."
MAX_ATTEMPTS=60  # 5 minutes (60 * 5 seconds)
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker ps | grep -q "insighteye"; then
        echo "Containers are running, checking backend..."
        if curl -f -s http://127.0.0.1:8000/health > /dev/null 2>&1; then
            echo "✅ Backend is ready!"
            break
        fi
    fi
    ATTEMPT=$((ATTEMPT + 1))
    echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Still waiting..."
    sleep 5
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "⚠️ Warning: Backend took too long to start"
    docker-compose logs --tail=100
fi

# -------------------------------
# Nginx Configuration (writing to temp file first)
# -------------------------------
echo "Configuring Nginx..."
cat > /tmp/insighteye-nginx.conf << 'NGINX_EOF'
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name _;

    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        access_log off;
    }

    location = / {
        return 301 /docs;
    }

    location /docs {
        proxy_pass http://127.0.0.1:8000/docs;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
}
NGINX_EOF

# Move to proper location
mv /tmp/insighteye-nginx.conf /etc/nginx/sites-available/insighteye

# Remove default and enable our config
rm -f /etc/nginx/sites-enabled/default
ln -sf /etc/nginx/sites-available/insighteye /etc/nginx/sites-enabled/

# Test and reload
nginx -t
systemctl enable nginx
systemctl restart nginx

echo "Nginx configuration applied"

# -------------------------------
# Verify Deployment
# -------------------------------
echo "Verifying deployment..."
sleep 5

# Check Docker containers
echo "Docker containers:"
docker ps

# Check backend health
echo "Testing backend on port 8000..."
for i in {1..10}; do
    if curl -f -s http://127.0.0.1:8000/health > /dev/null 2>&1; then
        echo "✅ Backend is responding"
        curl -s http://127.0.0.1:8000/health
        break
    else
        echo "⏳ Attempt $i/10: Waiting for backend..."
        sleep 3
    fi
done

# Check Nginx
echo "Testing Nginx on port 80..."
for i in {1..5}; do
    if curl -f -s http://127.0.0.1:80/health > /dev/null 2>&1; then
        echo "✅ Nginx is responding"
        curl -s http://127.0.0.1:80/health
        break
    else
        echo "⏳ Attempt $i/5: Waiting for Nginx..."
        sleep 3
    fi
done

# Final status
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:80/health || echo "000")
echo "Final HTTP status code: $HTTP_CODE"

if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ Deployment successful!"
else
    echo "⚠️ Warning: Status code $HTTP_CODE"
    echo "Nginx config check:"
    nginx -T
    echo "Docker logs:"
    docker-compose logs --tail=50
fi

echo "========================================="
echo "Deployment Complete: $(date)"
echo "========================================="