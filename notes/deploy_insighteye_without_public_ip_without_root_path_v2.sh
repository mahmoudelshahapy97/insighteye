#!/bin/bash
set -e

# -------------------------------
# Variables
# -------------------------------
APP_REPO="https://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@github.com/mahmoudelshahapy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"
DOCKER_COMPOSE_VERSION="v2.27.0"

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
curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# -------------------------------
# Git LFS setup & clone repo
# -------------------------------
echo "Cloning repository..."
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
docker-compose down -v || true
docker-compose up -d --build

# Wait for services
echo "Waiting for services to start..."
sleep 20

# -------------------------------
# Nginx Configuration
# -------------------------------
echo "Configuring Nginx..."
cat > /etc/nginx/sites-available/insighteye <<'EOL'
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name _;

    # Remove any default sites
    root /var/www/html;

    # Health check endpoint for ALB
    location /health {
        proxy_pass http://127.0.0.1:8000/health;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Disable logging for health checks
        access_log off;
    }

    # Root redirects to docs
    location = / {
        return 301 /docs;
    }

    # API Documentation
    location /docs {
        proxy_pass http://127.0.0.1:8000/docs;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Main application - proxy everything else
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        
        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
}
EOL

# Remove default site and enable our config
rm -f /etc/nginx/sites-enabled/default
ln -sf /etc/nginx/sites-available/insighteye /etc/nginx/sites-enabled/

# Test and reload
nginx -t
systemctl enable nginx
systemctl restart nginx

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
        echo "✅ Backend is responding on port 8000"
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
        echo "✅ Nginx is responding on port 80"
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
    echo "⚠️ Warning: Unexpected status code"
    echo "Checking logs..."
    docker-compose logs --tail=50
fi

echo "========================================="
echo "Deployment Complete: $(date)"
echo "========================================="