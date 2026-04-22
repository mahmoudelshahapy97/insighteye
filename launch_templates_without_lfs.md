#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log) 2>&1

echo "=== Starting deployment at $(date) ==="

UBUNTU_USER="ubuntu"
APP_DIR="/home/ubuntu/insighteye"

# GitHub Private Repo Access
USE_PAT=true
GITHUB_PAT="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"
GITHUB_REPO="https://oauth2:${GITHUB_PAT}@github.com/mahmoudelshahapy97/insighteye.git"

# Install dependencies
echo "Installing dependencies..."
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get upgrade -y
apt-get install -y docker.io docker-buildx curl git nginx

systemctl enable docker
systemctl start docker

# Install Docker Compose
echo "Installing Docker Compose..."
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Also create cli-plugins version for compatibility
mkdir -p /usr/local/lib/docker/cli-plugins
cp /usr/local/bin/docker-compose /usr/local/lib/docker/cli-plugins/docker-compose

# Configure Docker permissions
echo "Configuring Docker permissions..."
chmod 666 /var/run/docker.sock
usermod -aG docker "${UBUNTU_USER}"

# Clone App Repository
echo "Cloning repository..."
if [ -d "${APP_DIR}" ]; then
  echo "App directory exists. Cleaning up..."
  chown -R ${UBUNTU_USER}:${UBUNTU_USER} "${APP_DIR}"
  
  su - ${UBUNTU_USER} -c "git config --global --add safe.directory ${APP_DIR}"
  
  cd "${APP_DIR}"
  echo "Pulling latest changes..."
  su - ${UBUNTU_USER} -c "cd ${APP_DIR} && git fetch --all --prune"
  su - ${UBUNTU_USER} -c "cd ${APP_DIR} && git reset --hard origin/main"
  su - ${UBUNTU_USER} -c "cd ${APP_DIR} && git pull"
else
  echo "Cloning fresh repo..."
  su - ${UBUNTU_USER} -c "git clone ${GITHUB_REPO} ${APP_DIR}"
  su - ${UBUNTU_USER} -c "git config --global --add safe.directory ${APP_DIR}"
fi

cd "${APP_DIR}"

# Set proper ownership
echo "Setting proper ownership..."
chown -R ${UBUNTU_USER}:${UBUNTU_USER} "${APP_DIR}"

# Ensure models folder exists
mkdir -p "${APP_DIR}/models"
echo "Make sure to manually upload your model files into ${APP_DIR}/models/"

# Build Docker images
echo "Building Docker images..."
cd "${APP_DIR}"
docker compose build --no-cache

# Start containers
echo "Starting containers..."
docker compose up -d --remove-orphans

# Wait for containers
echo "Waiting for containers to start..."
sleep 30

# Verify containers are running
echo "Checking container status..."
docker compose ps

# Create systemd service for Docker Compose
echo "Creating systemd service..."
SERVICE_NAME="insighteye-app.service"

cat > /etc/systemd/system/${SERVICE_NAME} <<'SERVICEEOF'
[Unit]
Description=InsightEye Docker App
After=docker.service network-online.target
Wants=network-online.target
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ubuntu/insighteye
User=root
Group=root
Environment=HOME=/root
ExecStartPre=/bin/sleep 10
ExecStart=/usr/local/bin/docker-compose up -d --remove-orphans
ExecStop=/usr/local/bin/docker-compose down
ExecReload=/usr/local/bin/docker-compose restart
Restart=on-failure
RestartSec=15
TimeoutStartSec=600

[Install]
WantedBy=multi-user.target
SERVICEEOF

systemctl daemon-reload
systemctl enable ${SERVICE_NAME}

# Configure Nginx Reverse Proxy
echo "Configuring Nginx..."
NGINX_CONF="/etc/nginx/sites-available/insighteye"

cat > ${NGINX_CONF} <<'NGINXEOF'
server {
    listen 80;
    server_name _;
    client_max_body_size 100M;

    location / {
        proxy_pass http://127.0.0.1:8001;

        # Required for WebSockets
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Standard proxy headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket-friendly timeouts
        proxy_connect_timeout 300s;
        proxy_send_timeout 300s;
        proxy_read_timeout 3600s;

        # Disable buffering for WS
        proxy_buffering off;
    }
}
NGINXEOF

ln -sf ${NGINX_CONF} /etc/nginx/sites-enabled/insighteye
rm -f /etc/nginx/sites-enabled/default || true

nginx -t
systemctl restart nginx
systemctl enable nginx

# Final status
echo ""
echo "=== Deployment Summary ==="
echo "Time: $(date)"
echo ""
echo "Docker containers:"
docker ps
echo ""
echo "Nginx status:"
systemctl status nginx --no-pager -l | head -20
echo ""
echo "=============== Deployment Status ==============="
echo ""
echo "Make sure your model files are in ${APP_DIR}/models/"
echo "Then restart containers with: docker compose restart"
echo "Check logs at: /var/log/user-data.log"
echo "App should be available at: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)"
