#!/bin/bash
# Exec logging for debugging
UBUNTU_USER="ubuntu"
APP_DIR="/home/ubuntu/insighteye"

# ------------------------------
# GitHub Private Repo Access
# ------------------------------
USE_PAT=true
GITHUB_PAT="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"
GITHUB_REPO="https://oauth2:${GITHUB_PAT}@github.com/mahmoudelshahapy97/insighteye.git"

# ------------------------------
# Install dependencies
# ------------------------------
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install -y docker.io curl git git-lfs nginx

sudo systemctl enable --now docker

# Install Docker Compose
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

sudo usermod -aG docker "${UBUNTU_USER}"

# ------------------------------
# Clone App Repository
# ------------------------------
if [ -d "${APP_DIR}" ]; then
  echo "App exists. Pulling latest..."
  cd "${APP_DIR}"
  sudo git fetch --all --prune
  sudo git reset --hard origin/HEAD
else
  echo "Cloning fresh repo..."
  git clone "${GITHUB_REPO}" "${APP_DIR}"
fi

sudo git lfs install --system
cd "${APP_DIR}"
sudo git lfs pull || true

# ------------------------------
# Create systemd service for Docker Compose
# ------------------------------
SERVICE_NAME="insighteye-app.service"

sudo cat > /etc/systemd/system/${SERVICE_NAME} <<EOF
[Unit]
Description=InsightEye Docker App
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=${APP_DIR}
User=${UBUNTU_USER}
Group=${UBUNTU_USER}
Environment=HOME=/home/${UBUNTU_USER}
ExecStart=/usr/local/bin/docker-compose up -d --remove-orphans
ExecStop=/usr/local/bin/docker-compose down
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now ${SERVICE_NAME}

echo "Waiting 10 seconds for the app to start..."
sleep 10

# ------------------------------
# Nginx Reverse Proxy (HTTP Only)
# ------------------------------
NGINX_CONF="/etc/nginx/sites-available/insighteye"

sudo cat > ${NGINX_CONF} <<'EOF'
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

sudo ln -sf ${NGINX_CONF} /etc/nginx/sites-enabled/insighteye
sudo rm -f /etc/nginx/sites-enabled/default || true

sudo nginx -t
sudo systemctl restart nginx

echo "=============== ✅ Deployment Completed Successfully ✅ ==============="
echo "Check logs at: /var/log/user-data.log"
echo "App URL:   http://YOUR_PUBLIC_IP"
