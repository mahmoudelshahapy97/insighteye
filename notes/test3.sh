#!/bin/bash
# Exec logging for debugging
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
set -euo pipefail

# ------------------------------- 
# Configuration
# -------------------------------
GITHUB_REPO_SSH="git@github.com:mahmoudelshahapy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"
UBUNTU_USER="ubuntu"
GITHUB_SSH_KEY="-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACAmOJ1X1udATJsH9/rJsce9R6ik17v+xsrBHPYMsjfMMQAAAJj++ocW/vqH
FgAAAAtzc2gtZWQyNTUxOQAAACAmOJ1X1udATJsH9/rJsce9R6ik17v+xsrBHPYMsjfMMQ
AAAEA0Vouoz6A/nX1Z/yWHvrIi06yHYpatg778SUvlDclYOCY4nVfW50BMmwf3+smxx71H
qKTXu/7GysEc9gyyN8wxAAAAFWluc2lnaHRleWUgZGVwbG95IGtleQ==
-----END OPENSSH PRIVATE KEY-----"

# ------------------------------- 
# Install packages
# -------------------------------
apt-get update -y
apt-get upgrade -y
apt-get install -y docker.io curl git git-lfs nginx ufw

# Enable Docker
systemctl enable --now docker

# Install Docker Compose
COMPOSE_BIN="/usr/local/bin/docker-compose"
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o "${COMPOSE_BIN}"
chmod +x "${COMPOSE_BIN}"

# Add ubuntu user to docker group
usermod -aG docker "${UBUNTU_USER}"

# Setup SSH deploy key
if [ -n "${GITHUB_SSH_KEY}" ]; then
  echo "Installing SSH deploy key..."
  sudo -u "${UBUNTU_USER}" mkdir -p /home/${UBUNTU_USER}/.ssh
  cat > /home/${UBUNTU_USER}/.ssh/id_rsa <<'KEY'
${GITHUB_SSH_KEY}
KEY
  chmod 600 /home/${UBUNTU_USER}/.ssh/id_rsa
  chown ${UBUNTU_USER}:${UBUNTU_USER} /home/${UBUNTU_USER}/.ssh/id_rsa
  sudo -u "${UBUNTU_USER}" ssh-keyscan -t rsa github.com >> /home/${UBUNTU_USER}/.ssh/known_hosts || true
  chmod 600 /home/${UBUNTU_USER}/.ssh/known_hosts
fi

# Clone repository
if [ -d "${APP_DIR}" ]; then
  echo "App directory exists, pulling latest..."
  cd "${APP_DIR}"
  sudo -u "${UBUNTU_USER}" git fetch --all --prune
  sudo -u "${UBUNTU_USER}" git reset --hard origin/HEAD
else
  echo "Cloning repo via SSH..."
  sudo -u "${UBUNTU_USER}" git clone "${GITHUB_REPO_SSH}" "${APP_DIR}"
fi

# Git LFS
git lfs install --system
cd "${APP_DIR}"
sudo -u "${UBUNTU_USER}" git lfs pull || true

# Systemd service for Docker Compose
SERVICE_NAME="insighteye-app.service"
cat > /etc/systemd/system/${SERVICE_NAME} <<EOF
[Unit]
Description=Insighteye docker-compose app
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
TimeoutStartSec=600
TimeoutStopSec=600
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now ${SERVICE_NAME}

# Nginx reverse proxy (optional, port 80 → 8000)
NGINX_CONF="/etc/nginx/sites-available/insighteye"
cat > ${NGINX_CONF} <<'NGINX'
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
}
NGINX

ln -sf ${NGINX_CONF} /etc/nginx/sites-enabled/insighteye
rm -f /etc/nginx/sites-enabled/default || true
nginx -t
systemctl restart nginx

# Firewall
ufw allow OpenSSH
ufw allow 80
ufw --force enable || true

echo "Deployment finished. Check /var/log/user-data.log for status."
