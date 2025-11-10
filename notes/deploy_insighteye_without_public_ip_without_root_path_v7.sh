#!/bin/bash
set -euo pipefail

# ---------------- CONFIG ----------------
APP_REPO="git@github.com:mahmoudelshahawy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"
SSH_KEY_CONTENT="-----BEGIN OPENSSH PRIVATE KEY-----
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
-----END OPENSSH PRIVATE KEY-----"
# ---------------------------------------

export DEBIAN_FRONTEND=noninteractive

echo "⏳ Installing dependencies..."
apt-get update -y
apt-get install -y --no-install-recommends git git-lfs docker.io nginx ssh

systemctl enable --now docker

# إعداد SSH Key لمستخدم ubuntu
mkdir -p /home/ubuntu/.ssh
echo "$SSH_KEY_CONTENT" > /home/ubuntu/.ssh/id_rsa
chmod 600 /home/ubuntu/.ssh/id_rsa
chown -R ubuntu:ubuntu /home/ubuntu/.ssh

# إضافة GitHub إلى known_hosts لتجنب سؤال التحقق
sudo -u ubuntu ssh-keyscan github.com >> /home/ubuntu/.ssh/known_hosts

# git-lfs init
sudo -u ubuntu git lfs install --skip-repo || true

# Clone or pull repo
if [ -d "$APP_DIR/.git" ]; then
    cd "$APP_DIR"
    sudo -u ubuntu git pull --rebase || true
else
    sudo -u ubuntu git clone "$APP_REPO" "$APP_DIR"
fi

# Pull git-lfs files
cd "$APP_DIR"
sudo -u ubuntu git lfs pull || true
chown -R ubuntu:ubuntu "$APP_DIR"

# Docker compose start
if docker compose version >/dev/null 2>&1; then
    cd "$APP_DIR"
    docker compose down -v || true
    docker compose up -d --build
else
    if command -v docker-compose >/dev/null 2>&1; then
        cd "$APP_DIR"
        docker-compose down -v || true
        docker-compose up -d --build
    else
        echo "No docker compose available" >&2
    fi
fi

# Nginx config
cat >/etc/nginx/sites-available/insighteye <<'EOF'
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

rm -f /etc/nginx/sites-enabled/default
ln -sf /etc/nginx/sites-available/insighteye /etc/nginx/sites-enabled/insighteye
nginx -t && systemctl restart nginx || true

echo "✅ Deployment with SSH key finished at $(date)"
