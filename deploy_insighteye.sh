#!/bin/bash
set -e

# -------------------------------
# Variables (change as needed)
# -------------------------------
APP_REPO="https://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@github.com/mahmoudelshahapy97/insighteye.git"
APP_DIR="/home/ubuntu/insighteye"
PUBLIC_IP=$(curl -s ifconfig.me)
DOCKER_COMPOSE_VERSION="v2.27.0"

# -------------------------------
# Update & install dependencies
# -------------------------------
apt update && apt upgrade -y
apt install -y docker.io curl git git-lfs nginx openssl

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

# -------------------------------
# Nginx setup
# -------------------------------
# Create self-signed SSL if not exists
SSL_KEY="/etc/ssl/private/nginx-selfsigned.key"
SSL_CERT="/etc/ssl/certs/nginx-selfsigned.crt"

if [ ! -f "$SSL_KEY" ] || [ ! -f "$SSL_CERT" ]; then
    openssl req -x509 -nodes -days 365 \
    -newkey rsa:2048 \
    -keyout $SSL_KEY \
    -out $SSL_CERT \
    -subj "/CN=$PUBLIC_IP"
fi

# Nginx config
NGINX_CONF="/etc/nginx/sites-available/insighteye"
cat > $NGINX_CONF <<EOL
server {
    listen 80;
    server_name _;

    return 301 https://\$host\$request_uri;
}

server {
    listen 443 ssl;
    server_name _;

    ssl_certificate $SSL_CERT;
    ssl_certificate_key $SSL_KEY;

    location /insighteye/ {
        proxy_pass http://127.0.0.1:8000/;

        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
EOL

# Enable site
ln -sf $NGINX_CONF /etc/nginx/sites-enabled/
nginx -t
systemctl enable --now nginx
systemctl restart nginx

# -------------------------------
# Test API internally
# -------------------------------
curl -s http://127.0.0.1:8000/health || echo "Health check failed"

echo "✅ Insighteye deployment completed!"
echo "Access your app: https://$PUBLIC_IP/insighteye/docs"
