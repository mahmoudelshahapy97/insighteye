#!/bin/bash
# Exec logging for debugging (writes to /var/log/user-data.log)
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
set -euo pipefail

# --------- Configuration - EDIT THESE ----------
DOMAIN="your.domain.tld"           # <- Replace with your domain (required for Let's Encrypt)
LETSENCRYPT_EMAIL="admin@domain.tld"  # <- Replace w/ email for cert registration
GITHUB_REPO_SSH="git@github.com:mahmoudelshahapy97/insighteye.git"
GITHUB_REPO_HTTPS="https://github.com/mahmoudelshahapy97/insighteye.git"
# Option A (recommended): SSH deploy key (private key text)
GITHUB_SSH_KEY="-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACAmOJ1X1udATJsH9/rJsce9R6ik17v+xsrBHPYMsjfMMQAAAJj++ocW/vqH
FgAAAAtzc2gtZWQyNTUxOQAAACAmOJ1X1udATJsH9/rJsce9R6ik17v+xsrBHPYMsjfMMQ
AAAEA0Vouoz6A/nX1Z/yWHvrIi06yHYpatg778SUvlDclYOCY4nVfW50BMmwf3+smxx71H
qKTXu/7GysEc9gyyN8wxAAAAFWluc2lnaHRleWUgZGVwbG95IGtleQ==
-----END OPENSSH PRIVATE KEY-----"  

# Put private key here OR leave empty and use PAT below
# Option B (fallback): PAT (less secure), set to non-empty to use
USE_PAT=false
GITHUB_PAT="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"      # If USE_PAT=true, set this to your github_pat with repo scope (not recommended inline)

APP_DIR="/home/ubuntu/insighteye"
UBUNTU_USER="ubuntu"
# -----------------------------------------------

# Update & install core packages
apt-get update -y
apt-get upgrade -y
apt-get install -y docker.io curl git git-lfs nginx certbot python3-certbot-nginx ufw

# Enable docker
systemctl enable --now docker

# Install Docker Compose (binary)
COMPOSE_BIN="/usr/local/bin/docker-compose"
curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o "${COMPOSE_BIN}"
chmod +x "${COMPOSE_BIN}"

# Add ubuntu user to docker group
usermod -aG docker "${UBUNTU_USER}"

# Setup SSH deploy key if provided (recommended)
if [ -n "${GITHUB_SSH_KEY}" ]; then
  echo "Installing SSH deploy key..."
  sudo -u "${UBUNTU_USER}" mkdir -p /home/${UBUNTU_USER}/.ssh
  cat > /home/${UBUNTU_USER}/.ssh/id_rsa <<'KEY'
${GITHUB_SSH_KEY}
KEY
  chmod 600 /home/${UBUNTU_USER}/.ssh/id_rsa
  chown ${UBUNTU_USER}:${UBUNTU_USER} /home/${UBUNTU_USER}/.ssh/id_rsa
  # Add github to known_hosts to avoid prompt
  sudo -u "${UBUNTU_USER}" ssh-keyscan -t rsa github.com >> /home/${UBUNTU_USER}/.ssh/known_hosts || true
  chmod 600 /home/${UBUNTU_USER}/.ssh/known_hosts
fi

# Clone repository (SSH preferred). Fallback to PAT if requested.
if [ -d "${APP_DIR}" ]; then
  echo "App directory already exists, pulling latest..."
  cd "${APP_DIR}"
  sudo -u "${UBUNTU_USER}" git fetch --all --prune
  sudo -u "${UBUNTU_USER}" git reset --hard origin/HEAD
else
  if [ -n "${GITHUB_SSH_KEY}" ]; then
    echo "Cloning repo via SSH..."
    sudo -u "${UBUNTU_USER}" git clone "${GITHUB_REPO_SSH}" "${APP_DIR}"
  elif [ "${USE_PAT}" = "true" ] && [ -n "${GITHUB_PAT}" ]; then
    echo "Cloning repo via PAT (fallback)..."
    # Use oauth2:token format to avoid username issues
    sudo -u "${UBUNTU_USER}" git clone "https://oauth2:${GITHUB_PAT}@github.com/mahmoudelshahapy97/insighteye.git" "${APP_DIR}"
  else
    echo "ERROR: No deployment credential provided. Set GITHUB_SSH_KEY or enable USE_PAT and set GITHUB_PAT."
    exit 1
  fi
fi

# Install Git LFS and pull large files
git lfs install --system
cd "${APP_DIR}"
sudo -u "${UBUNTU_USER}" git lfs pull || true

# Create a systemd service to run docker-compose (ensures restart on failure/reboot)
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

# Reload systemd and start application
systemctl daemon-reload
systemctl enable --now ${SERVICE_NAME}

# Wait a little for app to start and health-check endpoint if exists
echo "Waiting 10s for services to start..."
sleep 10

# Install and configure nginx reverse proxy (default listens on 80)
# If your app exposes port 8000 on host (docker-compose should map it) this will proxy to localhost:8000
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

# Open firewall (optional — AWS SG is authoritative, but this helps for local)
ufw allow OpenSSH
ufw allow http
ufw allow https
ufw --force enable || true

# Obtain Let's Encrypt certificate via certbot if DOMAIN is set
if [ -n "${DOMAIN}" ] && [ "${DOMAIN}" != "your.domain.tld" ]; then
  echo "Obtaining Let's Encrypt certificate for ${DOMAIN}..."
  # Ensure DNS A record points to this instance's IP before running this step
  certbot --nginx -d "${DOMAIN}" --non-interactive --agree-tos -m "${LETSENCRYPT_EMAIL}" --redirect || {
    echo "certbot failed — check DNS and that port 80/443 are reachable. You can retry certbot manually."
  }
else
  echo "Skipping certbot: DOMAIN not set or placeholder."
fi

# Basic health check loop (runs in background) - optional
cat > /usr/local/bin/insighteye-healthcheck.sh <<'HC'
#!/bin/bash
set -e
# Simple HTTP check - customize path if your app exposes /health
URL="http://127.0.0.1:8000/health"
LOG="/var/log/insighteye-health.log"
while true; do
  if curl -fsS --max-time 5 "$URL" > /dev/null; then
    echo "$(date -Iseconds) OK" >> "$LOG"
  else
    echo "$(date -Iseconds) FAIL - restarting docker-compose" >> "$LOG"
    cd "${APP_DIR}" && /usr/local/bin/docker-compose restart || true
  fi
  sleep 30
done
HC

chmod +x /usr/local/bin/insighteye-healthcheck.sh
# Start it with systemd so it persists
cat > /etc/systemd/system/insighteye-health.service <<'HS'
[Unit]
Description=Insighteye Healthcheck
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/insighteye-healthcheck.sh
Restart=always
User=root

[Install]
WantedBy=multi-user.target
HS

systemctl daemon-reload
systemctl enable --now insighteye-health.service

echo "Deployment finished. Check /var/log/user-data.log and /var/log/insighteye-health.log for status."
