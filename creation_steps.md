Perfect — I will give you a **clean, official deployment guide** for your repo ✅
(Using **Ubuntu 22.04 EC2**, **Docker + Nginx reverse proxy + HTTPS**)

---

## ✅ **Production Deployment Guide for Insighteye App**

🔹 Requirements: Fresh EC2 Ubuntu instance + Public IP
🔹 Stack: Docker + Docker Compose + Nginx SSL reverse proxy
🔹 App repo: [https://github.com/mahmoudelshahapy97/insighteye.git](https://github.com/mahmoudelshahapy97/insighteye.git)
🔹 App repo: [https://github.com/mahmoudelshahapy97/insighteye.git](https://github.com/mahmoudelshahapy97/insighteye.git)

---

# ✅ STEP-1 — Connect to EC2

```bash
ssh -i your-key.pem ubuntu@YOUR_EC2_PUBLIC_IP
```

---

# ✅ STEP-2 — Install Docker & Docker Compose

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y docker.io curl git
sudo systemctl enable --now docker

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
 -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

sudo usermod -aG docker ubuntu
newgrp docker
```

Test:

```bash
docker --version
docker-compose --version
```

---

# ✅ STEP-3 — Clone Repo + Pull LFS Models

```bash
cd /home/ubuntu
sudo apt-get install git-lfs -y
git lfs install

git clone https://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@github.com/mahmoudelshahapy97/insighteye.git
cd insighteye
git lfs pull
```

---

---

```

Save → CTRL+O → ENTER → CTRL+X

---

# ✅ STEP-6 — Start Docker Services

```bash
docker-compose down -v
docker-compose up -d --build
docker ps
```

Check logs:

```bash
docker logs insighteye_app -f
```

If everything is ✅
You should see FastAPI running on port `8000`

Test internally:

```bash
curl http://localhost:8000/health
```

---

# ✅ STEP-7 — Install Nginx Reverse Proxy

```bash
sudo apt install nginx -y
sudo systemctl enable nginx
```

---

# ✅ STEP-8 — Create Nginx Config for HTTPS (Using IP only ✅)

```bash
sudo nano /etc/nginx/sites-available/insighteye
```

Paste:

```nginx
server {
    listen 80;
    server_name _;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name _;

    ssl_certificate /etc/ssl/certs/nginx-selfsigned.crt;
    ssl_certificate_key /etc/ssl/private/nginx-selfsigned.key;

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
```

Enable config:

```bash
sudo ln -sf /etc/nginx/sites-available/insighteye /etc/nginx/sites-enabled/
```

YOUR_PUBLIC_IP==dig +short myip.opendns.com @resolver1.opendns.com
or
YOUR_PUBLIC_IP==curl ifconfig.me
or
YOUR_PUBLIC_IP==curl checkip.amazonaws.com

---

# ✅ STEP-9 — Create SSL Certificate (Self-signed for IP)

```bash
sudo openssl req -x509 -nodes -days 365 \
-newkey rsa:2048 \
-keyout /etc/ssl/private/nginx-selfsigned.key \
-out /etc/ssl/certs/nginx-selfsigned.crt \
-subj "/CN=YOUR_PUBLIC_IP"
```

---

# ✅ STEP-10 — Restart Nginx

```bash
sudo nginx -t
sudo systemctl restart nginx
```

---

# ✅ STEP-11 — Test in Browser ✅

Use:

```
https://YOUR_PUBLIC_IP/insighteye/docs
```

If browser shows "unsafe" connection → click **Advanced → Proceed**
(Self-signed ✅)

✅ Your API, UI and ML models are now fully deployed 🎯

---

# OPTIONAL ✅ Use Real SSL for Domain

If you later attach a domain & DNS:

```bash
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx -d insighteye.io
```

---

## ✅ Done ✅

If you want — I can also:
✅ Add HTTPS for domain
✅ Setup automatic restart & monitoring
✅ Configure production logging
✅ Setup CloudFront + WAF
✅ Add WebSockets sticky-session support for chat
✅ Full CI/CD pipeline

---

Would you like me to create a **script** that performs all steps automatically in 1-click? 🚀
