#!/bin/bash

# Create directory for Nginx to access certificates
sudo mkdir -p /etc/nginx/ssl/insighteye.io

# Copy certificates with proper permissions
sudo cp /etc/letsencrypt/live/insighteye.io/fullchain.pem /etc/nginx/ssl/insighteye.io/
sudo cp /etc/letsencrypt/live/insighteye.io/privkey.pem /etc/nginx/ssl/insighteye.io/

# Set appropriate permissions
sudo chown -R www-data:www-data /etc/nginx/ssl/insighteye.io/
sudo chmod -R 750 /etc/nginx/ssl/insighteye.io/

# Modify your Nginx configuration to use these files instead
# Update your nginx configuration to point to:
# ssl_certificate /etc/nginx/ssl/insighteye.io/fullchain.pem;
# ssl_certificate_key /etc/nginx/ssl/insighteye.io/privkey.pem;

echo "Certificate files copied to /etc/nginx/ssl/insighteye.io/"
echo "Now update your Nginx configuration to point to these new locations."

# To automatically update these certificates when Let's Encrypt renews them,
# create a hook for certbot:
sudo mkdir -p /etc/letsencrypt/renewal-hooks/post/
sudo tee /etc/letsencrypt/renewal-hooks/post/copy-certs-to-nginx.sh > /dev/null << 'EOT'
#!/bin/bash
cp /etc/letsencrypt/live/insighteye.io/fullchain.pem /etc/nginx/ssl/insighteye.io/
cp /etc/letsencrypt/live/insighteye.io/privkey.pem /etc/nginx/ssl/insighteye.io/
chown www-data:www-data /etc/nginx/ssl/insighteye.io/*.pem
chmod 640 /etc/nginx/ssl/insighteye.io/*.pem
systemctl reload nginx
EOT

sudo chmod +x /etc/letsencrypt/renewal-hooks/post/copy-certs-to-nginx.sh
echo "Created certbot renewal hook to automatically update Nginx certificates."
