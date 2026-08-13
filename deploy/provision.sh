#!/usr/bin/env bash
# Server setup for the Weather Data API. Run AS ROOT on the EC2 box:
#
#   sudo bash /home/ec2-user/restapi_flask/deploy/provision.sh
#
# Idempotent: safe to re-run. It does NOT obtain the TLS certificate — DNS has to
# resolve first. Until it does, an HTTP-only vhost is installed so certbot's ACME
# challenge can complete; re-run this afterwards to install the hardened TLS
# vhost. It also does not create the database role (see deploy/sql/roles.sql).
set -euo pipefail

APP=/home/ec2-user/restapi_flask
DOMAIN=api.dustincremascoli.com
ENVDIR=/etc/restapi
PWFILE=/root/.restapi/api_ro.pw

say() { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }

[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }
[[ -d $APP ]] || { echo "missing $APP — deploy the code first" >&2; exit 1; }

say "Python virtualenv"
PYBIN="${PYBIN:-/usr/bin/python3.11}"
[[ -x $PYBIN ]] || PYBIN=$(command -v python3)
if [[ ! -x $APP/.venv/bin/python ]]; then
  "$PYBIN" -m venv "$APP/.venv"
fi
echo "  venv: $("$APP/.venv/bin/python" -V 2>&1)"
"$APP/.venv/bin/pip" install --quiet --upgrade pip
"$APP/.venv/bin/pip" install --quiet -r "$APP/requirements.txt"
chown -R ec2-user:ec2-user "$APP"

say "Secrets"
install -d -m 0750 -o root -g ec2-user "$ENVDIR"
if [[ ! -f $ENVDIR/restapi.env ]]; then
  [[ -s $PWFILE ]] || { echo "missing $PWFILE — run deploy/sql/roles.sql first" >&2; exit 1; }
  cat > "$ENVDIR/restapi.env" <<EOF
PG_HOST=127.0.0.1
PG_PORT=5432
PG_USER=api_ro
PG_PASSWORD=$(cat "$PWFILE")
CACHE_HISTORICAL=3600
CACHE_FORECAST=300
DEFAULT_DATE_WINDOW_DAYS=60
STATEMENT_TIMEOUT_MS=8000
# Per worker, per database. 2 workers x 2 databases x 2 = 8 connections, under
# api_ro's CONNECTION LIMIT of 20.
POOL_MAX_SIZE=2
GUNICORN_WORKERS=2
GUNICORN_THREADS=4
LOG_LEVEL=INFO
EOF
  echo "  wrote $ENVDIR/restapi.env"
else
  echo "  $ENVDIR/restapi.env exists, left alone"
fi
chown root:ec2-user "$ENVDIR/restapi.env"
chmod 0640 "$ENVDIR/restapi.env"

say "systemd unit"
install -m 0644 "$APP/deploy/restapi.service" /etc/systemd/system/restapi.service
systemctl daemon-reload
systemctl enable restapi >/dev/null

say "nginx"
install -m 0644 "$APP/deploy/proxy_params_restapi.inc" /etc/nginx/proxy_params_restapi.inc
install -d -m 0755 /var/www/letsencrypt
# The proxy cache directory must exist and be writable by nginx, or every
# request fails with "no such file or directory" on the cache path.
install -d -m 0700 -o nginx -g nginx /var/cache/nginx/api

if [[ ! -f /etc/letsencrypt/live/$DOMAIN/fullchain.pem ]]; then
  say "No certificate for $DOMAIN yet — installing HTTP-only vhost"
  cat > /etc/nginx/conf.d/api.conf <<EOF
limit_req_zone \$binary_remote_addr zone=api_rate:10m rate=120r/m;
limit_conn_zone \$binary_remote_addr zone=api_conn:10m;
upstream restapi_app { server unix:/run/restapi/restapi.sock fail_timeout=0; }

server {
    listen 80;
    listen [::]:80;
    server_name $DOMAIN;

    location /.well-known/acme-challenge/ { root /var/www/letsencrypt; }

    location / {
        limit_req zone=api_rate burst=40 nodelay;
        limit_conn api_conn 20;
        limit_req_status 429;
        proxy_pass http://restapi_app;
        include /etc/nginx/proxy_params_restapi.inc;
    }
}
EOF
else
  say "Certificate present — installing the hardened TLS vhost"
  install -m 0644 "$APP/deploy/nginx-api.conf" /etc/nginx/conf.d/api.conf
fi

nginx -t
systemctl reload nginx

say "Starting the API"
systemctl restart restapi
sleep 2
systemctl is-active restapi || {
  echo "service failed to start — journalctl -u restapi -n 40" >&2
  exit 1
}
curl -s --unix-socket /run/restapi/restapi.sock http://localhost/health || echo "(socket not reachable)"
echo

say "Done."
if [[ ! -f /etc/letsencrypt/live/$DOMAIN/fullchain.pem ]]; then
  cat <<EOF

Next steps:
  1. Add a DNS A record:  $DOMAIN  ->  $(curl -s --max-time 5 ifconfig.me || echo '<this host IP>')
  2. Once it resolves:    sudo certbot --nginx -d $DOMAIN
  3. Re-run this script to install the hardened TLS vhost.

EOF
fi
