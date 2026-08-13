proxy_http_version 1.1;
proxy_set_header Host              $host;
proxy_set_header X-Real-IP         $remote_addr;
proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
proxy_set_header X-Forwarded-Proto $scheme;
proxy_set_header Connection        "";
# Slightly longer than the app's 8s statement timeout so Postgres cancels a slow
# query and the app returns a clean 503, rather than nginx timing out first and
# reporting a gateway error for what is really a slow query.
proxy_connect_timeout 5s;
proxy_send_timeout    15s;
proxy_read_timeout    15s;
proxy_buffering on;
proxy_buffers 16 16k;
proxy_busy_buffers_size 32k;
