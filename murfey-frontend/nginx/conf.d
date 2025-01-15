worker_processes  3;
pid /tmp/nginx.pid; # Changed from /var/run/nginx.pid
error_log  /var/log/nginx/error.log;
events {
  worker_connections  10240;
}

http {
  include mime.types;
  client_body_temp_path /tmp/client_temp;
  proxy_temp_path       /tmp/proxy_temp_path;
  fastcgi_temp_path     /tmp/fastcgi_temp;
  uwsgi_temp_path       /tmp/uwsgi_temp;
  scgi_temp_path        /tmp/scgi_temp;
  server {
    listen 8080;
    location / {
      root   /usr/share/nginx/html;
      index  index.html index.htm;
      try_files $uri $uri/ /index.html;
    }
    error_page 500 502 503 504 /50x.html;
    location = /50x.html {
      root  /usr/share/nginx/html;
    }
  }
}

