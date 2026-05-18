#!/bin/sh
# Entrypoint for the scraper image.
#
# Sets secure permissions on the credentials directory at every startup so
# that credential files are readable only by root (the container user).
#
# To change the permissions, edit this file and rebuild the image, or
# override the entrypoint in docker-compose.yml:
#
#   scraper:
#     entrypoint: ["/bin/sh", "-c"]
#     command: ["chmod 750 /credentials && chmod 640 /credentials/* 2>/dev/null; exec supervisord -c /app/supervisord.conf"]

chmod 700 /credentials 2>/dev/null || true
chmod 600 /credentials/* 2>/dev/null || true

playwright install chromium

exec supervisord -c /app/supervisord.conf
