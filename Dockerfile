FROM python:3.14-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    supervisor \
    wget \
    gnupg \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /logs && chmod 777 /logs

RUN pip install --upgrade "pip>=26.1"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN playwright install chromium
RUN playwright install-deps chromium

COPY scraper/scraper.py scraper.py
COPY api/api.py api.py
COPY alerts/alerts.py alerts.py
COPY webui/app.py webui/app.py
COPY webui/templates webui/templates
COPY webui/static webui/static
COPY supervisord.conf supervisord.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 3000 5001 8000

ENTRYPOINT ["/entrypoint.sh"]
