# Dockerfile
FROM python:3.11-slim

# Systempakete: ffmpeg für Transcoding, ca-certificates, tzdata
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python-Abhängigkeiten
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# App-Code
COPY . /app

# Unprivilegierter Nutzer (optional)
RUN useradd -ms /bin/bash appuser
USER appuser

# Healthcheck: versucht nur, Python zu starten und Config zu prüfen (dry run)
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD python -c "import os; import sys; \
  req=['IMAP_HOST','IMAP_USER','IMAP_PASS','S3_BUCKET','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','OPENAI_API_KEY','WEBHOOK_URL']; \
  sys.exit(0 if all(os.getenv(k) for k in req) else 1)"

# Start
CMD ["python", "main.py"]
