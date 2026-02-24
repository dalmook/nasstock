FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ingest_prices.py /app/ingest_prices.py
COPY nas_alert_runner.py /app/nas_alert_runner.py
COPY nas_web_app.py /app/nas_web_app.py
COPY chat-configs.js /app/chat-configs.js

CMD ["python", "/app/ingest_prices.py", "--mode", "daily", "--db-path", "/data/stock_prices.db", "--symbols-file", "/app/chat-configs.js"]
