FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ingest_prices.py /app/ingest_prices.py
COPY market_data_service.py /app/market_data_service.py
COPY nas_alert_runner.py /app/nas_alert_runner.py
COPY nas_web_app.py /app/nas_web_app.py
COPY refresh_ticker_master.py /app/refresh_ticker_master.py
COPY generate_universe_symbols.py /app/generate_universe_symbols.py
COPY chat-configs.js /app/chat-configs.js
COPY universe_symbols.json /app/universe_symbols.json

CMD ["python", "/app/ingest_prices.py", "--mode", "daily", "--db-path", "/data/stock_prices.db", "--symbols-file", "/app/chat-configs.js"]
