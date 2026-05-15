FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

ARG APP_PORT

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN pip install --no-cache-dir .

EXPOSE $APP_PORT

CMD gunicorn -w 16 --bind 0.0.0.0:$APP_PORT --timeout 300 run:app
