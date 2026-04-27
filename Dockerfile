FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Cache bust: v11 — reduce maxItems to 25 to stay under 60s MCP timeout
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD sh -c "gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 2"
