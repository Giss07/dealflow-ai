#!/bin/sh
if [ "$SERVICE_TYPE" = "worker" ]; then
    echo "Starting worker..."
    exec python worker.py
else
    echo "Starting web server on port ${PORT:-8080}..."
    exec gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers 2
fi
