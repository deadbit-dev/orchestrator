FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
RUN pip install --no-cache-dir "websockets>=14.0,<16.0"
COPY service.py /app/service.py
COPY orchestrator /app/orchestrator

EXPOSE 9000 9001
CMD ["python3", "/app/service.py"]
