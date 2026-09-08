# ── Stage 1: install dependencies ──────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime image ──────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /install /usr/local
COPY app/ ./app/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

# Shell form so Docker interpolates BACKEND_HOST / BACKEND_PORT at container start.
CMD uvicorn app.main:app --host "${BACKEND_HOST:-0.0.0.0}" --port "${BACKEND_PORT:-8000}"
