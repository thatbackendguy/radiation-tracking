# ── Stage 1: build dependency wheels ────────────────────────────────────────
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

# ── Stage 2: runtime image ──────────────────────────────────────────────────
FROM python:3.11-slim
WORKDIR /opt
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache /wheels/*

# Copy the source into a package dir so `python -m aggregated_sink` resolves
# against aggregated_sink/__main__.py.
COPY . ./aggregated_sink/

ENTRYPOINT ["python", "-m", "aggregated_sink"]
