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

# Copy the source into a package dir so `python -m data_provider` resolves
# against data_provider/__main__.py (M1's entry point).
COPY . ./data_provider/

ENTRYPOINT ["python", "-m", "data_provider"]
