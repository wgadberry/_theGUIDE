# =============================================================================
# T16O / theGuide — Pipeline Worker Base Image
# Shared by: GuideKeeper, Shredder, ChainWalker, Bloodhound, and future workers
#
# Build:
#   docker build -t t16o-pipeline:latest ./pipeline
#
# Run standalone (CLI mode):
#   docker run --rm -e ARANGO_HOST=localhost t16o-pipeline:latest \
#     python GuideKeeper.py --tx sample_tx.json
#
# Run as stream consumer:
#   docker run --rm --env-file .env t16o-pipeline:latest \
#     python GuideKeeper.py --name gk-1
# =============================================================================

# ── BASE ──────────────────────────────────────────────────────
FROM python:3.11-slim

# ── LABELS ────────────────────────────────────────────────────
LABEL maintainer="T16O"
LABEL project="theGuide"
LABEL component="pipeline"

# ── SYSTEM DEPENDENCIES ───────────────────────────────────────
# curl        — healthcheck + optional RPC calls
# gcc         — some pip packages need compilation
# libssl-dev  — cryptography / solders dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# ── WORKING DIRECTORY ─────────────────────────────────────────
WORKDIR /app

# ── PYTHON DEPENDENCIES ───────────────────────────────────────
# Copy requirements first for layer caching —
# only rebuilds this layer when requirements.txt changes
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── APPLICATION CODE ──────────────────────────────────────────
# Copy all pipeline worker scripts
# Each worker is a standalone script sharing this base image
COPY . .

# ── NON-ROOT USER ─────────────────────────────────────────────
# Security best practice — don't run as root
RUN useradd -m -u 1001 t16o \
    && chown -R t16o:t16o /app
USER t16o

# ── DEFAULT ENTRYPOINT ────────────────────────────────────────
# Override in docker-compose command: per worker
# ex: command: ["python", "GuideKeeper.py", "--name", "gk-1"]
ENTRYPOINT ["python"]
CMD ["GuideKeeper.py"]
