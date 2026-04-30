# =============================================================================
# AeroCache AI Microservice — Dockerfile
# =============================================================================
#
# Base: python:3.11-slim (Debian Bookworm-based)
#
# Why 3.11-slim?
#   - Python 3.11 is ~25% faster than 3.10 for CPU-bound tasks due to the
#     "Specializing Adaptive Interpreter" (PEP 659). This directly benefits
#     NumPy/scikit-learn inference speed.
#   - The -slim variant excludes unnecessary system packages (compilers,
#     headers, docs), reducing the image from ~900 MB to ~180 MB.
#
# Performance note:
#   scikit-learn and NumPy ship pre-compiled C extensions (wheels) that link
#   against OpenBLAS. pip install on slim images pulls these wheels directly —
#   no compilation needed, keeping build time under 60 seconds.
# =============================================================================

FROM python:3.11-slim AS runtime

# OCI labels for container registries.
LABEL org.opencontainers.image.title="aerocache-ai" \
      org.opencontainers.image.description="AeroCache IsolationForest eviction microservice" \
      org.opencontainers.image.version="1.0.0"

# Set a non-root working directory.
WORKDIR /app

# -----------------------------------------------------------------------------
# System dependencies
# -----------------------------------------------------------------------------
# libgomp1: GNU OpenMP runtime library, required by scikit-learn's joblib
#           backend when n_jobs=-1 (parallel tree fitting in IsolationForest).
#           Without it, sklearn falls back to single-threaded execution.
# curl:     Used by the Docker Compose healthcheck to call GET /health.
#           Not needed at application runtime — purely for the probe command.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgomp1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# Python dependencies
# -----------------------------------------------------------------------------
# Copy requirements.txt first (before source files) so Docker can cache the
# pip install layer.  If only .py files change, this layer is not invalidated
# and rebuilt — significantly speeding up iterative development builds.
COPY requirements.txt .

# Pip install flags:
#   --no-cache-dir    — don't write pip's download cache to the image layer
#                       (saves ~50 MB in the final image)
#   --upgrade pip     — ensure we use the latest pip resolver (avoids
#                       legacy resolver warnings with NumPy/sklearn)
RUN pip install --upgrade pip --no-cache-dir \
    && pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------------
# Application source
# -----------------------------------------------------------------------------
# Copy source files after dependencies so changing a .py file only invalidates
# the last two layers (COPY + CMD) rather than re-running pip install.
COPY schemas.py             .
COPY feature_engineering.py .
COPY model.py               .
COPY main.py                .

# Document the port Uvicorn will bind to.
EXPOSE 8000

# -----------------------------------------------------------------------------
# Runtime environment
# -----------------------------------------------------------------------------
# PYTHONUNBUFFERED=1: Forces stdout/stderr to be unbuffered so Uvicorn logs
#   appear in `docker compose logs` immediately rather than being held in
#   Python's internal write buffer.
# PYTHONDONTWRITEBYTECODE=1: Prevents Python from writing .pyc files into the
#   container filesystem (no benefit in a container, just unnecessary I/O).
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
# Uvicorn flags:
#   main:app          — module name : FastAPI application object
#   --host 0.0.0.0    — bind to all interfaces (required inside a container;
#                       127.0.0.1 would not be reachable from other containers)
#   --port 8000       — must match EXPOSE and docker-compose ports mapping
#   --workers 1       — single worker process. IsolationForest already uses
#                       n_jobs=-1 (all cores via joblib), so multiple Uvicorn
#                       workers would over-subscribe the CPU without benefit.
#   --log-level info  — INFO-level Uvicorn access logs; change to "warning"
#                       in production to reduce log volume.
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info"]
