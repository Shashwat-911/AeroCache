"""
main.py — FastAPI entry point for the AeroCache AI eviction microservice.

Endpoints
---------
GET  /health              — Liveness probe used by Docker Compose health checks.
POST /predict-evictions   — Core eviction prediction endpoint.

Performance budget
------------------
Java's AIEvictionPolicy enforces a 200 ms hard timeout.  Our internal target
is ≤ 100 ms for the full request lifecycle so we have a 2× safety margin:

    Network I/O      ~5–10 ms  (LAN, same Docker network)
    JSON parsing      ~1–2 ms  (Pydantic, Rust-backed in v2)
    Feature engineering ~2 ms  (NumPy, vectorised)
    IsolationForest  ~5–30 ms  (50 trees, n_jobs=-1)
    JSON serialisation ~1 ms
    ─────────────────────────
    Total budget     ≤ ~50 ms  (comfortable margin)

Concurrency
-----------
FastAPI runs on Uvicorn (asyncio event loop).  The predict-evictions endpoint
uses a synchronous CPU-bound function (sklearn + numpy).  To avoid blocking
the event loop we run it in a thread pool via asyncio.get_event_loop().
run_in_executor().  This lets the event loop continue serving other requests
(e.g. /health checks) while inference runs on a worker thread.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from fastapi import FastAPI, HTTPException

from schemas import EvictionRequest, EvictionResponse, HealthResponse
from feature_engineering import build_feature_matrix
from model import predict_cold_keys


# ---------------------------------------------------------------------------
# Thread pool for CPU-bound inference
# ---------------------------------------------------------------------------

# A dedicated thread pool isolates sklearn/numpy work from the asyncio loop.
# max_workers=4 is plenty for inference batches — IsolationForest already
# uses n_jobs=-1 internally and spawns its own joblib threads.
_INFERENCE_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="aerocache-ai-infer")


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown hooks
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager (FastAPI ≥ 0.93 recommended pattern).

    On startup  : warm up the thread pool with a dummy task so the first real
                  request is not penalised by thread creation overhead.
    On shutdown : shut down the thread pool gracefully (waits for in-flight tasks).
    """
    # ── Startup ──────────────────────────────────────────────────────────────
    import numpy as np
    from model import predict_cold_keys as _pcl
    from feature_engineering import build_feature_matrix as _bfm

    # Warm-up: run a tiny prediction so sklearn's internal caches are populated.
    # This costs ~50 ms at startup but saves it on the first real request.
    try:
        loop = asyncio.get_event_loop()
        dummy_entries = [
            type("E", (), {
                "key": f"warmup-{i}",
                "frequency": i,
                "last_accessed": time.time() * 1000 - i * 1000,
                "timestamps": [time.time() * 1000 - j * 500 for j in range(5)],
            })()
            for i in range(10)
        ]
        feats, keys = _bfm(dummy_entries)           # type: ignore[arg-type]
        await loop.run_in_executor(_INFERENCE_POOL,
                                   partial(_pcl, feats, keys, 2))
        print("[AeroCache-AI] Warm-up complete. Service ready.", flush=True)
    except Exception as exc:
        # Warm-up failure is non-fatal — log and continue.
        print(f"[AeroCache-AI] Warm-up warning: {exc}", flush=True)

    yield  # ← application runs here

    # ── Shutdown ─────────────────────────────────────────────────────────────
    _INFERENCE_POOL.shutdown(wait=True)
    print("[AeroCache-AI] Thread pool shut down. Goodbye.", flush=True)


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AeroCache AI Eviction Service",
    description=(
        "Lightweight IsolationForest microservice that identifies 'cold' cache "
        "keys for predictive eviction in the AeroCache distributed cache cluster."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    tags=["ops"],
)
async def health() -> HealthResponse:
    """
    Returns ``{"status": "ok"}`` immediately.

    Used by:
    - Docker Compose ``healthcheck`` to gate dependent service startup.
    - Java ``AIEvictionPolicy`` is NOT required to call this; it hits
      ``/predict-evictions`` directly.  But it is useful for monitoring.
    """
    return HealthResponse(status="ok", service="aerocache-ai")


# ---------------------------------------------------------------------------
# POST /predict-evictions
# ---------------------------------------------------------------------------

@app.post(
    "/predict-evictions",
    response_model=EvictionResponse,
    summary="Identify cold keys for eviction",
    tags=["eviction"],
)
async def predict_evictions(request: EvictionRequest) -> EvictionResponse:
    """
    Accept cache access statistics and return the coldest keys to evict.

    **Request** (JSON body):
    ```json
    {
        "evict_count": 5,
        "access_logs": [
            {
                "key":           "session:user123",
                "frequency":     42,
                "last_accessed": 1714500000000,
                "timestamps":    [1714499900000, 1714499950000]
            }
        ]
    }
    ```

    **Response**:
    ```json
    {
        "cold_keys":         ["session:zombie", "temp:abc"],
        "model_used":        "IsolationForest",
        "inference_time_ms": 18.4,
        "keys_evaluated":    1000
    }
    ```

    **Performance contract**: The entire endpoint must complete in < 100 ms
    to comfortably beat the Java engine's 200 ms hard timeout.

    **Error handling**: Returns HTTP 422 (handled by FastAPI/Pydantic) for
    malformed payloads, HTTP 500 for unexpected inference errors.  The Java
    engine treats any non-2xx response as a signal to fall back to LRU.
    """
    t0 = time.perf_counter()

    # ── Step 1: Feature engineering ─────────────────────────────────────────
    # Pure NumPy — fast, but we run it in the thread pool anyway to keep the
    # async event loop unblocked for concurrent /health calls.
    loop = asyncio.get_event_loop()

    try:
        features, keys = await loop.run_in_executor(
            _INFERENCE_POOL,
            partial(build_feature_matrix, request.access_logs),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Feature engineering failed: {exc}",
        ) from exc

    # ── Step 2: IsolationForest inference ────────────────────────────────────
    # CPU-bound — runs on a worker thread via the thread pool so the event
    # loop remains responsive during the sklearn fit + score phase.
    try:
        cold_keys, model_name = await loop.run_in_executor(
            _INFERENCE_POOL,
            partial(predict_cold_keys, features, keys, request.evict_count),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Inference failed: {exc}",
        ) from exc

    # ── Step 3: Build and return response ───────────────────────────────────
    elapsed_ms = (time.perf_counter() - t0) * 1_000.0

    return EvictionResponse(
        cold_keys=cold_keys,
        model_used=model_name,
        inference_time_ms=round(elapsed_ms, 2),
        keys_evaluated=len(keys),
    )
