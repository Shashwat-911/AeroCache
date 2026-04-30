"""
schemas.py — Pydantic request/response models for the AeroCache AI microservice.

All models use strict typing so FastAPI validates the incoming JSON payload before
it ever reaches the feature engineering or inference code.  Invalid payloads are
rejected at the HTTP boundary with an automatic 422 response — the Java engine
will see a non-2xx status and fall back to LRU cleanly.
"""

from __future__ import annotations

from typing import List
from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class AccessEntry(BaseModel):
    """
    Statistics for a single cache key, as recorded by CacheEngine.AccessStats.

    Matches the JSON object built by AIEvictionPolicy.buildRequestJson():
        {
            "key":           "session:user123",
            "frequency":     42,
            "last_accessed": 1714500000000,
            "timestamps":    [1714499900000, 1714499950000, 1714500000000]
        }

    Field notes
    -----------
    key           — The cache key string (may contain colons, slashes, etc.).
    frequency     — Total number of GET hits recorded for this key.  Higher
                    frequency = hotter key.  Never negative.
    last_accessed — Epoch milliseconds of the most recent access.  Java's
                    System.currentTimeMillis() output.
    timestamps    — Rolling window of the last ≤20 access timestamps (epoch ms),
                    oldest first.  Used to compute inter-access intervals.
                    May be empty for keys that were written but never read.
    """
    key:           str         = Field(..., min_length=1, description="Cache key string")
    frequency:     int         = Field(..., ge=0,         description="Total access count")
    last_accessed: float       = Field(..., ge=0,         description="Last access epoch-ms")
    timestamps:    List[float] = Field(default_factory=list,
                                       description="Up to 20 recent access timestamps (epoch-ms)")

    @field_validator("timestamps")
    @classmethod
    def cap_timestamps(cls, v: List[float]) -> List[float]:
        """Silently cap the timestamp list at 20 entries to bound memory usage."""
        return v[-20:] if len(v) > 20 else v


class EvictionRequest(BaseModel):
    """
    Payload sent by AIEvictionPolicy.evict() over HTTP POST.

    Fields
    ------
    access_logs  — One AccessEntry per key currently in the cache.
                   The Java side sends ALL keys so the model can compare the
                   entire population and identify relative outliers.
    evict_count  — How many cold keys the Java engine wants back.  Default 5
                   matches AIEvictionPolicy.DEFAULT_EVICT_COUNT.
    """
    access_logs: List[AccessEntry] = Field(
        ...,
        min_length=1,
        description="Access statistics for every key currently in the cache"
    )
    evict_count: int = Field(
        default=5,
        ge=1,
        le=500,
        description="Number of cold keys to return"
    )


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class EvictionResponse(BaseModel):
    """
    Response returned to AIEvictionPolicy.

    Fields
    ------
    cold_keys        — Ordered list of cache keys to evict, coldest first.
                       The Java engine calls engine.evictKeys(coldKeys) and
                       removes them in iteration order.
    model_used       — Human-readable identifier of the algorithm used.
                       Useful for observability dashboards.
    inference_time_ms — Wall-clock time taken for feature engineering + inference,
                        in milliseconds.  Java's 200 ms timeout should give us
                        comfortable headroom.
    keys_evaluated   — Total number of keys the model scored.  Useful for
                        debugging uneven eviction behaviour.
    """
    cold_keys:         List[str] = Field(..., description="Keys to evict, coldest first")
    model_used:        str       = Field(..., description="Algorithm identifier")
    inference_time_ms: float     = Field(..., description="Feature + inference wall time (ms)")
    keys_evaluated:    int       = Field(..., description="Total keys the model scored")


# ---------------------------------------------------------------------------
# Health check model
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    """Response for GET /health."""
    status:  str = "ok"
    service: str = "aerocache-ai"
