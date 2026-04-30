"""
model.py — IsolationForest-based cold-key predictor.

Design
------
We intentionally keep the model stateless (fit + predict on every request).
This avoids the complexity of:
  - persisting and versioning a trained model on disk
  - managing concept drift as the access pattern evolves
  - synchronising model state across multiple AI service replicas

The tradeoff: we pay O(n * n_estimators) training cost on every call.
With n_estimators=50 and n_keys ≤ 5 000 this costs roughly 5–30 ms on a
modern CPU — well within the 100 ms budget.

Algorithm summary
-----------------
IsolationForest isolates anomalies by recursively partitioning features at
random split points.  Keys that are isolated quickly (short average path
length across all trees) are anomalies — i.e. they differ most from the
majority of the population.

In our context: a key with high time_since_last_access, low frequency, large
inter-access gaps, and low access rate sits far from the dense "hot key"
cluster and will be isolated early → high anomaly score → cold key → evict.

Scoring
-------
decision_function() returns the raw anomaly score per sample:
  - More negative  → more anomalous → colder → higher eviction priority
  - Near zero / positive → inlier → hot key → keep

We sort ascending (most negative first) and return the top `evict_count` keys.
"""

from __future__ import annotations

from typing import List, Tuple

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

# Number of isolation trees.  50 is fast (~5 ms) and stable enough for our
# batch sizes.  The sklearn default is 100 — we halve it to save latency.
N_ESTIMATORS: int = 50

# Expected fraction of outliers in the population.  0.15 = 15% cold keys.
# IsolationForest uses this to set the decision boundary; it does NOT cap
# the number of keys returned — we do that manually via evict_count.
CONTAMINATION: float = 0.15

# Minimum number of samples required to run IsolationForest.
# Below this threshold we fall back to simple frequency-based sorting.
MIN_SAMPLES_FOR_IF: int = 5

# Seed for reproducibility during testing.  Set to None in production if
# you want true randomness (slightly different scores each call).
RANDOM_STATE: int = 42


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def predict_cold_keys(
    features: np.ndarray,
    keys: List[str],
    evict_count: int,
) -> Tuple[List[str], str]:
    """
    Identify the coldest ``evict_count`` keys using IsolationForest.

    Parameters
    ----------
    features:
        (n_keys, 4) float64 array from :func:`feature_engineering.build_feature_matrix`.
        Column layout: [time_since_last_access_ms, frequency,
                        avg_inter_access_ms, access_rate_per_sec].
    keys:
        Parallel list of cache key strings, same row order as ``features``.
    evict_count:
        Number of cold keys to return.  Clamped to [1, n_keys].

    Returns
    -------
    cold_keys : List[str]
        Ordered list of the coldest keys, most anomalous first.
    model_name : str
        Human-readable label for the model used (included in the response).
    """
    n = len(keys)

    # Clamp evict_count so we never try to return more keys than exist.
    evict_count = max(1, min(evict_count, n))

    # ── Fast path: too few samples for IsolationForest ──────────────────────
    if n < MIN_SAMPLES_FOR_IF:
        return _fallback_frequency_sort(keys, features, evict_count), "frequency-fallback"

    # ── Step 1: Normalise features ──────────────────────────────────────────
    # StandardScaler centres each feature to mean=0, std=1.  This prevents
    # time_since_last_access (which can be in the millions of ms) from drowning
    # out access_rate (which is typically < 100 req/s).
    #
    # We fit the scaler on the same batch we are scoring — the model is purely
    # comparative ("which keys are coldest relative to the current population?")
    # so there is no train/test split concern.
    scaler = StandardScaler()
    X = scaler.fit_transform(features)   # shape: (n, 4), float64

    # ── Step 2: Fit and score IsolationForest ───────────────────────────────
    iso_forest = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,   # guides internal threshold, not our cap
        max_samples="auto",            # min(256, n_samples) — keeps it fast
        random_state=RANDOM_STATE,
        n_jobs=-1,                     # use all available CPU cores for fitting
    )
    iso_forest.fit(X)

    # decision_function scores: more negative = more anomalous = colder.
    # Shape: (n,) float64
    scores: np.ndarray = iso_forest.decision_function(X)

    # ── Step 3: Rank and select top cold keys ───────────────────────────────
    # argsort ascending: index 0 = most anomalous (lowest / most-negative score)
    sorted_indices: np.ndarray = np.argsort(scores)

    cold_keys: List[str] = [keys[i] for i in sorted_indices[:evict_count]]

    return cold_keys, "IsolationForest"


# ---------------------------------------------------------------------------
# Fallback: frequency-based sorting (no ML)
# ---------------------------------------------------------------------------

def _fallback_frequency_sort(
    keys: List[str],
    features: np.ndarray,
    evict_count: int,
) -> List[str]:
    """
    When the batch is too small for IsolationForest, simply sort by
    time_since_last_access (col 0) descending — the key not accessed for
    the longest time is evicted first.

    This gives correct intuitive behaviour even with 1–4 keys and requires
    no model fitting.
    """
    # Column 0 = time_since_last_access_ms.  Largest = coldest.
    time_since = features[:, 0]
    # argsort ascending → reverse it to get largest first.
    sorted_idx = np.argsort(time_since)[::-1]
    return [keys[i] for i in sorted_idx[:evict_count]]
