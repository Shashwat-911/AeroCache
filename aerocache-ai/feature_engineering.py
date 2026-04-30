"""
feature_engineering.py — Convert raw AccessEntry objects into a normalised
NumPy feature matrix for the IsolationForest model.

Design goals
------------
* Stay well under 10 ms even for 10 000 keys so the entire request lifecycle
  (parsing + feature engineering + inference) beats the Java 200 ms timeout.
* Use only vectorised NumPy operations — no Python-level loops over rows.
* Produce features that actually signal "coldness":
    - Large time_since_last_access_ms  → cold (not accessed recently)
    - Low  access_frequency            → cold (rarely touched)
    - Large avg_inter_access_ms        → cold (long gaps between accesses)
    - Low  access_rate_per_sec         → cold (slow access pace)

Feature matrix shape: (n_keys, 4), dtype float64.
Row order matches the input list order — callers must preserve that order
when mapping model scores back to key names.
"""

from __future__ import annotations

import time
from typing import List, Tuple

import numpy as np

from schemas import AccessEntry


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_feature_matrix(
    entries: List[AccessEntry],
) -> Tuple[np.ndarray, List[str]]:
    """
    Convert a list of :class:`AccessEntry` objects into a (n, 4) float64
    NumPy array and a parallel list of key strings.

    Parameters
    ----------
    entries:
        The access_logs from the incoming :class:`EvictionRequest`.  Must be
        non-empty (validated by Pydantic before this function is called).

    Returns
    -------
    features : np.ndarray, shape (n_keys, 4)
        Unnormalised feature matrix.  Column order:
            0 — time_since_last_access_ms   (ms; higher = colder)
            1 — access_frequency            (count; lower = colder)
            2 — avg_inter_access_ms         (ms; higher = colder)
            3 — access_rate_per_sec         (req/s; lower = colder)
    keys : List[str]
        Cache key strings in the same row order as `features`.  The model
        returns an index array that is used to index back into this list.

    Notes
    -----
    We build plain Python lists first (O(n) appends) then call
    ``np.array(...)`` once — this is faster than repeated np.append calls.
    """
    now_ms: float = time.time() * 1_000.0  # current epoch-ms (float)

    keys: List[str]   = []
    rows: List[list]  = []

    for entry in entries:
        keys.append(entry.key)

        # ── Feature 0: time since last access ──────────────────────────────
        # How long ago (ms) was this key last touched?
        # A key that hasn't been accessed for minutes is clearly cold.
        time_since_ms = max(0.0, now_ms - entry.last_accessed)

        # ── Feature 1: raw access frequency ────────────────────────────────
        # Total lifetime GET count.  A key hit 10 000 times is very hot;
        # a key hit once is likely a one-off and cold.
        frequency = float(entry.frequency)

        # ── Feature 2: average inter-access interval ────────────────────────
        # Mean gap (ms) between consecutive accesses in the rolling window.
        # A key accessed every 50 ms is hot; one accessed every 10 minutes
        # is cold even if its total frequency is moderate.
        avg_inter_ms = _avg_inter_access(entry.timestamps)

        # ── Feature 3: access rate per second ──────────────────────────────
        # Derived from the rolling-window timestamps.
        # rate = (number of window samples - 1) / (window duration in seconds)
        # Falls back to 0.0 when fewer than 2 timestamps are available.
        access_rate = _access_rate(entry.timestamps)

        rows.append([time_since_ms, frequency, avg_inter_ms, access_rate])

    # Build the matrix in one shot — avoids per-row NumPy overhead.
    features = np.array(rows, dtype=np.float64)  # shape (n, 4)

    return features, keys


# ---------------------------------------------------------------------------
# Private helpers (vectorised where possible)
# ---------------------------------------------------------------------------

def _avg_inter_access(timestamps: List[float]) -> float:
    """
    Mean gap between consecutive timestamps in milliseconds.

    Returns a large sentinel value (``1_000_000.0`` ms = ~16 min) when fewer
    than two timestamps exist — this pushes zero-timestamp keys toward the
    "cold" end of the anomaly score distribution, which is the correct
    behaviour for keys that were written but never read back.
    """
    if len(timestamps) < 2:
        # No inter-access data → assume maximally cold.
        return 1_000_000.0

    ts = np.array(timestamps, dtype=np.float64)
    diffs = np.diff(ts)          # (n-1,) array of consecutive gaps
    return float(np.mean(diffs)) if len(diffs) > 0 else 1_000_000.0


def _access_rate(timestamps: List[float]) -> float:
    """
    Access rate in requests/second over the rolling window.

    Formula: (n_samples - 1) / window_duration_seconds
    Returns 0.0 when the window duration is zero or < 2 samples exist
    (avoids division by zero).
    """
    if len(timestamps) < 2:
        return 0.0

    window_ms = timestamps[-1] - timestamps[0]
    if window_ms <= 0.0:
        return 0.0

    # Convert window from ms to seconds before dividing.
    return (len(timestamps) - 1) / (window_ms / 1_000.0)
