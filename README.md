# AeroCache

> **A lightweight, fault-tolerant, distributed in-memory caching system with an AI-driven predictive eviction strategy.**

[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-green?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://docs.docker.com/compose/)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Tech Stack & Requirements](#tech-stack--requirements)
5. [Quick Start](#quick-start)
6. [Configuration Reference](#configuration-reference)
7. [Wire Protocol](#wire-protocol)
8. [Smart Client SDK](#smart-client-sdk)
9. [AI Eviction Microservice API](#ai-eviction-microservice-api)
10. [Fault Tolerance & Replication](#fault-tolerance--replication)
11. [Cluster Topology](#cluster-topology)
12. [Manual Demo Walkthrough](#manual-demo-walkthrough)
13. [Design Decisions](#design-decisions)
14. [Component Deep-Dive](#component-deep-dive)

---

## Overview

AeroCache is a **systems engineering prototype** demonstrating the design and implementation of a production-grade distributed cache from scratch. Every core data structure — the hash map, the doubly-linked list, the consistent hash ring — is implemented manually using the Java Standard Library only (no Caffeine, Guava, or Ehcache).

### Key Features

| Feature | Implementation |
|---|---|
| **O(1) Cache Operations** | Custom open-addressing `ThreadSafeHashMap` + intrusive `DoublyLinkedList` |
| **Baseline LRU Eviction** | O(1) tail-pop from the DLL, synchronized under a single fair `ReentrantLock` |
| **Consistent Hashing** | MD5 + `TreeMap` ring with 150 virtual nodes per physical node |
| **Async Replication** | `LinkedBlockingQueue` pipeline drains REPLICATE commands to the slave |
| **Heartbeat Failover** | TCP PING every 2 s; slave self-promotes after 3 consecutive misses |
| **Split-Brain Prevention** | Monotonic `AtomicLong` epoch counter; stale-master writes rejected |
| **AI Predictive Eviction** | `IsolationForest` microservice identifies cold keys; 200 ms hard timeout with LRU fallback |
| **Smart Client** | Embedded hash ring routes requests directly to the correct master — zero proxy hops |
| **Virtual Threads** | Java 21 `Executors.newVirtualThreadPerTaskExecutor()` for the TCP server |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     AeroCacheClient (SDK)                       │
│     Local ConsistentHashRing  →  direct TCP to master node      │
└───────────────┬──────────────────────────┬──────────────────────┘
                │                          │
     ┌──────────▼──────────┐   ┌──────────▼──────────┐
     │  node-1  MASTER     │   │  node-3  MASTER     │
     │  port 6001          │   │  port 6003          │
     │  CacheEngine        │   │  CacheEngine        │
     │  TCPServer (VT)     │   │  TCPServer (VT)     │
     │  ReplicationManager │   │  ReplicationManager │
     └──────────┬──────────┘   └──────────┬──────────┘
       REPLICATE│ async                    │ async
     ┌──────────▼──────────┐   ┌──────────▼──────────┐
     │  node-2  SLAVE      │   │  node-4  SLAVE      │
     │  port 6002          │   │  port 6004          │
     │  HeartbeatService   │   │  HeartbeatService   │
     └─────────────────────┘   └─────────────────────┘

     All nodes ──── POST /predict-evictions ────▶ aerocache-ai
                                                  FastAPI + IsolationForest
                                                  port 8000
```

### TCP Communication Flow

```
Client SET user:42 "Alice"
    │
    ▼  (local MD5 hash → TreeMap lookup)
AeroCacheClient resolves key → node-1:6001
    │
    ▼  (persistent socket, no new handshake)
node-1 TCPServer (virtual thread)
    │
    ├──▶ CacheEngine.put("user:42", "Alice")
    │         │
    │         └── if memory ≥ capacity:
    │               AIEvictionPolicy
    │                 │
    │                 ├─[< 200ms]──▶ POST /predict-evictions
    │                 │              IsolationForest → cold_keys[]
    │                 │              engine.evictKeys(cold_keys)
    │                 │
    │                 └─[timeout]──▶ LRUEvictionPolicy (fallback)
    │                                engine.evictLRU()
    │
    ├──▶ +OK  (returned to client immediately)
    │
    └──▶ ReplicationManager.replicateSet("user:42", "Alice")
              LinkedBlockingQueue (non-blocking)
                    │
              [background virtual thread]
                    │
              node-2:6002  REPLICATE SET user:42 Alice
```

---

## Project Structure

```
aerocache/
├── aerocache-node/                      # Java cache node
│   ├── Dockerfile                       # Multi-stage: JDK builder → JRE runtime
│   └── src/main/java/aerocache/
│       ├── Main.java                    # Entry point — reads env vars, wires all components
│       ├── core/
│       │   ├── ThreadSafeHashMap.java   # Custom separate-chaining HashMap + RW locks
│       │   ├── DoublyLinkedList.java    # Intrusive DLL with sentinel head/tail
│       │   ├── CacheEngine.java         # Orchestrates map + DLL + eviction + TTL
│       │   ├── EvictionPolicy.java      # Strategy interface
│       │   ├── LRUEvictionPolicy.java   # O(1) tail-pop baseline
│       │   ├── AIEvictionPolicy.java    # HttpClient → AI service, 200ms timeout
│       │   └── AccessStats.java         # Per-key frequency + timestamp tracking
│       ├── ring/
│       │   ├── CacheNode.java           # Node POJO: host, port, role, epoch
│       │   ├── ConsistentHashRing.java  # TreeMap + MD5 + 150 v-nodes
│       │   └── HashRingManager.java     # Thread-safe ring facade + replication topology
│       ├── net/
│       │   ├── TCPServer.java           # Virtual-thread-per-connection server
│       │   ├── CommandParser.java       # CRLF text protocol parser
│       │   ├── CommandHandler.java      # Routes commands to CacheEngine
│       │   └── TCPClient.java           # Persistent socket + exponential backoff reconnect
│       ├── replication/
│       │   ├── ReplicationManager.java  # Bounded queue → async slave writes
│       │   ├── HeartbeatService.java    # Periodic PING, miss counter, promotion trigger
│       │   ├── PromotionHandler.java    # Slave → master state transition
│       │   └── SplitBrainGuard.java     # AtomicLong epoch, rejects stale-master writes
│       └── client/
│           ├── AeroCacheClient.java     # Smart routing client SDK
│           ├── NodeConnection.java      # Persistent per-node TCP connection pool
│           └── AeroCacheConnectionException.java
│
├── aerocache-ai/                        # Python AI microservice
│   ├── Dockerfile                       # python:3.11-slim + libgomp1 + uvicorn
│   ├── requirements.txt
│   ├── schemas.py                       # Pydantic request/response models
│   ├── feature_engineering.py           # NumPy vectorised feature extraction
│   ├── model.py                         # IsolationForest cold-key predictor
│   └── main.py                          # FastAPI app + ThreadPoolExecutor
│
└── docker-compose.yml                   # 5-service cluster orchestration
```

---

## Tech Stack & Requirements

### Prerequisites

| Tool | Minimum Version | Purpose |
|---|---|---|
| **Docker Desktop** | 24.x | Container runtime |
| **Docker Compose** | v2.x (`compose` plugin) | Multi-container orchestration |
| **Java JDK** | 21 | Local development / testing (optional if using Docker) |
| **Python** | 3.11 | Local development of AI service (optional) |
| **netcat (`nc`)** | any | Demo / smoke testing via CLI |
| **curl** | any | AI service health checks |

> **Docker is the only hard requirement** to run the full cluster. Java and Python are only needed if you want to run components individually outside of Docker.

### Java Dependencies (Standard Library Only)

The Java core engine uses **zero external libraries**. All data structures are implemented from scratch using:

- `java.util.concurrent.locks.ReentrantReadWriteLock` — for `ThreadSafeHashMap`
- `java.util.TreeMap` — for the consistent hash ring
- `java.security.MessageDigest` (MD5) — for ring position hashing
- `java.util.concurrent.LinkedBlockingQueue` — for the replication pipeline
- `java.net.http.HttpClient` (Java 11+) — for AI microservice calls
- `java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()` — Java 21 virtual threads

### Python Dependencies

```
fastapi>=0.110.0
uvicorn[standard]>=0.29.0
pydantic>=2.0.0
scikit-learn>=1.4.0
numpy>=1.26.0
```

---

## Quick Start

### 1. Clone / Navigate to the project

```bash
cd aerocache
```

### 2. Build and start the cluster

```bash
docker compose up --build
```

This command:
1. Builds the Java node image (multi-stage, compiles all `.java` files)
2. Builds the Python AI image (installs scikit-learn, FastAPI, Uvicorn)
3. Starts `aerocache-ai` and waits for its `/health` probe to pass
4. Starts `node-1` and `node-3` (masters) once AI is healthy
5. Starts `node-2` and `node-4` (slaves)

Expected output (condensed):
```
aerocache-ai   | INFO:     Application startup complete.
aerocache-ai   | [AeroCache-AI] Warm-up complete. Service ready.
aerocache-node-1 | AeroCache Node  —  Distributed In-Memory Cache
aerocache-node-1 | Node     : node-1:6001  Role: MASTER
aerocache-node-2 | AeroCache Node  —  Distributed In-Memory Cache
aerocache-node-2 | Node     : node-2:6002  Role: SLAVE
```

### 3. Verify all services are healthy

```bash
docker compose ps
```

```
NAME                STATUS          PORTS
aerocache-ai        Up (healthy)    0.0.0.0:8000->8000/tcp
aerocache-node-1    Up              0.0.0.0:6001->6001/tcp
aerocache-node-2    Up              0.0.0.0:6002->6002/tcp
aerocache-node-3    Up              0.0.0.0:6003->6003/tcp
aerocache-node-4    Up              0.0.0.0:6004->6004/tcp
```

### 4. Run smoke tests

```bash
# SET a key on Master 1
echo -e "SET greeting Hello, AeroCache!\r" | nc localhost 6001

# GET it back
echo -e "GET greeting\r" | nc localhost 6001
# Expected: $Hello, AeroCache!

# Verify replication — read from the SLAVE
echo -e "GET greeting\r" | nc localhost 6002
# Expected: $Hello, AeroCache!  (replicated asynchronously)

# DEL the key
echo -e "DEL greeting\r" | nc localhost 6001
# Expected: +OK

# Confirm miss
echo -e "GET greeting\r" | nc localhost 6001
# Expected: $-1
```

### 5. Test the AI service directly

```bash
curl -s -X POST http://localhost:8000/predict-evictions \
  -H "Content-Type: application/json" \
  -d '{
    "evict_count": 3,
    "access_logs": [
      {"key":"hot:key1","frequency":500,"last_accessed":'"$(date +%s%3N)"',"timestamps":['"$(date +%s%3N)"']},
      {"key":"cold:key2","frequency":1,"last_accessed":1000000,"timestamps":[1000000]},
      {"key":"cold:key3","frequency":2,"last_accessed":1000500,"timestamps":[1000500]},
      {"key":"hot:key4","frequency":450,"last_accessed":'"$(date +%s%3N)"',"timestamps":[]},
      {"key":"cold:key5","frequency":1,"last_accessed":999000,"timestamps":[]}
    ]
  }' | python3 -m json.tool
```

Expected response:
```json
{
  "cold_keys": ["cold:key2", "cold:key3", "cold:key5"],
  "model_used": "IsolationForest",
  "inference_time_ms": 18.4,
  "keys_evaluated": 5
}
```

### 6. Stop the cluster

```bash
docker compose down
```

---

## Configuration Reference

All Java node configuration is injected via environment variables (set in `docker-compose.yml`). You can override any value for custom deployments.

| Variable | Default | Description |
|---|---|---|
| `CACHE_PORT` | `6001` | TCP port the node's server binds to |
| `CACHE_ROLE` | `MASTER` | `MASTER` or `SLAVE` |
| `CACHE_CAPACITY` | `10000` | Maximum number of key-value entries |
| `NODE_HOST` | `localhost` | This node's hostname (used as the ConsistentHashRing seed) |
| `PEER_HOST` | _(none)_ | Hostname of the paired peer node |
| `PEER_PORT` | _(none)_ | Port of the paired peer node |
| `AI_HOST` | `localhost` | Hostname of the Python AI microservice |
| `AI_PORT` | `8000` | Port of the Python AI microservice |
| `AI_ENABLED` | `true` | Set `false` to force pure-LRU mode on this node |

---

## Wire Protocol

AeroCache uses a **human-readable CRLF text protocol** (similar to Redis wire format), making it trivially debuggable via `telnet` or `netcat`.

### Request format
```
COMMAND [arg1] [arg2]\r\n
```

### Commands

| Command | Format | Response (success) | Response (miss/fail) |
|---|---|---|---|
| **GET** | `GET <key>` | `$<value>` | `$-1` |
| **SET** | `SET <key> <value>` | `+OK` | `-ERR <msg>` |
| **DEL** | `DEL <key>` | `+OK` | `$-1` |
| **PING** | `PING` | `+PONG` | — |
| **REPLICATE** | `REPLICATE SET/DEL ...` | `+OK` | `-ERR <msg>` |

### Response prefixes

| Prefix | Meaning |
|---|---|
| `+` | Success status (e.g. `+OK`, `+PONG`) |
| `$` | Bulk string value (e.g. `$Alice`). `$-1` = null / not found |
| `-ERR` | Error — followed by a human-readable message |

### Interactive session example (telnet)

```
$ telnet localhost 6001
Trying 127.0.0.1...
Connected to localhost.

SET session:user1 {"name":"Bob","role":"admin"}
+OK

GET session:user1
${"name":"Bob","role":"admin"}

DEL session:user1
+OK

GET session:user1
$-1

PING
+PONG
```

---

## Smart Client SDK

The `AeroCacheClient` embeds the same consistent hashing ring as the server cluster and routes requests **directly** to the correct master — zero proxy hops.

### Usage

```java
import aerocache.client.AeroCacheClient;
import aerocache.client.AeroCacheConnectionException;
import java.util.List;

public class MyApp {
    public static void main(String[] args) {
        // Seed nodes = the two masters in the cluster
        List<String> masters = List.of(
            "localhost:6001",   // node-1 master
            "localhost:6003"    // node-3 master
        );

        try (AeroCacheClient client = new AeroCacheClient(masters)) {

            // SET — routed directly to the responsible master
            client.set("product:42", "{\"name\":\"Widget\",\"price\":9.99}");

            // GET — same routing, returns null on cache miss
            String json = client.get("product:42");
            System.out.println(json); // {"name":"Widget","price":9.99}

            // DELETE
            client.delete("product:42");

            // Ping all known masters (diagnostic)
            List<String> alive = client.pingAll();
            System.out.println("Alive nodes: " + alive);

        } catch (AeroCacheConnectionException e) {
            System.err.println("Cache node unreachable: " + e.getTargetNode());
            // Implement application-level fallback (e.g. read from DB)
        }
    }
}
```

### Fault tolerance behaviour

```
client.get("mykey")
    │
    ├─ Attempt 1 → node-1:6001 (IOException: connection reset)
    │      NodeConnection auto-invalidates socket
    │      Log: WARNING: First attempt failed. Retrying once...
    │
    ├─ Attempt 2 → node-1:6001 (NodeConnection reconnects transparently)
    │      Success → returns value
    │
    └─ If attempt 2 also fails:
           throws AeroCacheConnectionException("node-1:6001", ...)
           → caller handles fallback
```

---

## AI Eviction Microservice API

### `GET /health`

Liveness probe used by Docker Compose healthcheck.

```bash
curl http://localhost:8000/health
```
```json
{"status": "ok", "service": "aerocache-ai"}
```

### `POST /predict-evictions`

Accepts cache access statistics and returns the coldest keys to evict.

**Request body:**
```json
{
  "evict_count": 5,
  "access_logs": [
    {
      "key": "session:user123",
      "frequency": 42,
      "last_accessed": 1714500000000,
      "timestamps": [1714499900000, 1714499950000, 1714500000000]
    }
  ]
}
```

**Response:**
```json
{
  "cold_keys": ["session:zombie99", "temp:abc"],
  "model_used": "IsolationForest",
  "inference_time_ms": 18.4,
  "keys_evaluated": 1000
}
```

**Field definitions:**

| Field | Type | Description |
|---|---|---|
| `key` | string | Cache key |
| `frequency` | int ≥ 0 | Total lifetime GET count |
| `last_accessed` | float | Epoch milliseconds of last access |
| `timestamps` | float[] | Rolling window of last ≤20 access timestamps |
| `evict_count` | int 1–500 | Number of cold keys to return |

**Features extracted per key:**

| Feature | Signal |
|---|---|
| `time_since_last_access_ms` | Large = cold |
| `access_frequency` | Low = cold |
| `avg_inter_access_interval_ms` | Large = cold |
| `access_rate_per_second` | Low = cold |

**Performance:** Typical inference latency is 15–35 ms for batches of 100–5000 keys. Java's hard timeout is 200 ms; the service internal target is ≤100 ms.

---

## Fault Tolerance & Replication

### Master–Slave Replication

```
master.put("k", "v")
    │
    ├──▶ local CacheEngine write (immediate)
    ├──▶ +OK returned to client (immediate)
    └──▶ ReplicationManager.replicateSet("k", "v")
              LinkedBlockingQueue (non-blocking, cap 10 000 cmds)
                    │
              [background virtual thread — drains queue]
                    │
              slave TCPServer: REPLICATE SET k v
              slave CacheEngine.put("k", "v")
```

### Failover Sequence

```
HeartbeatService (slave-side, runs every 2 s)
    │
    ├── PING → node-1 (miss #1) → WARNING
    ├── PING → node-1 (miss #2) → WARNING
    ├── PING → node-1 (miss #3) → THRESHOLD REACHED
    │
    ├──▶ ringManager.markNodeUnhealthy("node-1:6001")
    │       Traffic immediately rerouted away from node-1
    │
    └──▶ PromotionHandler.promote(self=node-2, failed=node-1)
              │
              ├── self.promote()          → role=MASTER, epoch++
              ├── guard.onPromotion(epoch) → SplitBrainGuard updated
              ├── ringManager.addNode(self) → node-2 now MASTER in ring
              └── SEVERE log banner printed
```

### Split-Brain Prevention

If node-1 recovers after node-2 has promoted itself:

- node-1 epoch = 1, node-2 epoch = 2
- node-1 tries to send `REPLICATE SET ...`
- `SplitBrainGuard.validateReplication(senderEpoch=1)` → **rejected** (1 < 2)
- Warning logged: `⚠ SPLIT-BRAIN DETECTED`
- node-1 must demote itself (operator intervention or future auto-demote extension)

---

## Cluster Topology

```
Pair A (keys hashed to node-1's virtual nodes):
  node-1 (MASTER, :6001)  ──replicates──▶  node-2 (SLAVE, :6002)
  node-2 monitors node-1 via PING every 2s

Pair B (keys hashed to node-3's virtual nodes):
  node-3 (MASTER, :6003)  ──replicates──▶  node-4 (SLAVE, :6004)
  node-4 monitors node-3 via PING every 2s

AI Service:
  aerocache-ai (:8000) — consulted by both masters during eviction
```

Key distribution: with 150 virtual nodes per physical master and MD5 hashing, each master owns ~50% of the key space (±5% std deviation).

---

## Manual Demo Walkthrough

### Demo 1 — Basic CRUD

```bash
# Open a connection to Master 1
nc localhost 6001

SET user:1 Alice
# +OK
GET user:1
# $Alice
SET user:1 AliceUpdated
# +OK  (update in place)
DEL user:1
# +OK
GET user:1
# $-1  (miss)
PING
# +PONG
```

### Demo 2 — Replication

```bash
# Write to Master 1
echo -e "SET replicated:key ValueFromMaster\r" | nc localhost 6001

# Read from Slave 2 (wait ~100ms for async replication)
sleep 0.2
echo -e "GET replicated:key\r" | nc localhost 6002
# Expected: $ValueFromMaster
```

### Demo 3 — Failover (most impressive for portfolio)

```bash
# Open log stream
docker compose logs -f node-2 &

# Kill the master
docker compose stop node-1

# Watch node-2 logs — within 6 seconds you'll see:
# =================================================================
#  AEROCACHE PROMOTION EVENT
#  Failed master : node-1:6001
#  Promoting     : node-2:6002  (SLAVE -> MASTER)
# =================================================================

# node-2 is now the master — write directly to it
echo -e "SET post:failover StillWorking\r" | nc localhost 6002
# +OK
```

### Demo 4 — AI Eviction

```bash
# Watch master logs for AI eviction events
docker compose logs -f node-1 | grep -i "AI\|evict"

# Fill the cache past capacity with a shell loop (requires nc + bash)
for i in $(seq 1 10100); do
  echo -e "SET key:$i value$i\r" | nc -q 1 localhost 6001
done

# You should see log lines like:
# [AIEvictionPolicy] AI eviction: requested=5 evicted=5 cold keys: [key:3, key:7, ...]
```

---

## Design Decisions

| Decision | Rationale |
|---|---|
| **Java stdlib only for core** | Demonstrates low-level systems understanding; no hiding behind Caffeine/Guava |
| **Single `ReentrantLock` over HashMap + DLL** | Atomicity of combined operations (lookup + reorder) is non-negotiable. Striped locks would save contention but add complexity without matching the prototype's scale |
| **150 virtual nodes** | Karger et al. recommend ≥100 for <10% std deviation. 150 gives ~5% at cluster sizes of 2–8 |
| **MD5 for ring hashing** | Deterministic across JVM versions; uniform bit distribution. `String.hashCode()` is JVM-implementation-specific |
| **Bounded replication queue** | Back-pressure signal: if the queue fills, the slave is critically behind. Dropping + logging is safer than OOM |
| **Lazy health bypass on ring** | Marking unhealthy (not removing virtual nodes) avoids O(V log V) write-lock acquisition on every transient miss |
| **Stateless IsolationForest** | Avoids model drift, versioning, and distributed state. Fit-predict per batch costs 15–30ms — acceptable given the 200ms budget |
| **Checked `AeroCacheConnectionException`** | Forces application developers to handle network failures explicitly at compile time |
| **`start_period: 15s` on AI healthcheck** | scikit-learn's first import takes 3–8s; the warm-up prediction adds another 5s. Without start_period, Docker marks the container unhealthy before it's actually ready |

---

## Component Deep-Dive

### ThreadSafeHashMap

- Separate chaining with `Entry<K,V>[]` bucket array
- Wang/Jenkins bit-spreading (`h ^ (h >>> 16)`) to reduce clustering
- `ReentrantReadWriteLock`: concurrent readers / exclusive writers
- Auto-resizes at 0.75 load factor using a single power-of-two table

### DoublyLinkedList

- Sentinel `head` and `tail` nodes — never removed, eliminate all null checks
- `moveToFront(node)` = `remove(node)` + `addToFront(node)`, both O(1)
- `removeLast()` is the LRU eviction primitive — single pointer reassignment

### ConsistentHashRing

- `TreeMap<Long, CacheNode>` — red-black tree gives O(log N) navigation
- `tailMap(h).stream().findFirst()` — clockwise lookup in O(log N)
- Wrap-around: if `tailMap` is empty, fall back to `ring.firstEntry()`
- Unhealthy nodes **skipped inline** during lookup — no ring rebuild required

### AIEvictionPolicy Fallback Chain

```
AI call attempt
    ├── HttpTimeoutException (>200ms)  → LRU fallback
    ├── ConnectException (refused)      → LRU fallback
    ├── HTTP 5xx response               → LRU fallback
    ├── Empty cold_keys[]               → LRU fallback
    └── Any Exception                   → LRU fallback (defensive catch-all)
```

---

*Built as a systems engineering portfolio project. All core data structures implemented from scratch.*
