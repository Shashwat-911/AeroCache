# =============================================================================
# AeroCache Java Node — Multi-Stage Dockerfile
# =============================================================================
#
# Stage 1 (builder): Full JDK to compile all Java sources.
# Stage 2 (runtime): JRE-only image — no compiler, no source code.
#                    Smaller attack surface and faster container pulls.
#
# Why eclipse-temurin:21?
#   - eclipse-temurin is the community distribution of OpenJDK maintained by
#     the Adoptium working group. It has better long-term support guarantees
#     than openjdk Docker images, which are officially deprecated.
#   - The -slim variant strips non-essential system packages, keeping the
#     image footprint at ~175 MB vs ~480 MB for the full Debian image.
#   - Java 21 is required for Virtual Threads (Loom), which TCPServer uses.
# =============================================================================


# -----------------------------------------------------------------------------
# Stage 1 — Builder
# -----------------------------------------------------------------------------
FROM eclipse-temurin:21-jdk-jammy AS builder

LABEL stage="builder"

# Working directory inside the builder container.
WORKDIR /build

# Copy the entire Java source tree.
# We copy everything first so Docker layer-caching works correctly:
# source changes invalidate this layer, but the base image layer is reused.
COPY src/ ./src/

# Create the output directory for compiled .class files.
RUN mkdir -p /build/classes

# Compile every .java file found under src/ in a single javac invocation.
#
# Flags explained:
#   -encoding UTF-8   — ensure string literals with non-ASCII chars compile correctly
#   --release 21      — target Java 21 class file format (enables Virtual Threads API)
#   -d /build/classes — write .class files into the output dir (preserving package dirs)
#   $(find ...)       — collect every .java file recursively; avoids maintaining
#                       a manual source list in the Dockerfile
RUN find ./src -name "*.java" -print0 \
    | xargs -0 javac \
        -encoding UTF-8 \
        --release 21 \
        -d /build/classes


# -----------------------------------------------------------------------------
# Stage 2 — Runtime
# -----------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS runtime

# OCI image labels — useful in Docker Hub / GitHub Container Registry.
LABEL org.opencontainers.image.title="aerocache-node" \
      org.opencontainers.image.description="AeroCache distributed cache node (Java 21)" \
      org.opencontainers.image.version="1.0.0"

WORKDIR /app

# Copy ONLY the compiled .class files from the builder stage.
# No JDK, no source code, no javac — the runtime image is minimal.
COPY --from=builder /build/classes ./classes

# The TCP port this node listens on.
# The actual value is injected at runtime via the CACHE_PORT environment
# variable, which Main.java reads with System.getenv("CACHE_PORT").
# Docker requires EXPOSE to document the intended port — it does NOT
# publish it to the host (that's docker-compose's `ports:` job).
EXPOSE 6001

# -----------------------------------------------------------------------------
# Runtime configuration defaults
# These are overridden by docker-compose.yml environment: blocks.
# Providing defaults here allows running the image standalone for testing:
#   docker run -e CACHE_PORT=6001 -e CACHE_ROLE=MASTER aerocache-node
# -----------------------------------------------------------------------------
ENV CACHE_PORT=6001 \
    CACHE_ROLE=MASTER \
    CACHE_CAPACITY=10000 \
    NODE_HOST=localhost \
    AI_HOST=localhost \
    AI_PORT=8000 \
    AI_ENABLED=true

# JVM tuning flags:
#   -XX:+UseZGC              — Z Garbage Collector; low-pause-time GC ideal for
#                              latency-sensitive caches (available in Java 21).
#   -Xms256m / -Xmx512m     — Reasonable heap bounds for a prototype. Production
#                              nodes would scale this with CACHE_CAPACITY.
#   -Djava.util.logging.*   — Route JUL (java.util.logging) to a simple
#                              single-line console format for docker compose logs.
ENTRYPOINT ["java", \
    "-XX:+UseZGC", \
    "-Xms256m", "-Xmx512m", \
    "-Djava.util.logging.SimpleFormatter.format=%1$tT [%4$s] %5$s%6$s%n", \
    "-cp", "/app/classes", \
    "aerocache.Main"]
