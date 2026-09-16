# Build stage
FROM rust:1.98-slim@sha256:bce1476d4be4d78b83705bc5f428b86d640eeeea33e9dadafbc037b5703a53bf AS builder

# Install build dependencies
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Compiler cache. The statically linked musl release runs on any glibc base and is
# verified against the digests published as the release's .sha256 assets, so the
# builder never trusts an unpinned download.
ARG SCCACHE_VERSION=0.17.0
ARG SCCACHE_SHA256_AMD64=67c4a96dd237c1f518f6b36083f270f9976d516f1e57fce891755ea782e50006
ARG SCCACHE_SHA256_ARM64=821a86343191aa1cbab74bd42f9e93c9a63bf85e4742945f40d3ae84193c1c77
ARG TARGETARCH
RUN set -eu; \
    case "${TARGETARCH:-amd64}" in \
    amd64) sccache_arch=x86_64; sccache_sha256="${SCCACHE_SHA256_AMD64}" ;; \
    arm64) sccache_arch=aarch64; sccache_sha256="${SCCACHE_SHA256_ARM64}" ;; \
    *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    sccache_release="sccache-v${SCCACHE_VERSION}-${sccache_arch}-unknown-linux-musl"; \
    curl -fsSL -o "/tmp/${sccache_release}.tar.gz" \
    "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/${sccache_release}.tar.gz"; \
    printf '%s  /tmp/%s.tar.gz\n' "${sccache_sha256}" "${sccache_release}" > /tmp/sccache.sha256; \
    sha256sum -c /tmp/sccache.sha256; \
    tar -xzf "/tmp/${sccache_release}.tar.gz" -C /tmp; \
    install -m 0755 "/tmp/${sccache_release}/sccache" /usr/local/bin/sccache; \
    rm -rf "/tmp/${sccache_release}" "/tmp/${sccache_release}.tar.gz" /tmp/sccache.sha256; \
    sccache --version

# Route every rustc invocation below through sccache. SCCACHE_DIR is the BuildKit
# cache mount declared on both cargo build steps, so objects survive image rebuilds.
ENV RUSTC_WRAPPER=sccache \
    SCCACHE_DIR=/root/.cache/sccache

# Create app directory
WORKDIR /app

COPY Cargo.lock ./
COPY Cargo.toml ./
COPY benches ./benches

# Create dummy main to cache dependencies
RUN --mount=type=cache,id=sccache-discogs-ingestion,target=/root/.cache/sccache \
    mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    touch src/lib.rs && \
    cargo build --release --locked && \
    rm -rf src

# Copy the vendored vocabularies: media.rs, identifiers.rs, and companies.rs each
# pull one in at compile time via include_str!, so the whole directory must be in
# the build context before `COPY src`. Narrowing this to a single file breaks the
# build the next time a mapper vendors another vocabulary.
# This is only the vendored vocab subtree, not the whole contracts/ tree.
COPY contracts/catalog-events/vocab ./contracts/catalog-events/vocab

# Copy actual source code
COPY src ./src

# Build the application
RUN --mount=type=cache,id=sccache-discogs-ingestion,target=/root/.cache/sccache \
    touch src/main.rs src/lib.rs && \
    cargo build --release --locked && \
    sccache --show-stats

# Runtime stage
FROM debian:13-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

# Build arguments for configurable UID/GID (must match the compose `user:` override)
ARG BUILD_DATE
ARG BUILD_VERSION=0.1.0
ARG VCS_REF
ARG UID=1000
ARG GID=1000

LABEL org.opencontainers.image.title="discogs-ingestion" \
      org.opencontainers.image.description="GrooveMap Discogs catalog ingestion and event publishing" \
      org.opencontainers.image.authors="Robert Wlodarczyk <robert@simplicityguy.com>" \
      org.opencontainers.image.url="https://groovemap.music" \
      org.opencontainers.image.documentation="https://github.com/groovemap-music/discogs-ingestion/blob/main/README.md" \
      org.opencontainers.image.source="https://github.com/groovemap-music/discogs-ingestion" \
      org.opencontainers.image.vendor="GrooveMap" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.base.name="docker.io/library/debian:13-slim"

# Install runtime dependencies
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3t64 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user with a fixed UID/GID so file ownership matches the
# runtime user regardless of any `useradd -r` system-UID auto-allocation.
RUN groupadd -r -g ${GID} extractor && useradd -r -l -u ${UID} -g extractor extractor

# Create necessary directories
RUN mkdir -p /discogs-data /logs && \
    chown -R extractor:extractor /discogs-data /logs

# Copy binary from builder
COPY --from=builder /app/target/release/discogs-ingestion /usr/local/bin/discogs-ingestion

# Package the producer-owned synthetic smoke contract so a released-image test needs no
# sibling repository checkout. LOCAL_MANIFEST is intentionally not set: normal image startup
# must keep using public monthly-dump acquisition unless an operator explicitly selects it.
COPY contracts/extractor-smoke /usr/share/discogs-ingestion/contracts/extractor-smoke

# Switch to non-root user
# UID/GID build arguments resolve to numeric IDs; DL3066 cannot infer their values.
# hadolint ignore=DL3066
USER ${UID}:${GID}

# Set environment variables
ENV LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD ["curl", "-f", "http://localhost:8000/health"]

# Expose health port
EXPOSE 8000

# Run the application
ENTRYPOINT ["discogs-ingestion"]
