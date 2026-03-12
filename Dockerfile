# ──────────────────────────────────────────────────────────────────────────────
#  TimeStar multi-stage Docker build
# ──────────────────────────────────────────────────────────────────────────────
#  Build:  docker build -t timestar .
#  Run:    docker run --rm -p 8086:8086 timestar
# ──────────────────────────────────────────────────────────────────────────────

# ---------------------------------------------------------------------------
#  Stage 1 — build
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS builder

ARG ENABLE_LTO=OFF

ENV DEBIAN_FRONTEND=noninteractive

# Seastar + TimeStar build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    g++-14 gcc-14 \
    cmake \
    ninja-build \
    git \
    pkg-config \
    python3 python3-pyelftools python3-yaml \
    ragel \
    meson \
    stow \
    libtool \
    # Seastar deps
    libboost-all-dev \
    libc-ares-dev \
    libcrypto++-dev \
    libfmt-dev \
    libgnutls28-dev \
    libhwloc-dev \
    liblz4-dev \
    libnuma-dev \
    libpciaccess-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libsctp-dev \
    liburing-dev \
    libxml2-dev \
    libyaml-cpp-dev \
    systemtap-sdt-dev \
    valgrind \
    xfslibs-dev \
    # TimeStar deps
    libsnappy-dev \
    libleveldb-dev \
    libxxhash-dev \
    && rm -rf /var/lib/apt/lists/*

# Use GCC 14
ENV CC=gcc-14 CXX=g++-14

WORKDIR /src
COPY . .

# Initialise the seastar submodule (stripped from context by .dockerignore's .git/ exclusion,
# so we need the full source tree — see note below)
# NOTE: .dockerignore excludes .git/ for speed. If you use this Dockerfile,
# either: (a) remove ".git/" from .dockerignore, or (b) ensure external/seastar
# is already populated (e.g. via `git submodule update --init` before `docker build`).

RUN mkdir -p build && cd build && \
    cmake .. \
      -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG" \
      -DCMAKE_C_COMPILER=gcc-14 \
      -DCMAKE_CXX_COMPILER=g++-14 \
      -DTIMESTAR_ENABLE_LTO=${ENABLE_LTO} \
      -DTIMESTAR_NATIVE_ARCH=OFF \
    && ninja timestar_http_server timestar_unit_test timestar_test

# Strip debug symbols — shrinks binary from ~500 MB to ~20 MB
RUN strip --strip-all build/bin/timestar_http_server

# ---------------------------------------------------------------------------
#  Stage 2 — minimal runtime image
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# Only runtime shared libraries — no -dev packages, no compilers
RUN apt-get update && apt-get install -y --no-install-recommends \
    libboost-program-options1.83.0 \
    libc-ares2 \
    libfmt9 \
    liblz4-1 \
    libgnutls30t64 \
    libatomic1 \
    libsctp1 \
    libprotobuf32t64 \
    libyaml-cpp0.8 \
    libhwloc15 \
    libsnappy1v5 \
    libleveldb1d \
    libxxhash0 \
    libnuma1 \
    liburing2 \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user and data directory
RUN useradd -r -s /bin/false -d /var/lib/timestar timestar \
    && mkdir -p /var/lib/timestar \
    && chown timestar:timestar /var/lib/timestar

COPY --from=builder /src/build/bin/timestar_http_server /usr/local/bin/timestar_http_server

# Persistent data volume
VOLUME /var/lib/timestar

EXPOSE 8086

# Container-friendly defaults
ENV TIMESTAR_DATA_DIR=/var/lib/timestar \
    TIMESTAR_OVERPROVISIONED=true

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD wget -q --spider http://localhost:8086/health || exit 1

USER timestar

ENTRYPOINT ["timestar_http_server"]
