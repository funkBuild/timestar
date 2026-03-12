FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies (mirrors ci.yml)
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    gcc-14 \
    g++-14 \
    libc-ares-dev \
    libfmt-dev \
    libleveldb-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libsnappy-dev \
    libssl-dev \
    liburing-dev \
    libboost-all-dev \
    liblz4-dev \
    libgnutls28-dev \
    libsctp-dev \
    libhwloc-dev \
    libnuma-dev \
    libpciaccess-dev \
    libcrypto++-dev \
    libxml2-dev \
    xfslibs-dev \
    systemtap-sdt-dev \
    libyaml-cpp-dev \
    libxxhash-dev \
    ragel \
    pkg-config \
    valgrind \
    python3-pip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install clang-format for lint check
RUN pip install --break-system-packages clang-format==22.1.1

WORKDIR /src
