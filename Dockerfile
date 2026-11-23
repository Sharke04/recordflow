# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage: Ubuntu + vcpkg (pinned to the manifest baseline), Release build.
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS build

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git curl ca-certificates zip unzip tar \
        pkg-config python3 perl bison flex autoconf automake libtool linux-libc-dev \
 && rm -rf /var/lib/apt/lists/*

# vcpkg at the commit named by "builtin-baseline" in vcpkg.json.
ARG VCPKG_COMMIT=93ad765c7b66ab9ce97469ec23a90b15c3a14ee5
ENV VCPKG_ROOT=/opt/vcpkg \
    VCPKG_DISABLE_METRICS=1
RUN git init -q "$VCPKG_ROOT" \
 && git -C "$VCPKG_ROOT" fetch -q --depth 1 https://github.com/microsoft/vcpkg.git "$VCPKG_COMMIT" \
 && git -C "$VCPKG_ROOT" checkout -q FETCH_HEAD \
 && "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics

# Dependencies first, so that source changes do not rebuild them.
# Release-only triplet: the image never needs debug libraries.
ENV VCPKG_TRIPLET=x64-linux-release
WORKDIR /src
COPY vcpkg.json ./
RUN --mount=type=cache,target=/root/.cache/vcpkg \
    "$VCPKG_ROOT/vcpkg" install \
        --triplet "$VCPKG_TRIPLET" --host-triplet "$VCPKG_TRIPLET" \
        --x-install-root=/opt/vcpkg_installed

COPY CMakeLists.txt CMakePresets.json ./
COPY src ./src
RUN cmake --preset linux \
        -DCMAKE_BUILD_TYPE=Release \
        -DRECORDFLOW_BUILD_TESTS=OFF \
        -DVCPKG_TARGET_TRIPLET="$VCPKG_TRIPLET" \
        -DVCPKG_HOST_TRIPLET="$VCPKG_TRIPLET" \
        -DVCPKG_INSTALLED_DIR=/opt/vcpkg_installed \
        -DVCPKG_MANIFEST_INSTALL=OFF \
 && cmake --build --preset linux \
 && strip build/linux/recordflow

# ---------------------------------------------------------------------------
# Runtime stage: the binary (vcpkg libraries are linked statically) and the
# shared libraries it needs from the base image (libc, libstdc++).
# ---------------------------------------------------------------------------
FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Fixed UID/GID so that volume ownership is predictable (decision 10.4).
ARG RECORDFLOW_UID=10001
RUN groupadd --system --gid "$RECORDFLOW_UID" recordflow \
 && useradd --system --uid "$RECORDFLOW_UID" --gid recordflow \
        --no-create-home --home-dir /var/lib/recordflow --shell /usr/sbin/nologin recordflow \
 && install -d -o recordflow -g recordflow -m 0700 /var/lib/recordflow

COPY --from=build /src/build/linux/recordflow /usr/local/bin/recordflow
COPY docker/recordflow/recordflow.toml /etc/recordflow/recordflow.toml

USER recordflow
WORKDIR /var/lib/recordflow
VOLUME /var/lib/recordflow

# Arguments are appended, so `--check-config` keeps the default file and a
# later `--config PATH` replaces it.
ENTRYPOINT ["/usr/local/bin/recordflow", "--config", "/etc/recordflow/recordflow.toml"]
