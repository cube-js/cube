FROM cubejs/rust-builder:trixie-llvm-22 AS builder

WORKDIR /build/cubestore

COPY cube/cubeshared /build/cube/cubeshared

COPY cubestore/rust-toolchain.toml .
COPY cubestore/Cargo.toml .
COPY cubestore/Cargo.lock .
COPY cubestore/cuberockstore cuberockstore
COPY cubestore/cubehll cubehll
COPY cubestore/cubezetasketch cubezetasketch
COPY cubestore/cubedatasketches cubedatasketches
COPY cubestore/cuberpc cuberpc
COPY cubestore/cubestore-sql-tests cubestore-sql-tests
COPY cubestore/cubestore/benches cubestore/benches
COPY cubestore/cubestore/Cargo.toml cubestore/Cargo.toml
RUN mkdir -p cubestore/src/bin && \
    echo "fn main() {print!(\"Dummy main\");} // dummy file" > cubestore/src/bin/cubestored.rs

ARG WITH_AVX2=1
ARG TARGETARCH
# jemalloc bakes the page size in at build time. The arm64 builders use 4 KiB
# pages, but arm64 hosts can use up to 64 KiB (Raspberry Pi, Ubuntu
# arm64-largemem, Asahi), so build for the largest one there.
# TODO: drop once we are on jemalloc 5.3.1+, where this is the default.
RUN [ "$WITH_AVX2" -eq "1" ] && export RUSTFLAGS="-C target-feature=+avx2"; \
    [ "$TARGETARCH" = "arm64" ] && export JEMALLOC_SYS_WITH_LG_PAGE="16"; \
    cargo build --release -p cubestore

# Cube Store get version from his own package
COPY cubestore/package.json package.json
COPY cubestore/cubestore cubestore
# TODO: drop the JEMALLOC_SYS_WITH_LG_PAGE export once we are on jemalloc
# 5.3.1+, where this is the default.
RUN [ "$WITH_AVX2" -eq "1" ] && export RUSTFLAGS="-C target-feature=+avx2"; \
    [ "$TARGETARCH" = "arm64" ] && export JEMALLOC_SYS_WITH_LG_PAGE="16"; \
    cargo build --release -p cubestore

FROM debian:trixie-slim

# /cube is group 0 writable for platforms that run the image under an arbitrary
# UID with GID 0, such as OpenShift.
RUN set -ex; \
    apt-get update; \
    apt-get install -y libssl3t64 ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd --gid 10001 cubestore; \
    useradd --uid 10001 --gid cubestore --no-create-home --shell /usr/sbin/nologin cubestore; \
    mkdir -p /cube/.cubestore/data /cube/data; \
    chown -R cubestore:0 /cube; \
    chmod -R g=u /cube

WORKDIR /cube

COPY --from=builder /build/cubestore/target/release/cubestored .
COPY cubestore/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

EXPOSE 3306

ENV RUST_BACKTRACE=true

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["./cubestored"]
