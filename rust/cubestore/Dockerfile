FROM cubejs/rust-builder:bookworm-llvm-18 AS builder

WORKDIR /build/cubestore

COPY cubeshared /build/cubeshared

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
RUN [ "$WITH_AVX2" -eq "1" ] && export RUSTFLAGS="-C target-feature=+avx2"; \
    cargo build --release -p cubestore

# Cube Store get version from his own package
COPY cubestore/package.json package.json
COPY cubestore/cubestore cubestore
RUN [ "$WITH_AVX2" -eq "1" ] && export RUSTFLAGS="-C target-feature=+avx2"; \
    cargo build --release -p cubestore

FROM debian:bookworm-slim

WORKDIR /cube

RUN set -ex; \
    apt-get update; \
    apt-get install -y libssl3 ca-certificates

COPY --from=builder /build/cubestore/target/release/cubestored .

EXPOSE 3306

ENV RUST_BACKTRACE=true

CMD ["./cubestored"]
