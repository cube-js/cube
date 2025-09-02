# This is the Dockerfile for gorgias to use.
# It is based on the official cubejs/cubestore image, except with our own cubestore changes.

# builder section is copied from cubestore/Dockerfile
FROM cubejs/rust-builder:bookworm-llvm-18 AS builder

WORKDIR /build/cubestore
COPY Cargo.toml .
COPY Cargo.lock .
COPY cuberockstore cuberockstore
COPY cubehll cubehll
COPY cubezetasketch cubezetasketch
COPY cubedatasketches cubedatasketches
COPY cuberpc cuberpc
COPY cubestore-sql-tests cubestore-sql-tests
COPY cubestore/Cargo.toml cubestore/Cargo.toml
RUN mkdir -p cubestore/src/bin && \
    echo "fn main() {print!(\"Dummy main\");} // dummy file" > cubestore/src/bin/cubestored.rs

ARG WITH_AVX2=1
RUN [ "$WITH_AVX2" -eq "1" ] && export RUSTFLAGS="-C target-feature=+avx2"; \
    cargo build --release -p cubestore

# Cube Store get version from his own package
COPY package.json package.json
COPY cubestore cubestore
RUN [ "$WITH_AVX2" -eq "1" ] && export RUSTFLAGS="-C target-feature=+avx2"; \
    cargo build --release -p cubestore


FROM cubejs/cubestore:v0.35.79

COPY --from=builder /build/cubestore/target/release/cubestored .

EXPOSE 3306

CMD ["./cubestored"]
