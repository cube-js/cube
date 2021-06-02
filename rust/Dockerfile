FROM rust:1.47-buster as builder
# Crashing....
# FROM rustlang/rust:nightly-buster as builder

# Because error[E0554]: `#![feature]` may not be used on the stable release channel
# /build/arrow/rust/arrow/src/lib.rs:127:1
# #![feature(specialization)]
RUN rustup update && \
    rustup default nightly-2021-02-17 && \
    rustup component add --toolchain nightly-2021-02-17 rustfmt

RUN apt update && apt upgrade -y && apt install -y git llvm-dev libclang-dev clang

WORKDIR /usr/src

WORKDIR /build/cubestore
COPY Cargo.toml .
COPY Cargo.lock .
COPY cubehll cubehll
COPY cubezetasketch cubezetasketch
COPY cuberpc cuberpc
COPY cubestore-sql-tests cubestore-sql-tests
COPY cubestore/Cargo.toml cubestore/Cargo.toml
RUN mkdir -p cubestore/src/bin && \
    echo "fn main() {print!(\"Dummy main\");} // dummy file" > cubestore/src/bin/cubestored.rs
RUN cargo build --release -p cubestore

COPY cubestore cubestore
RUN cargo build --release -p cubestore

FROM debian:buster-slim

WORKDIR /cube

RUN set -ex; \
	apt-get update; \
	apt-get install -y libssl1.1 curl

COPY --from=builder /build/cubestore/target/release/cubestored .

EXPOSE 3306

ENV RUST_BACKTRACE=true

CMD ["./cubestored"]
