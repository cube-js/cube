FROM rust:1.47-buster as builder
# Crashing....
# FROM rustlang/rust:nightly-buster as builder

# Because error[E0554]: `#![feature]` may not be used on the stable release channel
# /build/arrow/rust/arrow/src/lib.rs:127:1
# #![feature(specialization)]
RUN rustup update && \
    rustup default nightly && \
    rustup component add rustfmt

RUN apt update && apt upgrade -y && apt install -y git llvm-dev libclang-dev clang

WORKDIR /usr/src

WORKDIR /build
RUN git clone -b cubestore https://github.com/cube-js/arrow.git

WORKDIR /build/cubestore
COPY Cargo.toml .
COPY crates/cubestore/Cargo.toml crates/cubestore/Cargo.toml
RUN mkdir -p crates/cubestore/src/bin && \
    echo "fn main() {print!(\"Dummy main\");} // dummy file" > crates/cubestore/src/bin/cubestored.rs
RUN cargo build --release

COPY . .
RUN cargo build --release
RUN cargo install --path crates/cubestore

FROM debian:buster-slim

WORKDIR /cube
COPY --from=builder /usr/local/cargo/bin/cubestored .

CMD ["./cubestored"]