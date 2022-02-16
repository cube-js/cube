FROM rust:1.47-buster as builder
# Crashing....
# FROM rustlang/rust:nightly-buster as builder

# Because error[E0554]: `#![feature]` may not be used on the stable release channel
# /build/arrow/rust/arrow/src/lib.rs:127:1
# #![feature(specialization)]
RUN rustup update && \
    rustup default nightly-2021-07-04 && \
    rustup component add --toolchain nightly-2021-07-04 rustfmt

RUN apt update \
	&& apt upgrade -y \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget gnupg git apt-transport-https ca-certificates \
	&& wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
	&& add-apt-repository "deb https://apt.llvm.org/buster/ llvm-toolchain-buster-12 main"  \
	&& apt update \
	&& apt install -y git llvm-12 clang-12 libclang-12-dev clang-12 \
	&& rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-12 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-12 100
RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-12 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-12 100

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

ARG WITH_AVX2=1
RUN [ "$WITH_AVX2" -eq "1" ] && export RUSTFLAGS="-C target-feature=+avx2"; \
	 cargo build --release -p cubestore

# Cube Store get version from his own package
COPY package.json package.json
COPY cubestore cubestore
RUN [ "$WITH_AVX2" -eq "1" ] && export RUSTFLAGS="-C target-feature=+avx2"; \
	cargo build --release -p cubestore

FROM debian:buster-slim

WORKDIR /cube

RUN set -ex; \
	apt-get update; \
	apt-get install -y libssl1.1 curl

COPY --from=builder /build/cubestore/target/release/cubestored .

EXPOSE 3306

# ENV RUST_BACKTRACE=true
ENV RUST_BACKTRACE=full

CMD ["./cubestored"]
