FROM rust:1-bullseye as builder

RUN rustup update && \
    rustup default nightly-2022-06-22 && \
    rustup component add --toolchain nightly-2022-06-22 rustfmt;

RUN apt update \
	&& apt upgrade -y \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget gnupg git apt-transport-https ca-certificates \
	&& wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
	&& add-apt-repository "deb https://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-15 main"  \
	&& apt update \
	&& apt install -y git llvm-15 clang-15 libclang-15-dev clang-15 cmake \
	&& rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-15 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-15 100
RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-15 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-15 100

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

FROM debian:bullseye-slim

WORKDIR /cube

RUN set -ex; \
	apt-get update; \
	apt-get install -y libssl1.1 curl

COPY --from=builder /build/cubestore/target/release/cubestored .

EXPOSE 3306

ENV RUST_BACKTRACE=true

CMD ["./cubestored"]
