ARG OS_NAME=bookworm-slim

FROM rust:$OS_NAME

ARG LLVM_VERSION=18

RUN rustup update && \
    rustup default nightly-2025-08-01 && \
    rustup component add --toolchain nightly-2025-08-01 rustfmt clippy;

RUN apt update \
    && apt upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common libssl-dev pkg-config wget gnupg git apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    # https://github.com/llvm/llvm-project/issues/62475 \
    # add it twice to workaround:
    && add-apt-repository --yes "deb https://apt.llvm.org/bookworm/ llvm-toolchain-bookworm-$LLVM_VERSION main" \
    && add-apt-repository --yes "deb https://apt.llvm.org/bookworm/ llvm-toolchain-bookworm-$LLVM_VERSION main" \
    && apt update \
    && apt install -y git llvm-$LLVM_VERSION clang-$LLVM_VERSION libclang-$LLVM_VERSION-dev clang-$LLVM_VERSION lld-$LLVM_VERSION cmake \
    && rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/clang-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/lld lld /usr/bin/lld-$LLVM_VERSION 100;

WORKDIR /usr/src
