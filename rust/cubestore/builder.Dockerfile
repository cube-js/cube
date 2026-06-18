ARG RUST_TAG=trixie-slim
ARG OS_NAME=trixie

FROM rust:$RUST_TAG AS base

ARG OS_NAME=trixie
ARG LLVM_VERSION=22

RUN rustup update && \
    rustup default nightly-2025-08-01 && \
    rustup component add --toolchain nightly-2025-08-01 rustfmt clippy;

RUN apt update \
    && apt upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y libssl-dev pkg-config wget gnupg git apt-transport-https ca-certificates \
    && wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /usr/share/keyrings/llvm-snapshot.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/llvm-snapshot.gpg] https://apt.llvm.org/$OS_NAME/ llvm-toolchain-$OS_NAME-$LLVM_VERSION main" > /etc/apt/sources.list.d/llvm.list \
    && apt update \
    && apt install -y git llvm-$LLVM_VERSION clang-$LLVM_VERSION libclang-$LLVM_VERSION-dev clang-$LLVM_VERSION lld-$LLVM_VERSION cmake \
    && rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/clang-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/lld lld /usr/bin/lld-$LLVM_VERSION 100;

# Platform-specific OpenSSL paths for static linking
FROM base AS final-amd64
ENV OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu
ENV OPENSSL_INCLUDE_DIR=/usr/include

FROM base AS final-arm64
ENV OPENSSL_LIB_DIR=/usr/lib/aarch64-linux-gnu
ENV OPENSSL_INCLUDE_DIR=/usr/include

# Select final stage based on target architecture
ARG TARGETARCH=amd64
FROM final-${TARGETARCH}
