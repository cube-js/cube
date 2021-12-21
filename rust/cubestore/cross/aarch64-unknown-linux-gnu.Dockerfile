# Based on top of ubuntu 16.04
# https://github.com/rust-embedded/cross/blob/master/docker/Dockerfile.aarch64-unknown-linux-gnu
FROM rustembedded/cross:aarch64-unknown-linux-gnu

RUN apt-get update && \
    apt-get install --assume-yes -y wget make git automake autoconf ca-certificates libc6-arm64-cross libc6-dev-arm64-cross apt-transport-https ca-certificates && \
    echo 'deb https://apt.llvm.org/xenial/ llvm-toolchain-xenial-9 main' >> /etc/apt/sources.list && \
    curl -JL http://llvm.org/apt/llvm-snapshot.gpg.key | apt-key add - && \
    apt-get update && \
    apt-get install -y llvm-9 clang-9 libclang-9-dev clang-9 make;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-9 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-9 100
RUN update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-9 100

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1j

ENV MACHINE=armv8
ENV ARCH=arm
ENV CC=aarch64-linux-gnu-gcc

RUN wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz -O - | tar -xz &&\
    cd openssl-${OPENSSL_VERSION} && \
    ./Configure --prefix=/openssl --openssldir=/openssl/lib linux-aarch64 && \
    make depend && \
    make -j $(nproc) && \
    make install_sw && \
    make install_ssldirs && \
    cd .. && rm -rf openssl-${OPENSSL_VERSION}

ENV PKG_CONFIG_ALLOW_CROSS=true
ENV PKG_CONFIG_ALL_STATIC=true
ENV RUSTFLAGS="-C target-feature=-crt-static"

ENV OPENSSL_DIR=/openssl \
    OPENSSL_STATIC=yes \
    OPENSSL_INCLUDE_DIR=/openssl/include \
    OPENSSL_LIB_DIR=/openssl/lib

ENV PATH="/cargo/bin:$PATH"
