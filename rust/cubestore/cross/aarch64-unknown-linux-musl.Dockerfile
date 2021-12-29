# Based on top of ubuntu 18.04
# https://github.com/rust-embedded/cross/blob/master/docker/Dockerfile.aarch64-unknown-linux-musl
FROM rustembedded/cross:aarch64-unknown-linux-musl

RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y curl pkg-config gnupg wget musl-tools libc6-dev apt-transport-https ca-certificates && \
    echo 'deb https://apt.llvm.org/bionic/ llvm-toolchain-bionic-9 main' >> /etc/apt/sources.list && \
    curl -JL http://llvm.org/apt/llvm-snapshot.gpg.key | apt-key add - && \
    apt-get update && \
    apt-get install -y llvm-9 clang-9 libclang-9-dev clang-9 make;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-9 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-9 100
RUN update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-9 100

RUN mkdir /musl

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1j

ENV MACHINE=armv8
ENV ARCH=arm
ENV CC=aarch64-linux-musl-gcc

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

ENV OPENSSL_STATIC=true
ENV OPENSSL_DIR=/openssl


ENV PATH="/cargo/bin:$PATH"
