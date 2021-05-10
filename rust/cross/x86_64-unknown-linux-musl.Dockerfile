# Based on top of ubuntu 20.04
# https://github.com/rust-embedded/cross/blob/master/docker/Dockerfile.x86_64-unknown-linux-musl
FROM rustembedded/cross:x86_64-unknown-linux-musl

RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y curl pkg-config wget musl-tools libc6-dev apt-transport-https ca-certificates && \
    echo 'deb https://apt.llvm.org/focal/ llvm-toolchain-focal-9 main' >> /etc/apt/sources.list && \
    curl -JL http://llvm.org/apt/llvm-snapshot.gpg.key | apt-key add - && \
    apt-get update && \
    apt-get install -y llvm-9 clang-9 libclang-9-dev clang-9 make;

RUN ln -s /usr/include/x86_64-linux-gnu/asm /usr/include/x86_64-linux-musl/asm && \
    ln -s /usr/include/asm-generic /usr/include/x86_64-linux-musl/asm-generic && \
    ln -s /usr/include/linux /usr/include/x86_64-linux-musl/linux && \
    ln -s /usr/bin/g++ /usr/bin/musl-g++

RUN mkdir /musl

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1h

RUN wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz -O - | tar -xz &&\
    cd openssl-${OPENSSL_VERSION} && \
    CC="musl-gcc -fPIE -pie" ./Configure no-shared no-async --prefix=/musl --openssldir=/musl/ssl linux-x86_64 && \
    make depend && \
    make -j $(nproc) && \
    make install_sw && \
    make install_ssldirs && \
    cd .. && rm -rf openssl-${OPENSSL_VERSION}

ENV PKG_CONFIG_ALLOW_CROSS=true
ENV PKG_CONFIG_ALL_STATIC=true
ENV RUSTFLAGS="-C target-feature=-crt-static"

ENV OPENSSL_STATIC=true
ENV OPENSSL_DIR=/musl


ENV PATH="/cargo/bin:$PATH"
