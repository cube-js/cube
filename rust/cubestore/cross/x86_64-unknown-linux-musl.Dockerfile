# Based on top of ubuntu 20.04
# https://github.com/rust-embedded/cross/blob/master/docker/Dockerfile.x86_64-unknown-linux-musl
FROM rustembedded/cross:x86_64-unknown-linux-musl

RUN apt-get update \
    && apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget musl-tools libc6-dev apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && add-apt-repository "deb https://apt.llvm.org/focal/ llvm-toolchain-focal-18 main"  \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    # llvm14-dev will install python 3.8 as bin/python3
    && DEBIAN_FRONTEND=noninteractive apt-get install -y llvm-18 clang-18 libclang-18-dev clang-18 make cmake \
    && rm -rf /var/lib/apt/lists/*;

RUN ln -s /usr/include/x86_64-linux-gnu/asm /usr/include/x86_64-linux-musl/asm && \
    ln -s /usr/include/asm-generic /usr/include/x86_64-linux-musl/asm-generic && \
    ln -s /usr/include/linux /usr/include/x86_64-linux-musl/linux && \
    ln -s /usr/bin/g++ /usr/bin/musl-g++

RUN mkdir /musl

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1w
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
ENV OPENSSL_ROOT_DIR=/musl
ENV OPENSSL_LIBRARIES=/musl/lib


ENV PATH="/cargo/bin:$PATH"
