# Based on top of ubuntu 20.04
# TODO: Migrate to https://github.com/cross-rs/cross/pkgs/container/aarch64-unknown-linux-gnu, when it will be released!
# https://github.com/rust-embedded/cross/blob/master/docker/Dockerfile.aarch64-unknown-linux-gnu
FROM cubejs/cross-aarch64-unknown-linux-gnu:31122022

ARG LLVM_VERSION=18

RUN apt-get update \
    && apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget curl apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && add-apt-repository "deb https://apt.llvm.org/focal/ llvm-toolchain-focal-$LLVM_VERSION main"  \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y libffi-dev binutils-multiarch binutils-aarch64-linux-gnu gcc-multilib g++-multilib \
    # llvm14-dev will install python 3.8 as bin/python3
    && DEBIAN_FRONTEND=noninteractive apt-get install -y llvm-$LLVM_VERSION lld-$LLVM_VERSION clang-$LLVM_VERSION libclang-$LLVM_VERSION-dev clang-$LLVM_VERSION \
        make cmake libsasl2-dev \
        libc6 libc6-dev libc6-arm64-cross libc6-dev-arm64-cross \
        gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/clang-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/lld lld /usr/bin/lld-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-$LLVM_VERSION 100;

ENV ARCH=arm \
    MACHINE=armv8 \
    AS=aarch64-linux-gnu-as \
    AR=aarch64-linux-gnu-ar \
    CC=aarch64-linux-gnu-gcc \
    CXX=aarch64-linux-gnu-g++ \
    CPP=aarch64-linux-gnu-cpp \
    LD=aarch64-linux-gnu-ld

ENV ZLIB_VERSION=1.3.1
RUN wget https://zlib.net/zlib-${ZLIB_VERSION}.tar.gz -O - | tar -xz && \
    cd zlib-${ZLIB_VERSION} && \
    ./configure --prefix=/usr/aarch64-linux-gnu && \
    make -j $(nproc) && \
    make install && \
    cd .. && rm -rf zlib-${ZLIB_VERSION};

# https://www.openssl.org/source/old/1.1.1/
ENV OPENSSL_VERSION=1.1.1w
RUN wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz -O - | tar -xz &&\
    cd openssl-${OPENSSL_VERSION} && \
    ./Configure --prefix=/usr/aarch64-linux-gnu --openssldir=/usr/aarch64-linux-gnu/lib linux-aarch64 && \
    make depend && \
    make -j $(nproc) && \
    make install_sw && \
    make install_ssldirs && \
    cd .. && rm -rf openssl-${OPENSSL_VERSION}

ENV PYO3_CROSS_PYTHON_VERSION=3.11 \
    PYO3_CROSS_INCLUDE_DIR=/usr/aarch64-linux-gnu/include \
    PYO3_CROSS_LIB_DIR=/usr/aarch64-linux-gnu/lib \
    OPENSSL_DIR=/usr/aarch64-linux-gnu \
    OPENSSL_STATIC=yes \
    OPENSSL_INCLUDE_DIR=/usr/aarch64-linux-gnu/include \
    OPENSSL_LIB_DIR=/usr/aarch64-linux-gnu/lib \
    OPENSSL_LIBRARIES=/usr/aarch64-linux-gnu/lib

ENV PKG_CONFIG_ALLOW_CROSS=true \
    PKG_CONFIG_ALL_STATIC=true \
    RUSTFLAGS="-C target-feature=-crt-static" \
    LIBZ_SYS_STATIC=1

ENV PATH="/cargo/bin:$PATH"
