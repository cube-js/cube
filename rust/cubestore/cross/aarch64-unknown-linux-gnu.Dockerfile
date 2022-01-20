# Based on top of ubuntu 16.04
# https://github.com/rust-embedded/cross/blob/master/docker/Dockerfile.aarch64-unknown-linux-gnu
FROM rustembedded/cross:aarch64-unknown-linux-gnu

RUN apt-get update \
    && apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && add-apt-repository "deb https://apt.llvm.org/xenial/ llvm-toolchain-xenial-12 main"  \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y gcc-multilib g++-multilib \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y llvm-12 clang-12 libclang-12-dev clang-12 make \
        libc6 libc6-dev libc6-arm64-cross libc6-dev-arm64-cross \
        gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-12 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-12 100
RUN update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-12 100

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1l

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
