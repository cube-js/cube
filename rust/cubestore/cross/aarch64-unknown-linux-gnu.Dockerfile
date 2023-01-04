# Based on top of ubuntu 20.04
# TODO: Migrate to https://github.com/cross-rs/cross/pkgs/container/aarch64-unknown-linux-gnu, when it will be released!
# https://github.com/rust-embedded/cross/blob/master/docker/Dockerfile.aarch64-unknown-linux-gnu
FROM cubejs/cross-aarch64-unknown-linux-gnu:31122022

RUN apt-get update \
    && apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && add-apt-repository "deb https://apt.llvm.org/focal/ llvm-toolchain-focal-14 main"  \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y gcc-multilib g++-multilib \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y llvm-14 clang-14 libclang-14-dev clang-14 \
        make cmake libsasl2-dev \
        libc6 libc6-dev libc6-arm64-cross libc6-dev-arm64-cross \
        gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-14 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-14 100
RUN update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-14 100
RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-14 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-14 100

# https://www.openssl.org/source/old/1.1.1/
ENV OPENSSL_VERSION=1.1.1q
ENV LIZB_VERSION=1.2.13

ENV ARCH=arm \
    MACHINE=armv8 \
    AS=aarch64-linux-gnu-as \
    AR=aarch64-linux-gnu-ar \
    CC=aarch64-linux-gnu-gcc \
    CXX=aarch64-linux-gnu-g++ \
    CPP=aarch64-linux-gnu-cpp \
    LD=aarch64-linux-gnu-ld

RUN wget https://zlib.net/zlib-${LIZB_VERSION}.tar.gz -O - | tar -xz && \
    cd zlib-${LIZB_VERSION} && \
    ./configure --prefix=/usr/aarch64-linux-gnu && \
    make -j $(nproc) && \
    make install && \
    cd .. && rm -rf zlib-${LIZB_VERSION};

RUN wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz -O - | tar -xz &&\
    cd openssl-${OPENSSL_VERSION} && \
    ./Configure --prefix=/usr/aarch64-linux-gnu --openssldir=/usr/aarch64-linux-gnu/lib linux-aarch64 && \
    make depend && \
    make -j $(nproc) && \
    make install_sw && \
    make install_ssldirs && \
    cd .. && rm -rf openssl-${OPENSSL_VERSION}

ENV PKG_CONFIG_ALLOW_CROSS=true
ENV PKG_CONFIG_ALL_STATIC=true
ENV RUSTFLAGS="-C target-feature=-crt-static"

ENV OPENSSL_DIR=/usr/aarch64-linux-gnu \
    OPENSSL_STATIC=yes \
    OPENSSL_INCLUDE_DIR=/usr/aarch64-linux-gnu/include \
    OPENSSL_LIB_DIR=/usr/aarch64-linux-gnu/lib \
    OPENSSL_LIBRARIES=/usr/aarch64-linux-gnu/lib \
    LIBZ_SYS_STATIC=1

ENV PATH="/cargo/bin:$PATH"
