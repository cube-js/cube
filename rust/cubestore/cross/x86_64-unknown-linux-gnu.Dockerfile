# libc 2.31 python 3.9
FROM debian:bullseye-slim

ARG LLVM_VERSION=18

RUN apt-get update && apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget curl gnupg git apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && add-apt-repository "deb https://apt.llvm.org/bullseye/ llvm-toolchain-bullseye-$LLVM_VERSION main"  \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y llvm-${LLVM_VERSION} lld-${LLVM_VERSION} clang-$LLVM_VERSION libclang-${LLVM_VERSION}-dev make cmake \
      lzma-dev liblzma-dev libpython3-dev \
    && rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/lld lld /usr/bin/lld-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/clang-$LLVM_VERSION 100 \
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-$LLVM_VERSION 100;

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1w
RUN cd tmp && wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz -O - | tar -xz \
    && cd openssl-${OPENSSL_VERSION} \
    && ./Configure no-shared no-async --prefix=/openssl --openssldir=/openssl/ssl linux-x86_64-clang \
    && make depend \
    && make -j $(nproc) \
    && make install_sw \
    && make install_ssldirs \
    && cd .. && rm -rf openssl-${OPENSSL_VERSION}

ENV PKG_CONFIG_ALLOW_CROSS=1
ENV OPENSSL_STATIC=true
ENV OPENSSL_DIR=/openssl
ENV OPENSSL_ROOT_DIR=/openssl
ENV OPENSSL_LIBRARIES=/openssl/lib

ENV PATH="/cargo/bin:$PATH"
