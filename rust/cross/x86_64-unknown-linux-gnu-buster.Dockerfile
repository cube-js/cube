FROM debian:buster-slim

RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y curl pkg-config wget gnupg git apt-transport-https ca-certificates && \
    echo 'deb https://apt.llvm.org/buster/ llvm-toolchain-buster-9 main' >> /etc/apt/sources.list && \
    curl -JL http://llvm.org/apt/llvm-snapshot.gpg.key | apt-key add - && \
    apt-get update && \
    apt-get install -y llvm-9 clang-9 libclang-9-dev clang-9 make;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-9 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-9 100
RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-9 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-9 100

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1h

RUN wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz -O - | tar -xz &&\
    cd openssl-${OPENSSL_VERSION} && \
    ./Configure no-shared no-async --prefix=/openssl --openssldir=/openssl/ssl linux-x86_64-clang && \
    make depend && \
    make -j $(nproc) && \
    make install_sw && \
    make install_ssldirs && \
    cd .. && rm -rf ${OPENSSL_VERSION}

ENV PKG_CONFIG_ALLOW_CROSS=1
ENV OPENSSL_STATIC=true
ENV OPENSSL_DIR=/openssl

ENV PATH="/cargo/bin:$PATH"
