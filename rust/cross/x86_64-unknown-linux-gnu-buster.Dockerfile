FROM debian:buster

RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y curl pkg-config wget llvm libclang-dev clang make

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
