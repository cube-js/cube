FROM debian:stretch-slim

RUN apt-get update \
    && apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget gnupg git apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && add-apt-repository "deb https://apt.llvm.org/stretch/ llvm-toolchain-stretch-12 main"  \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y llvm-12 clang-12 libclang-12-dev clang-12 make \
    && rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-12 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-12 100
RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-12 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-12 100

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1l

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
