FROM debian:buster-slim

RUN apt-get update && apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget gnupg git apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && add-apt-repository "deb https://apt.llvm.org/buster/ llvm-toolchain-buster-14 main"  \
    && apt-get update \
    # llvm14-dev will install python 3.8 as bin/python3
    && DEBIAN_FRONTEND=noninteractive apt-get install -y llvm-14 clang-14 libclang-14-dev clang-14 make cmake \
      lzma-dev liblzma-dev \
    && rm -rf /var/lib/apt/lists/*;

RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-14 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-14 100
RUN update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-14 100
RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-14 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-14 100

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1q
RUN cd tmp && wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz -O - | tar -xz \
    && cd openssl-${OPENSSL_VERSION} \
    && ./Configure no-shared no-async --prefix=/openssl --openssldir=/openssl/ssl linux-x86_64-clang \
    && make depend \
    && make -j $(nproc) \
    && make install_sw \
    && make install_ssldirs \
    && cd .. && rm -rf ${OPENSSL_VERSION}

ENV PKG_CONFIG_ALLOW_CROSS=1
ENV OPENSSL_STATIC=true
ENV OPENSSL_DIR=/openssl
ENV OPENSSL_ROOT_DIR=/openssl
ENV OPENSSL_LIBRARIES=/openssl/lib

ARG PYTHON_VERSION=3.11.3
RUN cd tmp && wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz -O - | tar -xz \
    && cd Python-${PYTHON_VERSION} && \
    ./configure  \
      --enable-shared \
      --with-openssl=/openssl \
      --enable-optimizations \
      --disable-ipv6 \
      --prefix=/usr \
    && make -j $(nproc) \
    && make install \
    && ln -f -s /usr/bin/python3.11 /usr/bin/python3 \
    && cd .. && rm -rf Python-${PYTHON_VERSION};

# pyo3 uses python3 to detect version, but there is a bug and it uses python3.7 (system), this force it to use a new python
ENV PYO3_PYTHON=python3.11

ENV PATH="/cargo/bin:$PATH"
