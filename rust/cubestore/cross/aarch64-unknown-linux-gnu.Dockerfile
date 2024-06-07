# Based on top of ubuntu 20.04
# TODO: Migrate to https://github.com/cross-rs/cross/pkgs/container/aarch64-unknown-linux-gnu, when it will be released!
# https://github.com/rust-embedded/cross/blob/master/docker/Dockerfile.aarch64-unknown-linux-gnu
FROM cubejs/cross-aarch64-unknown-linux-gnu:31122022

RUN apt-get update \
    && apt-get -y upgrade \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common pkg-config wget apt-transport-https ca-certificates \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - \
    && add-apt-repository "deb https://apt.llvm.org/focal/ llvm-toolchain-focal-18 main"  \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    # python3 on x86 is required for cross compiling python :D
    && DEBIAN_FRONTEND=noninteractive apt-get install -y python3.12 python3.11 python3.10 python3.9 \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y libffi-dev binutils-multiarch binutils-aarch64-linux-gnu gcc-multilib g++-multilib \
    # llvm14-dev will install python 3.8 as bin/python3
    && DEBIAN_FRONTEND=noninteractive apt-get install -y llvm-18 clang-18 libclang-18-dev clang-18 \
        make cmake libsasl2-dev \
        libc6 libc6-dev libc6-arm64-cross libc6-dev-arm64-cross \
        gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*;

# CLang
RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-18 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100
RUN update-alternatives --install /usr/bin/clang-cpp clang-cpp /usr/bin/clang-cpp-18 100
RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-18 100
RUN update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-18 100
# Python
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1
RUN update-alternatives --install /usr/bin/python3 python /usr/bin/python3.11 1

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
    PYO3_CROSS_LIB_DIR=/usr/aarch64-linux-gnu/lib

ENV PYTHON_VERSION=3.12.0
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz -O - | tar -xz && \
    cd Python-${PYTHON_VERSION} && \
    touch config.site-aarch64 && \
    echo "ac_cv_buggy_getaddrinfo=no" >> config.site-aarch64 && \
    echo "ac_cv_file__dev_ptmx=no" >> config.site-aarch64 && \
    echo "ac_cv_file__dev_ptc=no" >> config.site-aarch64 && \
    CONFIG_SITE=config.site-aarch64 ./configure  \
      --enable-shared \
      --enable-optimizations \
      --disable-ipv6 \
      --prefix=/usr/aarch64-linux-gnu \
      --build=aarch64-unknown-linux-gnu \
      --host=x86_64-linux-gnu \
      --with-build-python=/usr/bin/python3.12 && \
    make -j $(nproc) && \
    make install && \
    cd .. && rm -rf Python-${PYTHON_VERSION};

ENV PYTHON_VERSION=3.11.3
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz -O - | tar -xz && \
    cd Python-${PYTHON_VERSION} && \
    touch config.site-aarch64 && \
    echo "ac_cv_buggy_getaddrinfo=no" >> config.site-aarch64 && \
    echo "ac_cv_file__dev_ptmx=no" >> config.site-aarch64 && \
    echo "ac_cv_file__dev_ptc=no" >> config.site-aarch64 && \
    CONFIG_SITE=config.site-aarch64 ./configure  \
      --enable-shared \
      --enable-optimizations \
      --disable-ipv6 \
      --prefix=/usr/aarch64-linux-gnu \
      --build=aarch64-unknown-linux-gnu \
      --host=x86_64-linux-gnu \
      --with-build-python=/usr/bin/python3.11 && \
    make -j $(nproc) && \
    make install && \
    cd .. && rm -rf Python-${PYTHON_VERSION};

ENV PYTHON_VERSION=3.10.11
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz -O - | tar -xz && \
    cd Python-${PYTHON_VERSION} && \
    touch config.site-aarch64 && \
    echo "ac_cv_buggy_getaddrinfo=no" >> config.site-aarch64 && \
    echo "ac_cv_file__dev_ptmx=no" >> config.site-aarch64 && \
    echo "ac_cv_file__dev_ptc=no" >> config.site-aarch64 && \
    CONFIG_SITE=config.site-aarch64 ./configure  \
      --enable-shared \
      --enable-optimizations \
      --disable-ipv6 \
      --prefix=/usr/aarch64-linux-gnu \
      --build=aarch64-unknown-linux-gnu \
      --host=x86_64-linux-gnu \
      --with-build-python=/usr/bin/python3.10 && \
    make -j $(nproc) && \
    make install && \
    cd .. && rm -rf Python-${PYTHON_VERSION};

ENV PYTHON_VERSION=3.9.18
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz -O - | tar -xz && \
    cd Python-${PYTHON_VERSION} && \
    touch config.site-aarch64 && \
    echo "ac_cv_buggy_getaddrinfo=no" >> config.site-aarch64 && \
    echo "ac_cv_file__dev_ptmx=no" >> config.site-aarch64 && \
    echo "ac_cv_file__dev_ptc=no" >> config.site-aarch64 && \
    CONFIG_SITE=config.site-aarch64 ./configure  \
      --enable-shared \
      --enable-optimizations \
      --disable-ipv6 \
      --prefix=/usr/aarch64-linux-gnu \
      --build=aarch64-unknown-linux-gnu \
      --host=x86_64-linux-gnu \
      --with-build-python=/usr/bin/python3.9 && \
    make -j $(nproc) && \
    make install && \
    cd .. && rm -rf Python-${PYTHON_VERSION};

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
