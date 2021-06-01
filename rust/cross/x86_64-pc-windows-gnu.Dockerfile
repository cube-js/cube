FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install --assume-yes -y curl pkg-config wget llvm libclang-dev gcc-mingw-w64-x86-64 g++-mingw-w64-x86-64 binutils-mingw-w64-x86-64 binutils make git automake autoconf ca-certificates gcc g++ mingw-w64-x86-64-dev

# https://www.openssl.org/source/old/1.1.1/
ARG OPENSSL_VERSION=1.1.1h

RUN wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz -O - | tar -xz
WORKDIR /openssl-${OPENSSL_VERSION}
RUN ./Configure --prefix=/openssl --openssldir=/openssl/lib --cross-compile-prefix=x86_64-w64-mingw32- mingw64 && \
    make && \
    make install_sw && \
    make install_ssldirs && \
    cd .. && rm -rf /openssl-${OPENSSL_VERSION}

ENV OPENSSL_DIR=/openssl \
    OPENSSL_STATIC=yes \
    OPENSSL_INCLUDE_DIR=/openssl/include \
    OPENSSL_LIB_DIR=/openssl/lib

ENV CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-g++ \
    CARGO_TARGET_X86_64_PC_WINDOWS_GNU_RUNNER=wine \
    CC_x86_64_pc_windows_gnu=x86_64-w64-mingw32-gcc-posix \
    CXX_x86_64_pc_windows_gnu=x86_64-w64-mingw32-g++-posix

RUN update-alternatives --auto x86_64-w64-mingw32-g++
RUN update-alternatives --set x86_64-w64-mingw32-g++ /usr/bin/x86_64-w64-mingw32-g++-posix
