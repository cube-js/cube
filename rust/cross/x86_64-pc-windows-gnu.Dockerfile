FROM rustembedded/cross:x86_64-pc-windows-gnu

RUN apt-get update && \
    apt-get install -y curl pkg-config wget llvm

RUN wget https://www.openssl.org/source/openssl-1.1.1i.tar.gz -O - | tar -xz
WORKDIR /openssl-1.1.1i
RUN ./config --prefix=/openssl --openssldir=/openssl/lib && make && make install

ENV OPENSSL_DIR=/openssl \
    OPENSSL_INCLUDE_DIR=/openssl/include \
    OPENSSL_LIB_DIR=/openssl/lib
