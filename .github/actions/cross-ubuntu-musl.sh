#!/bin/bash

echo 'Install script for Ubuntu'

sudo apt-get -y update
sudo apt-get -y install musl-tools llvm

wget https://www.openssl.org/source/openssl-1.1.1i.tar.gz -O - | tar -xz &&\
  cd openssl-1.1.1i && \
  CC="musl-gcc -fPIE -pie" ./Configure no-shared no-async no-engine -DOPENSSL_NO_SECURE_MEMORY --prefix=/musl --openssldir=/musl/ssl linux-x86_64 && \
  make depend && \
  make -j $(nproc) && \
  sudo make install && \
  cd .. && rm -rf openssl-1.1.1i
