# syntax=docker/dockerfile:1

# Cross compiling python needs a build interpreter of the exact same version :D
FROM ubuntu:20.04 AS build-python

ARG PYTHON_VERSION
ARG PYTHON_VERSION_SUFFIX

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y build-essential wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*;

RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}${PYTHON_VERSION_SUFFIX}.tgz -O - | tar -xz \
    && cd Python-${PYTHON_VERSION}${PYTHON_VERSION_SUFFIX} \
    && ./configure --prefix=/opt/build-python --disable-test-modules --without-ensurepip \
    && make -j $(nproc) \
    && make install \
    && cd .. && rm -rf Python-${PYTHON_VERSION}${PYTHON_VERSION_SUFFIX};

FROM base

ARG PYTHON_VERSION
ARG PYTHON_VERSION_SUFFIX
ARG PYTHON_RELEASE

# --enable-optimizations is disabled, because it's not supported with CROSS
# 3.9/3.10 ignore --with-build-python and instead search PATH for python$VERSION, so the build
# interpreter has to be reachable both ways
RUN --mount=type=bind,from=build-python,source=/opt/build-python,target=/opt/build-python \
    export PATH="/opt/build-python/bin:${PATH}" \
    && wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}${PYTHON_VERSION_SUFFIX}.tgz -O - | tar -xz \
    && cd Python-${PYTHON_VERSION}${PYTHON_VERSION_SUFFIX} \
    && touch config.site-aarch64 \
    && echo "ac_cv_buggy_getaddrinfo=no" >> config.site-aarch64 \
    && echo "ac_cv_file__dev_ptmx=no" >> config.site-aarch64 \
    && echo "ac_cv_file__dev_ptc=no" >> config.site-aarch64 \
    && CONFIG_SITE=config.site-aarch64 ./configure  \
      --enable-shared \
      --disable-ipv6 \
      --prefix=/usr/aarch64-linux-gnu \
      --without-ensurepip \
      --build=aarch64-unknown-linux-gnu \
      --host=x86_64-linux-gnu \
      --with-build-python=/opt/build-python/bin/python${PYTHON_RELEASE} \
    && make -j $(nproc) \
    && make install \
    && cd .. && rm -rf Python-${PYTHON_VERSION}${PYTHON_VERSION_SUFFIX};

ENV PYO3_CROSS_PYTHON_VERSION=${PYTHON_RELEASE} \
    PYO3_CROSS_INCLUDE_DIR=/usr/aarch64-linux-gnu/include \
    PYO3_CROSS_LIB_DIR=/usr/aarch64-linux-gnu/lib
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
