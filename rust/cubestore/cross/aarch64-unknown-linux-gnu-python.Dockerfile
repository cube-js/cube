FROM base

ARG PYTHON_VERSION
ARG PYTHON_RELEASE

# python  is required for cross compiling python :D
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python${PYTHON_RELEASE} \
    && rm -rf /var/lib/apt/lists/*;

RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz -O - | tar -xz \
    && cd Python-${PYTHON_VERSION} \
    && touch config.site-aarch64 \
    && echo "ac_cv_buggy_getaddrinfo=no" >> config.site-aarch64 \
    && echo "ac_cv_file__dev_ptmx=no" >> config.site-aarch64 \
    && echo "ac_cv_file__dev_ptc=no" >> config.site-aarch64 \
    && CONFIG_SITE=config.site-aarch64 ./configure  \
      --enable-shared \
      --enable-optimizations \
      --disable-ipv6 \
      --prefix=/usr/aarch64-linux-gnu \
      --build=aarch64-unknown-linux-gnu \
      --host=x86_64-linux-gnu \
      --with-build-python=/usr/bin/python${PYTHON_RELEASE} \
    && make -j $(nproc) \
    && make install \
    && cd .. && rm -rf Python-${PYTHON_VERSION} \
    && rm -rf /var/lib/apt/lists/*; \
