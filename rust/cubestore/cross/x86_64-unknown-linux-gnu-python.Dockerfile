FROM base

ARG PYTHON_VERSION
ARG PYTHON_RELEASE

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
    && ln -f -s /usr/bin/python${PYTHON_RELEASE} /usr/bin/python3 \
    && cd .. && rm -rf Python-${PYTHON_VERSION};

# pyo3 uses python3 to detect version, but there is a bug and it uses python3.9 (system), this force it to use a new python
ENV PYO3_PYTHON=python${PYTHON_RELEASE}

ENV PATH="/cargo/bin:$PATH"
