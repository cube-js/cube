FROM rustembedded/cross:x86_64-unknown-linux-gnu

RUN yum -y upgrade
RUN yum -y install wget llvm-devel libclang centos-release-scl
RUN yum -y install --setopt=tsflags=nodocs devtoolset-9-gcc*

RUN mkdir /musl

RUN wget https://www.openssl.org/source/openssl-1.1.1i.tar.gz -O - | tar -xz &&\
    cd openssl-1.1.1i && \
    ./Configure no-shared no-async --prefix=/musl --openssldir=/musl/ssl linux-x86_64 && \
    make depend && \
    make -j $(nproc) && \
    make install && \
    cd .. && rm -rf openssl-1.1.1i

ENV GLIB_C 2.18

# /lib64/libc.so.6: version `GLIBC_2.18' not found
RUN wget http://ftp.gnu.org/gnu/glibc/glibc-${GLIB_C}.tar.gz -O - | tar -xz && \
    cd glibc-${GLIB_C} && \
    # configure: error: you must configure in a separate build directory
    mkdir build && cd build && \
    ./../configure --prefix='/opt/glibc-$GLIB_C' && \
    make -j $(nproc) && \
    make install && \
    cd .. && cd .. && rm -rf glibc-${GLIB_C}

ENV PKG_CONFIG_ALLOW_CROSS=1
ENV OPENSSL_STATIC=true
ENV OPENSSL_DIR=/musl

ENV PATH="/cargo/bin:$PATH"

# https://austindewey.com/2019/03/26/enabling-software-collections-binaries-on-a-docker-image/
COPY x86_64-unknown-linux-gnu-entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh
ENTRYPOINT [ "/usr/bin/entrypoint.sh" ]
