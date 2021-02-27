FROM joseluisq/rust-linux-darwin-builder:1.50.0

ENV OPENSSL_STATIC=true

ENV CC_x86_64_apple_darwin=x86_64-w64-mingw32-gcc-posix
ENV CXX_x86_64_apple_darwin=x86_64-w64-mingw32-g++-posix
