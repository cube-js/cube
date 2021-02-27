FROM joseluisq/rust-linux-darwin-builder:1.50.0

ENV OPENSSL_STATIC=true

ENV CC_x86_64_apple_darwin=o64-clang
ENV CXX_x86_64_apple_darwin=o64-clang++
