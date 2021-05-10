Cross Build Images
==================

> Docker images, which is used to build Cube Store via cross

Host only:

- x86_64-apple-darwin
- arm64-apple-darwin (need Big Sur, because osx.framework)

For all another, we are using Cross.

Keep in mind:

- Don't use modern unix*, which ship newest `libc` (why we are using x86_64-unknown-linux-gnu-stretch instead of x86_64-unknown-linux-gnu-buster)
- Better to use one clang/gcc version across images (`clang-9`)
- Try to use one OS for all images (`debian`) for unix*

```sh
export CROSS_VERSION=10052021

#docker build -t cubejs/rust-cross:x86_64-apple-darwin-$CROSS_VERSION -f x86_64-apple-darwin.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-pc-windows-msvc-$CROSS_VERSION -f x86_64-pc-windows-msvc.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-pc-windows-gnu-$CROSS_VERSION -f x86_64-pc-windows-gnu.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-unknown-linux-gnu-buster-$CROSS_VERSION -f x86_64-unknown-linux-gnu-buster.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-unknown-linux-gnu-stretch-$CROSS_VERSION -f x86_64-unknown-linux-gnu-stretch.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-unknown-linux-musl-$CROSS_VERSION -f x86_64-unknown-linux-musl.Dockerfile .
docker build -t cubejs/rust-cross:aarch64-unknown-linux-gnu-$CROSS_VERSION -f aarch64-unknown-linux-gnu.Dockerfile .

#docker push cubejs/rust-cross:x86_64-apple-darwin
docker push cubejs/rust-cross:x86_64-pc-windows-msvc-$CROSS_VERSION
docker push cubejs/rust-cross:x86_64-pc-windows-gnu-$CROSS_VERSION
docker push cubejs/rust-cross:x86_64-unknown-linux-gnu-buster-$CROSS_VERSION
docker push cubejs/rust-cross:x86_64-unknown-linux-gnu-stretch-$CROSS_VERSION
docker push cubejs/rust-cross:x86_64-unknown-linux-musl-$CROSS_VERSION
docker push cubejs/rust-cross:aarch64-unknown-linux-gnu-$CROSS_VERSION

# Verify versions
docker run --rm -it cubejs/rust-cross:x86_64-unknown-linux-gnu-stretch-$CROSS_VERSION cc --version
docker run --rm -it cubejs/rust-cross:x86_64-unknown-linux-gnu-buster-$CROSS_VERSION cc --version
```
