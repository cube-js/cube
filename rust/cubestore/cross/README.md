Cross Build Images
==================

> Docker images, which is used to build Cube Store via cross

Host only:

- x86_64-apple-darwin
- arm64-apple-darwin (need Big Sur, because osx.framework)

For all another, we are using Cross.

Keep in mind:

- Don't use modern unix*, which ship newest `libc` (current used 2.31)
- Better to use one clang/gcc version across images (`clang-14`)
- Try to use one OS for all images (`debian`) for unix*

```sh
export DOCKER_BUILDKIT_MAX_PARALLELISM=2
docker buildx bake x86_64-unknown-linux-gnu-python --push
docker buildx bake aarch64-unknown-linux-gnu-python --push
docker buildx bake x86_64-unknown-linux-musl-python --push

# dmY
export CROSS_VERSION=31072025

# Verify versions
docker run --platform linux/amd64 --rm -it cubejs/rust-cross:x86_64-unknown-linux-gnu-$CROSS_VERSION cc --version
docker run --platform linux/amd64 --rm -it cubejs/rust-cross:aarch64-unknown-linux-gnu-$CROSS_VERSION cc --version
```
