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
- Install 3 last versions for Python (`3.9`, `3.10`, `3.11`)

```sh
# dmY
export $(cat .env | xargs)

# docker build -t cubejs/rust-cross:x86_64-apple-darwin-$CROSS_VERSION -f x86_64-apple-darwin.Dockerfile .
# docker buildx build --platform linux/amd64 -t cubejs/rust-cross:x86_64-pc-windows-gnu-$CROSS_VERSION -f x86_64-pc-windows-gnu.Dockerfile .
# docker buildx build --platform linux/amd64 -t cubejs/rust-cross:x86_64-pc-windows-msvc-$CROSS_VERSION -f x86_64-pc-windows-msvc.Dockerfile .

docker buildx build --platform linux/amd64 -t cubejs/rust-cross:x86_64-unknown-linux-gnu-$CROSS_VERSION -f x86_64-unknown-linux-gnu.Dockerfile .
docker buildx build --platform linux/amd64 -t cubejs/rust-cross:x86_64-unknown-linux-musl-$CROSS_VERSION -f x86_64-unknown-linux-musl.Dockerfile .
docker buildx build --platform linux/amd64 -t cubejs/rust-cross:aarch64-unknown-linux-gnu-$CROSS_VERSION -f aarch64-unknown-linux-gnu.Dockerfile .
r u
#docker push cubejs/rust-cross:x86_64-apple-darwin
#docker push cubejs/rust-cross:x86_64-pc-windows-gnu-$CROSS_VERSION
#docker push cubejs/rust-cross:x86_64-pc-windows-msvc-$CROSS_VERSION
docker push cubejs/rust-cross:x86_64-unknown-linux-gnu-$CROSS_VERSION
docker push cubejs/rust-cross:x86_64-unknown-linux-musl-$CROSS_VERSION
docker push cubejs/rust-cross:aarch64-unknown-linux-gnu-$CROSS_VERSION

# Verify versions
docker run --platform linux/amd64 --rm -it cubejs/rust-cross:x86_64-unknown-linux-gnu-$CROSS_VERSION cc --version
docker run --platform linux/amd64 --rm -it cubejs/rust-cross:x86_64-unknown-linux-gnu-buster-$CROSS_VERSION cc --version
docker run --platform linux/amd64 --rm -it cubejs/rust-cross:aarch64-unknown-linux-gnu-$CROSS_VERSION cc --version
```
