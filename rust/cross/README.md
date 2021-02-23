Cross Build Images
==================

```sh
docker build -t cubejs/rust-cross:x86_64-apple-darwin -f x86_64-apple-darwin.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-pc-windows-msvc -f x86_64-pc-windows-msvc.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-pc-windows-gnu -f x86_64-pc-windows-gnu.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-unknown-linux-gnu -f x86_64-unknown-linux-gnu.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-unknown-linux-musl -f x86_64-unknown-linux-musl.Dockerfile .
```

```sh
docker push cubejs/rust-cross:x86_64-apple-darwin
docker push cubejs/rust-cross:x86_64-pc-windows-msvc
docker push cubejs/rust-cross:x86_64-pc-windows-gnu
docker push cubejs/rust-cross:x86_64-unknown-linux-gnu
docker push cubejs/rust-cross:x86_64-unknown-linux-musl
```
