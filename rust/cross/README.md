Cross Build Images
==================

```
docker build -t cubejs/rust-cross:x86_64-apple-darwin -f x86_64-apple-darwin.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-pc-windows-gnu -f x86_64-pc-windows-gnu.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-unknown-linux-gnu -f x86_64-unknown-linux-gnu.Dockerfile .
docker build -t cubejs/rust-cross:x86_64-unknown-linux-musl -f x86_64-unknown-linux-musl.Dockerfile .
```
