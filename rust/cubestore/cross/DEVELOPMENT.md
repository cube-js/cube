### Tagging

```sh
docker pull ghcr.io/cross-rs/aarch64-unknown-linux-gnu:main
docker image tag ghcr.io/cross-rs/aarch64-unknown-linux-gnu:main cubejs/cross-aarch64-unknown-linux-gnu:03042024
docker push cubejs/cross-aarch64-unknown-linux-gnu:03042024

# https://hub.docker.com/r/cubejs/cross-aarch64-unknown-linux-gnu/tags
```

```sh
# https://github.com/cross-rs/cross/pkgs/container/x86_64-unknown-linux-musl
docker pull ghcr.io/cross-rs/x86_64-unknown-linux-musl:main
docker image tag ghcr.io/cross-rs/x86_64-unknown-linux-musl:main cubejs/cross-x86_64-unknown-linux-musl:03042024
docker push cubejs/cross-x86_64-unknown-linux-musl:03042024

# https://hub.docker.com/r/cubejs/cross-x86_64-unknown-linux-musl/tags
```