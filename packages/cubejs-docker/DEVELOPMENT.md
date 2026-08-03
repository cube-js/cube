# Development guide

## How to build

Release version

### Debian:

```sh
docker build -t cubejs/cube:latest -f latest.Dockerfile .
docker buildx build --platform linux/amd64 -t cubejs/cube:latest -f latest.Dockerfile .
docker buildx build --platform linux/amd64,linux/arm64 -t cubejs/cube:latest -f latest.Dockerfile .
```

### JDK

```sh
docker build -t cubejs/cube:latest-jdk -f latest-debian-jdk.Dockerfile .
```

### Not released, development (from `cubejs-docker` directory)

```sh
docker build -t cubejs/cube:dev -f dev.Dockerfile ../../
docker buildx build --platform linux/amd64 -t cubejs/cube:dev -f dev.Dockerfile ../../
```
