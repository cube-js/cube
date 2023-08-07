# Development guide

## How to build

Release version

### Debian:

```sh
docker build -t heora/cubejs-cube:latest -f latest.Dockerfile .
docker buildx build --platform linux/amd64 -t heora/cubejs-cube:latest -f latest.Dockerfile .
docker buildx build --platform linux/amd64,linux/arm64 -t heora/cubejs-cube:latest -f latest.Dockerfile .
```

### Alpine

```sh
docker build -t heora/cubejs-cube:alpine -f latest-alpine.Dockerfile .
docker buildx build --platform linux/amd64 -t heora/cubejs-cube:alpine -f latest-alpine.Dockerfile .
```

### JDK

```sh
docker build -t heora/cubejs-cube:alpine-jdk -f latest-alpine-jdk.Dockerfile .
docker build -t heora/cubejs-cube:latest-jdk -f latest-debian-jdk.Dockerfile .
```

### Not released, development (from `cubejs-docker` directory)

```sh
docker build -t heora/cubejs-cube:dev -f dev.Dockerfile ../../
docker buildx build --platform linux/amd64 -t heora/cubejs-cube:dev -f dev.Dockerfile ../../
```

```sh
docker build -t heora/cubejs-cube:dev-alpine -f dev-alpine.Dockerfile ../../
docker buildx build --platform linux/amd64 -t heora/cubejs-cube:dev-alpine -f dev-alpine.Dockerfile ../../
```
