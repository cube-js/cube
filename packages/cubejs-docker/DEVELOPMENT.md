# Development guide

## How to build

Release version

Buster:

```sh
docker build -t cubejs/cube:latest -f latest.Dockerfile .
```

Alpine

```sh
docker build -t cubejs/cube:alpine -f latest-alpine.Dockerfile .
```

Not released, development (from `cubejs-docker` directory)

```sh
docker build -t cubejs/cube:dev -f dev.Dockerfile ../../
```

```sh
docker build -t cubejs/cube:dev-alpine -f dev-alpine.Dockerfile ../../
```
