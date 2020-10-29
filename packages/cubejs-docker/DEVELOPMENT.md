# Development guide

## How to build

Release version

```sh
docker build -t cubejs/cube:latest -f latest.Dockerfile .
```

Not released, development (from `cubejs-docker` directory)

```sh
docker build -t cubejs/cube:dev -f dev.Dockerfile ../../
```
