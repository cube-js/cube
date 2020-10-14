<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

# Cube.js Official Docker Image

Attention: We are working on Docker, and it's not ready for use. Please wait for the official announcement before using it.

## How to build

Release version

```sh
docker build -t cubejs/cube:latest -f latest.Dockerfile .
```

Not released, development (from `cubejs-docker` directory)

```sh
docker build -t cubejs/cube:dev -f dev.Dockerfile ../../
```

### License

Cube.js Docker is [Apache 2.0 licensed](./LICENSE).
