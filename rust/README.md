<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Examples](#examples) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Rust/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ARust+branch%3Amaster)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_shield)


Cube Store
==========

Cube.js pre-aggregation storage layer.

## Build

```
docker build -t cubejs/cubestore:latest .
```

```
docker run --rm cubejs/cubestore:latest
```

## Development

Check out https://github.com/cube-js/arrow/tree/cubestore-2020-11-06 and put **.cargo/config.toml** in the current directory with following contents:

```
paths = ["../../arrow/rust"]
```

It should point to checked out Apache Arrow fork and it'll allow you to build project against locally modified sources.

## License

Cube Store is [Apache 2.0 licensed](./cubestore/LICENSE).
