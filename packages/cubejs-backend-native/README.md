<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Native

Native module for Cube.js (binding to Rust codebase).

## Supported architectures and platforms

> If your platform/architecture is not supported, you can launch Cube
> using Docker.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

There are two different types of builds: with Python, and a fallback.
If Cube cannot detect a `libpython` library or your system is not supported, it will use a fallback build.

### With python

Supported python versions: `3.12`, `3.11`, `3.10`, `3.9`.

|          | `linux-gnu` | `linux-musl` | `darwin` | `win32` |
|----------|:-----------:|:------------:|:--------:|:-------:|
| `x86`    |     N/A     |     N/A      |   N/A    |   N/A   |
| `x86_64` |      ✅      |     N/A      |   N/A    |   N/A   |
| `arm64`  |      ✅      |     N/A      |   N/A    |   N/A   |

### Fallback (without python)

|          | `linux-gnu` | `linux-musl` | `darwin` | `win32` |
|----------|:-----------:|:------------:|:--------:|:-------:|
| `x86`    |     N/A     |     N/A      |   N/A    |   N/A   |
| `x86_64` |      ✅      |     N/A      |    ✅     |    ✅    |
| `arm64`  |      ✅      |     N/A      |    ✅     |         |

### License

Cube.js Native is [Apache 2.0 licensed](./LICENSE).

