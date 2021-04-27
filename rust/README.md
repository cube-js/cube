<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) •
[Examples](#examples) • [Blog](https://cube.dev/blog) •
[Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Rust/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ARust+branch%3Amaster)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_shield)

# Cube Store

Cube.js pre-aggregation storage layer.

## Motivation

Over the past year, we've accumulated feedback around various use-cases with
pre-aggregations and how to store them. We've learned that there are a set of
problems where relational databases as a storage layer has significant
performance and functionality issues.

These problems include:

- Performance issues with high cardinality rollups (1B and more)
- Lack of HyperLogLog support
- Degraded performance for big `UNION ALL` queries
- Poor `JOIN` performance across rolled up tables
- Table/schema name length issues across different database types
- SQL type differences between source and external database

Over time, we realized that if we try to fix these issues with existing database
engines, we'd end up modifying these databases' codebases in one way or another.

We decided to take another approach and write our own materialized OLAP cache
store, designed solely to store and serve rollup tables at scale.

## Approach

To optimize performance as much as possible, we went with a native approach and
are using Rust to develop Cube Store, utilizing a set of technologies like
RocksDB, Apache Parquet, and Arrow that have proven effectiveness in solving
data access problems.

Cube Store is fully open-sourced and released under the Apache 2.0 license.

## Plans

We intend to start distributing Cube Store with Cube.js, and eventually make
Cube Store the default pre-aggregation storage layer for Cube.js. Support for
MySQL and Postgres as external databases will continue, but at a lower priority.

We'll also update all documentation regarding pre-aggregations and include usage
and deployment instructions for Cube Store.

## Supported architectures and platforms

> If your platform/architecture is not supported, you can launch Cube Store
> using Docker.

|          | `linux-gnu` | `linux-musl` | `darwin` | `win32` |
| -------- | :---------: | :----------: | :------: | :-----: |
| `x86`    |     N/A     |     N/A      |   N/A    |   N/A   |
| `x86_64` |     ✅      |      ✅      |    ✅    |   ✅    |
| `arm64`  |             |              |  ✅[1]   |         |

[1] It can be launched using Rosetta 2 via the `x86_64-apple` binary.

## Usage

### With Cube.js

Starting with `v0.26.48`, Cube.js ships with Cube Store enabled when `CUBEJS_DEV_MODE=true`.
You don't need to set up any `CUBEJS_EXT_DB_*` environment variables or
`externalDriverFactory` inside your `cube.js` configuration file.

For versions prior to `v0.26.48`, you should upgrade your project to the latest
version and install the Cube Store driver:

```bash
yarn add @cubejs-backend/cubestore-driver
```

After starting up, Cube.js will print a message:

`🔥 Cube Store (0.26.64) is assigned to 3030 port.`

### With Docker

Start Cube Store in a Docker container and bind port `3030` to `127.0.0.1`:

```bash
docker run -d -p 3030:3030 cubejs/cubestore:edge
```

Configure Cube.js to use the above connection for an external database via the
`.env` file:

```dotenv
CUBEJS_EXT_DB_TYPE=cubestore
CUBEJS_EXT_DB_HOST=127.0.0.1
```

### With Docker Compose

Create a `docker-compose.yml` file with the following content:

```yml
version: '2.2'
services:
  cubestore:
    image: cubejs/cubestore:edge

  cube:
    image: cubejs/cube:latest
    ports:
      # 4000 is a port for Cube.js API
      - 4000:4000
      # 3000 is a port for Playground web server
      # it is available only in dev mode
      - 3000:3000
    env_file: .env
    depends_on:
      - cubestore
    links:
      - cubestore
    volumes:
      - ./schema:/cube/conf/schema
```

Configure Cube.js to use the above connection for an external database via the
`.env` file:

```dotenv
CUBEJS_EXT_DB_TYPE=cubestore
CUBEJS_EXT_DB_HOST=cubestore
```

## Build

```bash
docker build -t cubejs/cubestore:latest .
docker run --rm cubejs/cubestore:latest
```

## Development

Check out https://github.com/cube-js/arrow/tree/cubestore-2020-11-06 and put
**.cargo/config.toml** in the current directory with following contents:

```toml
paths = ["../../arrow/rust"]
```

It should point to checked out Apache Arrow fork and it'll allow you to build
project against locally modified sources.

## License

Cube Store is [Apache 2.0 licensed](./cubestore/LICENSE).
