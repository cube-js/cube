<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) •
[Examples](#examples) • [Blog](https://cube.dev/blog) •
[Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

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
| `arm64`  |     ✅      |              |  ✅[1]   |         |

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
      - 4000:4000  # Cube.js API and Developer Playground
      - 3000:3000  # Dashboard app, if created
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

### Configuration

Cubestore can be configured via next (hopefuly self-explanatory) environment variables:

* `CUBESTORE_BIND_ADDR` - bind address, default `0.0.0.0:3306`
* `CUBESTORE_PORT` - port, default `3306`
* `CUBESTORE_STATUS_BIND_ADDR` - status probe bind address, default `0.0.0.0:3031`
* `CUBESTORE_HTTP_BIND_ADDR` - HTTP API bind address, default `0.0.0.0:3030`
* `CUBESTORE_STALE_STREAM_TIMEOUT` - stale stream timeout, default `600s`
* `CUBESTORE_WORKERS` - number of workers
* `CUBESTORE_WORKER_BIND_ADDR` - worker bind address, default `0.0.0.0:<CUBESTORE_WORKER_PORT>`
* `CUBESTORE_WORKER_PORT` - worker port
* `CUBESTORE_META_BIND_ADDR` - metastore bind address, default `0.0.0.0:<CUBESTORE_META_PORT>
* `CUBESTORE_META_ADDR` - remote metastore address
* `CUBESTORE_META_PORT` - remote metastore port
* `CUBESTORE_SERVER_NAME` - server name, default `localhost`
* `CUBESTORE_LONG_TERM_JOB_RUNNERS` - count of job runners, default `32`
* `CUBESTORE_NO_UPLOAD` - disable uploading, default `false`
* `CUBESTORE_ENABLE_TOPK` - enable TOP K, default `true`
* `CUBESTORE_METRICS_FORMAT` - metrics format, can be `statsd` or `dogstatsd`
* `CUBESTORE_METRICS_ADDRESS` - metrics address, default `127.0.0.1`
* `CUBESTORE_METRICS_PORT` - metrics port, default `8125`
* `CUBESTORE_TELEMETRY` - enable telemetry, default `true`
* `CUBESTORE_LOG_LEVEL` - log level, can be `error`, `warn`, `info`, `debug`, `trace`, default `info`
* `CUBESTORE_EVENT_LOOP_WORKER_THREADS` - number of event loop worker threads
* `CUBESTORE_SELECT_WORKER_TITLE` - worker process title, default `--sel-worker`
* `CUBESTORE_LOG_CONTEXT` - context string to add to all logs, default `<empty>`
* `CUBESTORE_DATA_DIR` - data directory, default `<current_dir>/.cubestore`
* `CUBESTORE_DUMP_DIR` - dump directory
* `CUBESTORE_PARTITION_SPLIT_THRESHOLD` - partition split threshold, default `2 097 152`
* `CUBESTORE_AGENT_ENDPOINT_URL` - agent endpoint url

* Storage configuration:
  * `CUBESTORE_REMOTE_DIR` - remote directory local path
  * `CUBESTORE_S3_BUCKET` - AWS S3 bucket name
  * `CUBESTORE_S3_REGION` - AWS S3 region
  * `CUBESTORE_S3_SUB_PATH` - AWS S3 sub path
  * `CUBESTORE_AWS_ACCESS_KEY_ID` - AWS access key id
  * `CUBESTORE_AWS_SECRET_ACCESS_KEY` - AWS secret access key
  * `CUBESTORE_AWS_CREDS_REFRESH_EVERY_MINS` - AWS credentials refresh interval in minutes
  * `CUBESTORE_MINIO_BUCKET` - MinIO bucket name
  * `CUBESTORE_MINIO_SUB_PATH` - MinIO sub path
  * `CUBESTORE_MINIO_ACCESS_KEY_ID` - MinIO access key id
  * `CUBESTORE_MINIO_SECRET_ACCESS_KEY` - MinIO secret access key
  * `CUBESTORE_MINIO_SERVER_ENDPOINT` - MinIO server endpoint
  * `CUBESTORE_MINIO_REGION` - MinIO region
  * `CUBESTORE_MINIO_CREDS_REFRESH_EVERY_MINS` - MinIO credentials refresh interval in minutes
  * `CUBESTORE_GCS_BUCKET` - GCS bucket name
  * `CUBESTORE_GCS_SUB_PATH` - GCS sub path
  * `CUBESTORE_GCP_CREDENTIALS` - GCP credentials
  * `CUBESTORE_GCP_KEY_FILE` - GCP key file path


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
