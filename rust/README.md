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
