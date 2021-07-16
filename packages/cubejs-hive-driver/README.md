<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Hive Database Driver

Pure Javascript Thrift HiveServer 2 driver.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

## Contributing Missing Hive Protocol

### Local Installation

1. Download Hive Thrift definition for your version from https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift.
2. Install Apache Thrift on your machine.
3. Run `$ thrift --gen js:node c TCLIService.thrift`.
4. Copy generated files to the idl directory of this repository.

### Using Docker

1. Have docker installed and running
2. Run `docker run -v "$PWD:/data" thrift thrift -o /data --gen js:node /data/TCLIService.thrift`
3. Copy generated files to the idl directory of this repository.

### License

Cube.js Hive Database Driver is [Apache 2.0 licensed](./LICENSE).
