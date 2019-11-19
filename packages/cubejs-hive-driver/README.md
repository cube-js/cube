<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![CircleCI](https://circleci.com/gh/cube-js/cube.js.svg?style=shield)](https://circleci.com/gh/cube-js/cube.js)

# Cube.js Hive Database Driver

Pure Javascript Thrift HiveServer 2 driver.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

## Contributing Missing Hive Protocol

1. Download Hive Thrift definition for your version from https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift.
2. Install Apache Thrift on your machine.
3. Run `$ thrift --gen js:node -o HIVE_<VERSION> TCLIService.thrift`.
4. Copy generated files to the idl directory of this repository.

### License

Cube.js Hive Database Driver is [Apache 2.0 licensed](./LICENSE).