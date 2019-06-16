# Cube.js Hive Database Driver

Pure Javascript Thrift HiveServer 2 driver.

[Learn more](https://github.com/statsbotco/cube.js#getting-started)

## Contributing Missing Hive Protocol

1. Download Hive Thrift definition for your version from https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift.
2. Install Apache Thrift on your machine.
3. Run `$ thrift --gen js:node -o HIVE_<VERSION> TCLIService.thrift`.
4. Copy generated files to the idl directory of this repository.

### License

Cube.js Hive Database Driver is [Apache 2.0 licensed](./LICENSE).