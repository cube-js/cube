<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Arrow Flight SQL Database Driver

This driver is based on top of Arrow Flight SQL JDBC driver.

[Learn more](https://arrow.apache.org/docs/format/FlightSql.html)

The current driver version is `18.3.0`. This value can be changed in the [package.json](./package.json) file (see the ARROW_FLIGHT_SQL_DRIVER_VERSION environment variable value in the `postinstall` script)


### Testing

To test follow the following steps

```
$ yarn
$ yarn test
```

Note: Unit tests require Java to be installed.

### License

Cube.js Arrow Flight SQL JDBC Driver is [Apache 2.0 licensed](./LICENSE).
