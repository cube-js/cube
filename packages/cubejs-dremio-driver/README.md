<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Dremio Driver

Pure Javascript Dremio driver.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

## Usage

* .env

```code
CUBEJS_DB_HOST=dremio-host
CUBEJS_DB_PORT=dremio-port
CUBEJS_DB_NAME=dremio-space-name
CUBEJS_DB_USER=dremio-user
CUBEJS_DB_PASS=dremio-password
CUBEJS_DB_TYPE=dremio
CUBEJS_DB_SSL=true|false
```

* cube.js

```code
const DremioDriver = require("@cubejs-backend/dremio-driver")

module.exports = {
  driverFactory: ({ dataSource }) =>
    new DremioDriver({ database: dataSource }),
};
```

### License

Cube.js Dremio Driver is [Apache 2.0 licensed](./LICENSE).
