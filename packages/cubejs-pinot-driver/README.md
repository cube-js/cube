<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Pinot Database Driver

Cube.js Pinot driver.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

### Project Status

Project is WIP 

### Installation

`npm i @inthememory/pinot-driver`

### Usage
#### For Docker

Build development [docker image](https://github.com/cube-js/cube/blob/master/packages/cubejs-docker/DEVELOPMENT.md). 

Assuming the built image is tagged `cubejs/cube:dev`

```
FROM cubejs/cube:dev
RUN npm i @inthememory/pinot-driver
```

```
    environment:
      - CUBEJS_DB_TYPE=pinot
      - CUBEJS_DB_HOST= #broker_host
      - CUBEJS_DB_PORT= #broker_port
      - CUBEJS_DB_USER= #database user
      - CUBEJS_DB_PASS= #database password
      - CUBEJS_DEV_MODE=true #if running locally
```

### License

Cube.js Pinot Database Driver is [Apache 2.0 licensed](./LICENSE).