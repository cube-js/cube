<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Vertica Database Driver

Cube.js Vertica driver that uses [vertica-nodejs](https://github.com/vertica/vertica-nodejs) package.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

### Project Status

Project is WIP pending approval of [PR 7298](https://github.com/cube-js/cube/pull/7289). 

A recent build of cubejs with the vertica driver preinstalled is available as a public docker image while the project is WIP

`docker pull timbrownls26/cubejs-vertica:0.0.1`

### Installation

`npm i @knowitall/vertica-driver`

### Usage
#### For Docker

Build development [docker image](https://github.com/cube-js/cube/blob/master/packages/cubejs-docker/DEVELOPMENT.md) from variant of cubejs in this [PR 7298](https://github.com/cube-js/cube/pull/7289). 

Assuming the built image is tagged `cubejs/cube:dev`

```
FROM cubejs/cube:dev

RUN npm i @knowitall/vertica-driver
```

Note: This driver isn't supported by front-end so we can not use connection wizard to config vertica data source. Please use env instead.

```
    environment:
      - CUBEJS_DB_TYPE=vertica
      - CUBEJS_DB_HOST= #host
      - CUBEJS_DB_NAME= #database name 
      - CUBEJS_DB_PORT=5433
      - CUBEJS_DB_USER= #database user
      - CUBEJS_DB_PASS= #database password
      - CUBEJS_DEV_MODE=true #if running locally
```
if `CUBEJS_DB_TYPE=vertica` then the vertica driver is loaded automatically.

### License

Cube.js Vertica Database Driver is [Apache 2.0 licensed](./LICENSE).
