<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://docs.cube.dev) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Vertica Database Driver

Cube.js Vertica driver that uses [vertica-nodejs](https://github.com/vertica/vertica-nodejs) package.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

Note: This driver isn't supported by front-end so we can not use connection wizard to config vertica data source. Please
use env instead.

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

> **`CUBEJS_DEV_MODE=true` is an authentication bypass.** Playground and its
> supporting endpoints are served with no authentication, so anyone who can reach the
> instance can mint an API token carrying any security context (signed with your API
> secret), read and overwrite your data model and `.env`, and — unless
> `CUBEJS_SQL_PASSWORD` is set — run arbitrary SQL through the SQL API. This is intentional — development mode is designed to run on a developer's
> local machine for ease of use and debugging. Never use it in production, and using it
> in the Cube cloud platform is highly discouraged, as it bypasses the platform's
> security model. See
> [`CUBEJS_DEV_MODE`](https://docs.cube.dev/reference/configuration/environment-variables#cubejs_dev_mode).

### License

Cube.js Vertica Database Driver is [Apache 2.0 licensed](./LICENSE).
