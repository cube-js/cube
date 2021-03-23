---
title: Using Pre-Aggregations
permalink: /caching/using-pre-aggregations
category: Caching
menuOrder: 2
---

Pre-aggregations is a powerful way to speed up your Cube.js queries. There are many
configuration options to consider. Please make sure to also check [this
Pre-Aggregations page in the data schema section](/pre-aggregations).

## Refresh Strategy

Refresh strategy can be customized by setting the
[refreshKey](/pre-aggregations#refresh-key) property for the pre-aggregation.

The default value of `refreshKey` is `every: '1 hour'`. It can be redefined
either by providing SQL:

```javascript
cube(`Orders`, {
  // ...

  preAggregations: {
    amountByCreated: {
      type: `rollup`,
      measureReferences: [amount],
      timeDimensionReference: createdAt,
      granularity: `month`,
      refreshKey: {
        sql: `SELECT MAX(created_at) FROM orders`,
      },
    },
  },
});
```

Or by providing a refresh time interval:

```javascript
cube(`Orders`, {
  // ...

  preAggregations: {
    amountByCreated: {
      type: `rollup`,
      measureReferences: [amount],
      timeDimensionReference: createdAt,
      granularity: `month`,
      refreshKey: {
        every: `12 hour`,
      },
    },
  },
});
```

## Background Refresh

You can refresh pre-aggregations in the background by setting
`scheduledRefresh: true`.

In development mode, Cube.js enables background refresh by default and will
refresh all pre-aggregations marked with the
[`scheduledRefresh`](/pre-aggregations#scheduled-refresh) parameter.

Please consult the [Production Checklist][link-production-checklist-refresh] for
best practices on running background refresh in production environments.

```js
cube(`Orders`, {
  // ...

  preAggregations: {
    amountByCreated: {
      type: `rollup`,
      measureReferences: [amount],
      timeDimensionReference: createdAt,
      granularity: `month`,
      scheduledRefresh: true,
    },
  },
});
```

## Read Only Data Source

In some cases, it may not be possible to stage pre-aggregation query results in
materialized tables in the source database. For example, the database driver may
not support it, or the source database may be read-only.

To fallback to a strategy where the pre-aggregation query results are downloaded
without first being materialized, set the `readOnly` property of
[`driverFactory`][ref-config-driverfactory] in your configuration:

```javascript
const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  driverFactory: () =>
    new PostgresDriver({
      readOnly: true,
    })
};
```

<!-- prettier-ignore-start -->
[[warning |]]
| Read only pre-aggregations are only suitable for small datasets
| since they require loading all the data into Cube.js process memory. We **do not**
| recommend using `readOnly` mode for production workloads.
<!-- prettier-ignore-end -->

By default, Cube.js uses temporary tables to extract data types from executed
query while `readOnly` is `false`. If the driver is used in `readOnly` mode, it
will use heuristics to extract data types from the database's response, but this
strategy has certain limitations:

- The aggregation results can be empty, and Cube.js will throw an exception
  because it is impossible to detect types
- Data types can be incorrectly inferred, in rare cases

We highly recommend leaving `readOnly` unset or explicitly setting it to
`false`.

## Pre-Aggregations Storage

When using **external** pre-aggregations, Cube.js will
store pre-aggregations inside its own purpose-built storage layer: Cube Store.

Alternatively, you can store external pre-aggregations in a different database, such MySQL or Postgres.
In order to make this work, you should set the
[`externalDriverFactory`][ref-config-extdriverfactory] and
[`externalDbType`][ref-config-extdbtype] properties in your `cube.js`
configuration file. These properties can also be set through the environment
variables.

[ref-config-extdbtype]: /config#options-reference-external-db-type
[ref-config-extdriverfactory]: /config#options-reference-external-driver-factory
[link-production-checklist-refresh]: /deployment/production-checklist#set-up-refresh-worker

```bash
CUBEJS_EXT_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_EXT_DB_PORT=<YOUR_DB_PORT_HERE>
CUBEJS_EXT_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_EXT_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_EXT_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_EXT_DB_TYPE=<SUPPORTED_DB_TYPE_HERE>
```

## Cube Store

Cube Store is an open-source aggregation layer.

### Motivation

Over the past year, we've accumulated feedback around various use-cases with pre-aggregations and how to store them. We've learned that there are a set of problems where relational databases as a storage layer has significant performance and functionality issues.

These problems include:

- Performance issues with high cardinality rollups (1B and more)
- Lack of HyperLogLog support
- Degraded performance for big `UNION ALL` queries
- Poor `JOIN` performance across rolled up tables
- Table/schema name length issues across different database types
- SQL type differences between source and external database

Over time, we realized that if we try to fix these issues with existing database engines, we'd end up modifying these databases' codebases in one way or another.

We decided to take another approach and write our own materialized OLAP cache store, designed solely to store and serve rollup tables at scale.

### Supported platforms

<!-- prettier-ignore-start -->
[[info | ]]
| If your platform and architecture is not supported, you can launch Cube Store by Docker.
<!-- prettier-ignore-end -->

| Target                               | Status |
|--------------------------------------|-------:|
| `x86_64-linux-gnu`                   |   ✓    |
| `x86_64-linux-musl`                  |   ✓    |
| `x86-linux-gnu`                      |  N/A   |
| `x86-linux-musl`                     |  N/A   |
| `x86_64-darwin`                      |   ✓    |
| `arm64-darwin` [1]                   |   ✓    |
| `x86_64-win32`                       |   ✓    |
| `x86-win32`                          |  N/A   |

[1] It can be launched by Rosseta 2 via the `x86_64-apple` binary.

### Installation

#### Automatically provisioning in Development mode

<!-- prettier-ignore-start -->
[[info | ]]
| You should use CUBEJS_DEV_MODE=true and EXTERNAL_DB variables should not be defined.
<!-- prettier-ignore-end -->

Starting from `v0.26.48` version, Cube.js ships with automatically provisioning for Cube Store in `CUBEJS_DEV_MODE`. You don't need to set up
any `EXTERNAL_DB` variables or `externalDriverFactory` inside your `cube.js` configuration file.

For versions before `v0.26.48`, You should upgrade your project to the latest version and install a driver for Cube Store:

```bash
$ npm add --save-dev @cubejs-backend/cubestore-driver
```

After starting up, Cube.js will print a message:

``
🔥 Cube Store (0.26.64) is assigned to 3030 port.
``

#### Inside Docker

Start Cube Store in a docker container and bind port `3030` to `127.0.0.1`:

```bash
docker run -d -p 3030:3030 cubejs/cubestore:edge
```

Setup connection for external database via `.env` file:

```
CUBEJS_EXT_DB_TYPE=cubestore
CUBEJS_EXT_DB_HOST=127.0.0.1
```

#### Inside Docker via docker-compose

Create a `docker-compose.yml` file with the following content.

```yml
version: '2.2'
services:
  cubestore:
    image: cubejs/cubestore:edge

  cube:
    image: cubejs/cube:latest
    ports:
      # 4000 is a port for Cube.js API
      - 4000:4000
      # 3000 is a port for Playground web server
      # it is available only in dev mode
      - 3000:3000
    env_file: .env
    depends_on:
      - cubestore
    links:
      - cubestore
    volumes:
      - ./schema:/cube/conf/schema
```

Setup connection for external database via `.env` file:

```
CUBEJS_EXT_DB_TYPE=cubestore
CUBEJS_EXT_DB_HOST=cubestore
```
