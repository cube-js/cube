---
title: Using Pre-Aggregations
permalink: /caching/using-pre-aggregations
category: Caching
menuOrder: 3
---

Pre-aggregations is a powerful way to speed up your Cube.js queries. There are
many configuration options to consider. Please make sure to also check [this
Pre-Aggregations page in the data schema section][ref-preaggs].

## Refresh Strategy

Refresh strategy can be customized by setting the
[refreshKey][ref-preaggs-refresh-key] property for the pre-aggregation.

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

Please consult the [Production Checklist][ref-production-checklist-refresh] for
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
    }),
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

## Partitioning

[Partitioning][wiki-partitioning] is an extremely effective optimization for
improving data access. It effectively "shards" the data between multiple tables,
splitting them by a defined attribute. An incoming query would be checked for
this attribute, and **only** valid partitions required to satisfy it are
selected. This results in faster refresh times due to unnecessary data not being
scanned and processed, and possibly even reduced cost, depending on your
database solution.

Cube.js supports [both time][ref-preagg-time-part] and
[segment-based][ref-preagg-segment-part] partitioning. However, it must first be
enabled for each pre-aggregation.

[Time-based partitioning][ref-preagg-time-part] is especially helpful for
incremental refreshes; when configured, Cube.js will only refresh partitions as
necessary. Without incremental refreshing, Cube.js will re-calculate the entire
pre-aggregation whenever [the refresh key][ref-preaggs-refresh-key] changes.

## Pre-Aggregations Storage

Cube.js can store pre-aggregations on the source database or on a separate,
**external** database.

When using **external** pre-aggregations, Cube.js will store pre-aggregations
inside its own purpose-built storage layer: Cube Store.

Alternatively, you can store external pre-aggregations in a different database,
such MySQL or Postgres. In order to make this work, you should set the
[`externalDriverFactory`][ref-config-extdriverfactory] and
[`externalDbType`][ref-config-extdbtype] properties in your `cube.js`
configuration file. These properties can also be set through the environment
variables.

```bash
CUBEJS_EXT_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_EXT_DB_PORT=<YOUR_DB_PORT_HERE>
CUBEJS_EXT_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_EXT_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_EXT_DB_PASS=<YOUR_DB_PASS_HERE>
CUBEJS_EXT_DB_TYPE=<SUPPORTED_DB_TYPE_HERE>
```

### Known limitations of MySQL/Postgres as external database

<!-- prettier-ignore-start -->
[[warning |]]
| Please be aware of the limitations when using MySQL/Postgres as an external
| database for pre-aggregations.
<!-- prettier-ignore-end -->

Some known limitations are listed below.

**Performance issues with high cardinality rollups:** Queries over billions of
datapoints or higher start exhibiting severe latency issues, negatively
impacting end-user experience.

**Lack of HyperLogLog support:** The HyperLogLog algorithm makes fast work of
distinct counts in queries, a common analytical operation. Unfortunately,
support between database vendors varies greatly, and therefore cannot be
guaranteed.

**Degraded performance for big `UNION ALL` queries:** A practical example of
this would be when querying over a date range using a pre-aggregation with a
`partitionGranularity`. The query would use several partitioned tables to
deliver the result set, and therefore needs to join all valid partitions.

**Poor JOIN performance across rolled up tables:** This often affects workloads
which require cross database joins.

**Table/schema name length mismatches:** A common issue when working across
different database types is that different databases have different length
limits on table names. Cube.js allows working around the issue with `sqlAlias`
but this becomes cumbersome with lots of pre-aggregations.

**SQL type differences between source and external database:** Different
databases often specify types differently, which can cause type mismatch issues.
This is also a common issue and source of frustration which Cube Store resolves.

## External vs Internal

In Cube.js, pre-aggregations are called **external** when they are flagged with
`external: true` which instructs Cube.js to store pre-aggregations inside its
own storage - Cube Store.

If pre-aggregations aren't flagged `external: true` they are considered
**internal** and will be saved to and queried from the source database.

<!-- prettier-ignore-start -->
[[info | ]]
| We recommend always using **external** pre-aggregations for better concurrency and performance.
<!-- prettier-ignore-end -->

You should use external pre-aggregations for scenarios where you need high
throughput for a big data backend. It allows downloading rollups and original
SQL pre-aggregations prepared in big data backends such as AWS Athena, BigQuery,
Presto, Hive and others to Cube Store for low latency and high throughput
querying.

While big data backends aren't very suitable for handling massive amounts of
concurrent queries even on pre-aggregated data, Cube.js pre-aggregations storage
can do it very well.

To set it up, simply add the `external` property to your pre-aggregation:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  //...

  preAggregations: {
    categoryAndDate: {
      type: `rollup`,
      measureReferences: [Orders.count, revenue],
      dimensionReferences: [category],
      timeDimensionReference: createdAt,
      granularity: `day`,
      partitionGranularity: `month`,
      external: true,
    },
  },
});
```

Note that by default, Cube.js materializes the pre-aggregation query results as
new tables in the source database. For external pre-aggregations, these source
tables are temporary - once downloaded and uploaded to the external database,
they are cleaned up.

### Known limitations of internal pre-aggregations

Internal pre-aggregations are not considered production-ready due to several
shortcomings, as noted below.

**Concurrency:** Databases (especially RDBMs) generally cannot handle high
concurrency without special configuration. Application databases such as MySQL
and Postgres do support concurrency, but often cannot cope with the demands of
analytical queries without significant tuning. On the other hand, data
warehousing solutions such as Athena and BigQuery often limit the number of
concurrent connections too.

**Latency:** Data warehousing solutions (such as BigQuery or Redshift) are often
slow to return results.

**Cost:** Some databases charge by the amount of data scanned for each query
(such as AWS Athena and BigQuery). Repeatedly querying for this data can easily
rack up costs.

## Garbage Collection

When pre-aggregations are refreshed, Cube.js will create new pre-aggregation
tables each time a version change is detected. This allows for seamless,
transparent hot swapping of tables for users of any database, even for those
without DDL transactions support.

However, it does lead to orphaned tables which need to be collected over time.
By default, Cube.js will store all content versions for 10 minutes and all
structure versions for 7 days. Then it will retain only the most recent ones and
orphaned tables are dropped from the database.

## Inspecting Pre-Aggregations

Cube Store partially supports the MySQL protocol. This allows you to execute
simple queries using a familiar SQL syntax. To check which pre-aggregations are
managed by Cube Store, for example, you could run the following query:

```sql
SELECT * FROM information_schema.tables;
```

These pre-aggregations are stored as Parquet files under the `.cubestore/`
folder in the project root during development.

## Running in Production

Cube Store is enabled by default when running Cube.js in development mode. In
production, Cube Store **must** run as a separate process. The easiest way to do
this is to use the official Docker images for Cube.js and Cube Store.

You can run Cube Store with Docker with the following command:

```bash
docker run -p 3030:3030 cubejs/cubestore
```

<!-- prettier-ignore-start -->
[[info | ]]
| Cube Store can further be configured via environment variables. To see a
| complete reference, please consult the [Cube Store section of the Environment
| Variables reference][ref-config-env].
<!-- prettier-ignore-end -->

Next, run Cube.js and tell it to connect to Cube Store running on `localhost`
(on the default port `3030`):

```bash
docker run -p 4000:4000 \
  -e CUBEJS_CUBESTORE_HOST=localhost \
  -v ~/cube-conf:/cube/conf \
  cubejs/cube
```

In the command above, we're specifying `CUBEJS_CUBESTORE_HOST` to let Cube.js
know where Cube Store is running.

You can also use Docker Compose to achieve the same:

```yaml
version: '2.2'
services:
  cubestore:
    image: cubejs/cubestore:latest

  cube:
    image: cubejs/cube:latest
    ports:
      - 4000:4000
    environment:
      - CUBEJS_CUBESTORE_HOST=localhost
      - CUBEJS_CUBESTORE_PORT=3030
    depends_on:
      - cubestore
    links:
      - cubestore
    volumes:
      - ./schema:/cube/conf/schema
```

### Scaling

<!-- prettier-ignore-start -->
[[warning | ]]
| Cube Store _can_ be run in a single instance mode, but this is usually
| unsuitable for production deployments. For high concurrency and data
| throughput, we **strongly** recommend running Cube Store as a cluster of
| multiple instances instead.
<!-- prettier-ignore-end -->

Scaling Cube Store for a higher concurrency is relatively simple when running in
cluster mode. Because [the storage layer](#running-in-production-storage) is
decoupled from the query processing engine, you can horizontally scale your Cube
Store cluster for as much concurrency as you require.

In cluster mode, Cube Store runs two kinds of nodes:

- a single **router** node handles incoming client connections, manages database
  metadata and serves simple queries.
- multiple **worker** nodes which execute SQL queries

In terms of configuration, both the router and worker nodes **must** specify the
`CUBESTORE_SERVER_NAME` environment variable. Additionally, the router node
**must** specify the `CUBESTORE_META_PORT` and `CUBESTORE_WORKERS` environment
variables; worker nodes **must** specify the `CUBESTORE_WORKER_PORT` and
`CUBESTORE_META_ADDR` environment variables. More information about these
variables can be found [in the Environment Variables reference][ref-config-env].

A sample Docker Compose stack setting this up might look like:

```yaml
version: '2.2'
services:
  cubestore_router:
    restart: always
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_LOG_LEVEL=trace
      - CUBESTORE_SERVER_NAME=cubestore_router:9999
      - CUBESTORE_META_PORT=9999
      - CUBESTORE_WORKERS=cubestore_worker_1:9001,cubestore_worker_2:9001
    expose:
      - 9999 # This exposes the Metastore endpoint
      - 3306 # This exposes the MySQL endpoint
      - 3030 # This exposes the HTTP endpoint for CubeJS
  cubestore_worker_1:
    restart: always
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_SERVER_NAME=cubestore_worker_1:9001
      - CUBESTORE_WORKER_PORT=9001
      - CUBESTORE_META_ADDR=cubestore_router:9999
    depends_on:
      - cubestore_router
    expose:
      - 9001
  cubestore_worker_2:
    restart: always
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_SERVER_NAME=cubestore_worker_2:9001
      - CUBESTORE_WORKER_PORT=9001
      - CUBESTORE_META_ADDR=cubestore_router:9999
    depends_on:
      - cubestore_router
    expose:
      - 9001

  cube:
    image: cubejs/cube:latest
    ports:
      - 4000:4000
      - 9999
    environment:
      - CUBEJS_CUBESTORE_HOST=cubestore_router
      - CUBEJS_CUBESTORE_PORT=9999
    depends_on:
      - cubestore_router
    volumes:
      - .:/cube/conf
```

### Storage

<!-- prettier-ignore-start -->
[[warning | ]]
| Cube Store can only use one type of remote storage at runtime.
<!-- prettier-ignore-end -->

Cube Store makes use of a separate storage layer for storing metadata as well as
for persisting pre-aggregations as Parquet files. Cube Store can use both AWS S3
and Google Cloud, or if desired, a local path on the server.

A simplified example with Docker Compose might look like:

```yaml
version: '2.2'
services:
  cubestore:
    image: cubejs/cubestore:latest
    environment:
      - CUBESTORE_S3_BUCKET=<BUCKET_NAME_ON_S3>
      - CUBESTORE_S3_REGION=<AWS_REGION>
      - CUBESTORE_S3_SUB_PATH=/
      - CUBESTORE_AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
      - CUBESTORE_AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
  cube:
    image: cubejs/cube:latest
    ports:
      - 4000:4000
    environment:
      - CUBEJS_CUBESTORE_HOST=localhost
      - CUBEJS_CUBESTORE_PORT=3030
    depends_on:
      - cubestore
    links:
      - cubestore
    volumes:
      - ./schema:/cube/conf/schema
```

[wiki-partitioning]: https://en.wikipedia.org/wiki/Partition_(database)
[ref-config-env]: /reference/environment-variables#cube-store
[ref-schema-timedimension]: /types-and-formats#dimensions-types-time
[ref-preaggs]: /pre-aggregations
[ref-preagg-time-part]: /pre-aggregations#rollup-time-partitioning
[ref-preagg-segment-part]: /pre-aggregations#rollup-segment-partitioning
[ref-preaggs-refresh-key]: /pre-aggregations#refresh-key
[ref-config-extdbtype]: /config#options-reference-external-db-type
[ref-config-driverfactory]: /config#options-reference-driver-factory
[ref-config-extdriverfactory]: /config#options-reference-external-driver-factory
[ref-production-checklist-refresh]:
  /deployment/production-checklist#set-up-refresh-worker
