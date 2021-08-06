---
title: Using Pre-Aggregations
permalink: /caching/using-pre-aggregations
category: Caching
menuOrder: 3
---

<!-- prettier-ignore-start -->
[[info |]]
| The Cube.js pre-aggregations workshop is on August 18th at 9-11 am PST! If you
| want to learn why/when you want to use pre-aggregations, how to get started,
| tips & tricks, you will want to attend this event 😀 <br/> You can register
| for the workshop at [the event
| page](https://cube.dev/events/pre-aggregations/).
<!-- prettier-ignore-end -->

Pre-aggregations is a powerful way to speed up your Cube.js queries. There are
many configuration options to consider. Please make sure to also check [the
Pre-Aggregations reference in the data schema section][ref-schema-ref-preaggs].

## Refresh Strategy

Refresh strategy can be customized by setting the
[refreshKey][ref-schema-ref-preaggs-refresh-key] property for the
pre-aggregation.

The default value of `refreshKey` is `every: '1 hour'`. It can be redefined
either by providing SQL:

```javascript
cube(`Orders`, {
  // ...

  preAggregations: {
    amountByCreated: {
      type: `rollup`,
      measures: [amount],
      timeDimension: createdAt,
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
      measures: [amount],
      timeDimension: createdAt,
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
`scheduledRefresh: true`. You can find more information about this setting in
the [Pre-Aggregation Reference][ref-schema-ref-preaggs-sched-refresh].

In development mode, Cube.js enables background refresh by default and will
refresh all pre-aggregations marked with the
[`scheduledRefresh`][ref-schema-ref-preaggs-sched-refresh] parameter.

Please consult the [Production Checklist][ref-prod-list-refresh] for best
practices on running background refresh in production environments.

```js
cube(`Orders`, {
  // ...

  preAggregations: {
    amountByCreated: {
      type: `rollup`,
      measures: [amount],
      timeDimension: createdAt,
      granularity: `month`,
      scheduledRefresh: true,
    },
  },
});
```

## Rollup Only Mode

To make Cube.js _only_ serve requests from pre-aggregations, the
[`CUBEJS_ROLLUP_ONLY` environment variable][ref-config-env-general] can be set
to `true` on an API instance. This will prevent it from checking the freshness
of the pre-aggregations; a separate [Refresh Worker][ref-deploy-refresh-wrkr]
must be configured to keep the pre-aggregations up-to-date.

<!-- prettier-ignore-start -->
[[warning |]]
| In a single node deployment (where the API instance and [Refresh Worker
| ][ref-deploy-refresh-wrkr] are configured on the same host), requests made to
| the API that cannot be satisfied by a rollup throw an error. Scheduled
| refreshes will continue to work in the background; if a pre-aggregation is
| being built at the time of a request, then the request will wait until the
| build is complete before returning results.
<!-- prettier-ignore-end -->

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
database solution. Cube.js supports partitioning data using the `timeDimension`
property in [a pre-aggregation definition][ref-schema-ref-preaggs].

### Time partitioning

Time-based partitioning is especially helpful for incremental refreshes; when
configured, Cube.js will only refresh partitions as necessary. Without
incremental refreshing, Cube.js will re-calculate the entire pre-aggregation
whenever [the refresh key][ref-schema-ref-preaggs-refresh-key] changes.

<!-- prettier-ignore-start -->
[[warning |]]
| Partitioned rollups currently cannot be used by queries without time
| dimensions.
<!-- prettier-ignore-end -->

Any `rollup` pre-aggregation can be partitioned by time using the
`partitionGranularity` property. In the example below, the
`partitionGranularity` is set to `month`, which means Cube.js will generate
separate tables for each month's worth of data:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  ...,

  preAggregations: {
    categoryAndDate: {
      measures: [Orders.count, revenue],
      dimensions: [category],
      timeDimension: createdAt,
      granularity: `day`,
      partitionGranularity: `month`,
    },
  },
});
```

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

## Pre-Aggregations Storage

The default pre-aggregations storage in Cube.js is its own purpose-built storage
layer: Cube Store.

Alternatively, you can store pre-aggregations either **internally** in the
source database, or **externally** in databases such as MySQL or Postgres.

In order to make external pre-aggregations work outside of Cube Store, you
should set the [`externalDriverFactory`][ref-config-extdriverfactory] and
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

<!-- prettier-ignore-start -->
[[warning |]]
| Please be aware of the limitations when using internal and external (outside
| of Cube Store) pre-aggregations.
<!-- prettier-ignore-end -->

<div
  style="text-align: center"
>
  <img
  alt="Internal vs External vs External with Cube Store diagram"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Caching/pre-aggregations.png"
  style="border: none"
  width="100%"
  />
</div>

#### Some known limitations when using Postgres/MySQL as a storage layer listed below.

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

#### Internal pre-aggregations

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

## Optimizing Pre-Aggregation Build Times

<!-- prettier-ignore-start -->
[[info | ]]
| For ideal performance, pre-aggregations should be built using a dedicated
| Refresh Worker. [See here for more details][ref-prod-list-refresh].
<!-- prettier-ignore-end -->

By default, Cube.js will use the source database as a temporary staging area for
writing pre-aggregations to determine column types. The data is loaded back into
memory before writing them to Cube Store (or an external database).

<div
  style="text-align: center"
>
  <img
  alt="Internal vs External vs External with Cube Store diagram"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Caching/build-regular.png"
  style="border: none"
  width="100%"
  />
</div>

If the dataset is large (more than 100k rows), then Cube.js can face issues when
the Node runtime runs out of memory.

### Batching

Batching is a more performant strategy where Cube.js sends compressed CSVs for
Cube Store to ingest.

<div
  style="text-align: center"
>
  <img
  alt="Internal vs External vs External with Cube Store diagram"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Caching/build-batching.png"
  style="border: none"
  width="100%"
  />
</div>

The performance scales to the amount of memory available on the Cube.js
instance. Batching is automatically enabled for any databases that can support
it.

### Export bucket

When dealing with larger pre-aggregations (more than 100k rows), performance can
be significantly improved by using an export bucket. This allows the source
database to persist data directly into cloud storage, which is then loaded into
Cube Store in parallel:

<div
  style="text-align: center"
>
  <img
  alt="Internal vs External vs External with Cube Store diagram"
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Caching/build-export-bucket.png"
  style="border: none"
  width="100%"
  />
</div>

Enabling the export bucket functionality requires extra configuration; please
refer to the database-specific documentation for more details:

- [AWS Athena][ref-connect-db-athena] (coming soon)
- [AWS Redshift][ref-connect-db-redshift]
- [BigQuery][ref-connect-db-bigquery]
- [Snowflake][ref-connect-db-snowflake]

When using cloud storage, it is important to correctly configure any data
retention policies to clean up the data in the export bucket as Cube.js does not
currently manage this. For most use-cases, 1 day is sufficient.

[ref-config-connect-db]: /connecting-to-the-database
[ref-config-driverfactory]: /config#options-reference-driver-factory
[ref-config-env]: /reference/environment-variables#cube-store
[ref-config-env-general]: /config#general
[ref-config-extdbtype]: /config#options-reference-external-db-type
[ref-config-extdriverfactory]: /config#options-reference-external-driver-factory
[ref-connect-db-athena]: /config/databases/aws-athena
[ref-connect-db-redshift]: /config/databases/aws-redshift
[ref-connect-db-bigquery]: /config/databases/google-bigquery
[ref-connect-db-mysql]: /config/databases/mysql
[ref-connect-db-postgres]: /config/databases/postgres
[ref-connect-db-snowflake]: /config/databases/snowflake
[ref-schema-timedimension]: /types-and-formats#dimensions-types-time
[ref-schema-ref-preaggs]: /schema/reference/pre-aggregations
[ref-schema-ref-preaggs-refresh-key]:
  /schema/reference/pre-aggregations#parameters-refresh-key
[ref-deploy-refresh-wrkr]: /deployment/overview#refresh-worker
[ref-schema-ref-preaggs-sched-refresh]:
  /schema/reference/pre-aggregations#parameters-scheduled-refresh
[ref-prod-list-refresh]: /deployment/production-checklist#set-up-refresh-worker
[wiki-partitioning]: https://en.wikipedia.org/wiki/Partition_(database)
