---
title: Overview
permalink: /caching
category: Caching
menuOrder: 1
---

Cube.js provides a two-level caching system. The first level is **in-memory**
cache and is active by default. We recommend using [Redis](https://redis.io) for
in-memory cache when
[running Cube.js in production](/deployment/production-checklist).

Cube.js [in-memory cache](#in-memory-cache) acts as a buffer for your database
when there's a burst of requests hitting the same data from multiple concurrent
users while [pre-aggregations](#pre-aggregations) are designed to provide the
right balance between time to insight and querying performance.

To reset the **in-memory** cache in development mode, just restart the server.

The second level of caching is called **pre-aggregations**, and requires
explicit configuration to activate.

We do not recommend changing the default **in-memory** caching configuration
unless it is necessary. To speed up query performance, consider using
**pre-aggregations**.

## Pre-Aggregations

Pre-aggregations is a layer of the aggregated data built and refreshed by
Cube.js. It can dramatically improve the query performance and provide a higher
concurrency.

<!-- prettier-ignore-start -->
[[info |]]
| To start building pre-aggregations, Cube.js requires write access to the
| [pre-aggregations schema](/config#options-reference-pre-aggregations-schema)
| in the source database. Cube.js first builds pre-aggregations as tables in
| the source database and then exports them into the pre-aggregations storage.
<!-- prettier-ignore-end -->

Pre-aggregations are defined in the data schema. You can learn more about
defining pre-aggregations in
[schema reference](/schema/reference/pre-aggregations).

```js
cube(`Orders`, {
  measures: {
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
  },

  dimensions: {
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },

  preAggregations: {
    amountByCreated: {
      type: `rollup`,
      external: true,
      measureReferences: [totalAmount],
      timeDimensionReference: createdAt,
      granularity: `month`,
    },
  },
});
```

## In-memory Cache

Cube.js caches the results of executed queries using in-memory cache. The cache
key is a generated SQL statement with any existing query-dependent
pre-aggregations.

Upon receiving an incoming request, Cube.js first checks the cache using this
key. If nothing is found in the cache, the query is executed in the database and
the result set is returned as well as updating the cache.

If an existing value is present in the cache and the `refreshKey` value for the
query hasn't changed, the cached value will be returned. Otherwise, a SQL query
will be executed either against the pre-aggregations storage or the source
database to populate the cache with the results and return them.

### Refresh Keys

Cube.js takes great care to prevent unnecessary queries from hitting your
database. The first stage caching system caches query results in Redis (or in
the in-memory store in development), but Cube.js needs a way to know if the data
powering that query result has changed. If the underlying data isn't any
different, the cached result is valid and can be returned skipping an expensive
query, but if there is a difference, the query needs to be re-run and its result
cached.

To aid with this, Cube.js defines a `refreshKey` for each cube.
[Refresh keys](/cube#parameters-refresh-key) are evaluated by Cube.js to assess
if the data needs to be refreshed.

```js
cube(`Orders`, {
  // This refreshKey tells Cube.js to refresh data every 5 minutes
  refreshKey: {
    every: `5 minute`
  }

  // With this refreshKey Cube.js will only refresh the data if
  // the value of previous MAX(created_at) changed
  // By default Cube.js will check this refreshKey every 10 seconds
  refreshKey: {
    sql: `SELECT MAX(created_at) FROM orders`
  }
});
```

By default, Cube.js will check and invalidate the cache in the background when
in [development mode][link-development-mode]. When development mode is disabled
you can set `CUBEJS_SCHEDULED_REFRESH_TIMER=true` to enable this behavior.

We recommend enabling background cache invalidation in a separate Cube.js worker
for production deployments. Please consult the [Production
Checklist][link-production-checklist] for more information.

[link-production-checklist]: /deployment/production-checklist
[link-development-mode]: /configuration/overview#development-mode
[link-production-checklist-refresh]:
  /deployment/production-checklist#set-up-refresh-worker

If background refresh is disabled, Cube.js will refresh the cache during query
execution. Since this could lead to delays in responding to end-users, we
recommend always enabling background refresh.

### Default Refresh Keys

The default values for `refreshKey` are

- `every: '2 minute'` for BigQuery, Athena, Snowflake, and Presto.
- `every: '10 second'` for all other databases.

+You can use a custom SQL query to check if a refresh is required by changing
the [`refreshKey`](/cube#parameters-refresh-key) property in a cube's Data
Schema. Often, a `MAX(updated_at_timestamp)` for OLTP data is a viable option,
or examining a metadata table for whatever system is managing the data to see
when it last ran.

### Disabling the cache

There's no straightforward way to disable caching in Cube.js. The reason is that
Cube.js not only stores cached values but also uses the cache as a point of
synchronization and coordination between nodes in a cluster. For the sake of
design simplicity, Cube.js doesn't distinguish client invocations, and all calls
to the data load API are idempotent. This provides excellent reliability and
scalability but has some drawbacks. One of those load data calls can't be traced
to specific clients, and as a consequence, there's no guaranteed way for a
client to initiate a new data loading query or know if the current invocation
wasn't initiated earlier by another client. Only Refresh Key freshness
guarantees are provided in this case.

For situations like real-time analytics or responding to live user changes to
underlying data, the `refreshKey` query cache can prevent fresh data from
showing up immediately. For these situations, the cache can effectively be
disabled by setting the
[`refreshKey.every`](/schema/reference/cube#parameters-refresh-key) parameter to
something very low, like `1 second`.

## Inspecting Queries

To inspect whether the query hits in-memory cache, pre-aggregation, or the
underlying data source, you can use the Playground or [Cube
Cloud][link-cube-cloud].

[Developer Playground][link-dev-playground] can be used to inspect a single
query. To do that, click the "cache" button after executing the query. It will
show you the information about the `refreshKey` for the query and whether the
query uses any pre-aggregations. To inspect multiple queries or list existing
pre-aggregations, you can use Cube Cloud.

<!-- prettier-ignore-start -->
[[info |]]
| [Cube Cloud][link-cube-cloud] currently is in early access. If you don't have
| an account yet, you can [sign up to the waitlist here][link-cube-cloud].
<!-- prettier-ignore-end -->

[link-cube-cloud]: https://cube.dev/cloud

To inspect queries in the Cube Cloud, navigate to the "History" page. You can
filter queries by multiple parameters on this page, including whether they hit
the cache, pre-aggregations, or raw data. Additionally, you can click on the
query to see its details, such as time spent in the database, the database
queue's size at the point of query execution, generated SQL, query timeline, and
more. It will also show you the optimal pre-aggregations that could be used for
this query.

To see existing pre-aggregations, navigate to the "Pre-Aggregations" page in the
Cube Cloud. The table shows all the pre-aggregations, the last refresh
timestamp, and the time spent to build the pre-aggregation. You can also inspect
every pre-aggregation's details: the list of queries it serves and all its
versions.

[link-dev-playground]: /dev-tools/dev-playground
