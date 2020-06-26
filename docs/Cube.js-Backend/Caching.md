---
title: Caching
permalink: /caching
category: Cube.js Backend
menuOrder: 4
---

Cube.js provides a two-level caching system. The first level is **in-memory** cache and is active by default. In-memory cache
requires [Redis](https://redis.io) in [production mode](deployment).

Cube.js [in-memory cache](#in-memory-cache) suits mostly as a protection layer for your database when there's a burst of requests hitting the same data from multiple concurrent users while [pre-aggregations](#pre-aggregations) are designed to provide right balance between time to insight and querying performance.

To reset **in-memory** cache in development mode just restart the server.

The second in-database level is called **pre-aggregations** and requires explicit configuration to activate.

## In-memory Cache

Cube.js caches the results of executed queries using in-memory cache. The cache
key is generated SQL with any existing query dependent pre-aggregations.

Upon incoming request Cube.js first checks the cache using this key.
If nothing is found it executes the query in database, returns result back alongside writing to the cache.
Since 0.15.0 in case there's an existing value in cache it will be returned only if `refreshKey` value for query isn't changed.
Otherwise [query renewal](#in-memory-cache-force-query-renewal) will be performed.
If you want to revert to the background renew behavior that was a default one before 0.15.0 please see [backgroundRenew](@cubejs-backend-server-core#options-reference-orchestrator-options).

### Refresh Keys

Cube.js takes great care to prevent unnecessary queries from hitting your database. The first stage caching system that caches query results stores query results in Redis (or in the in memory store in development), but Cube.js needs a way to know if the data powering that query result has changed. If the data powering the result isn't any different, the cached result is valid and can be returned skipping that expensive query, but if the underlying data is different, the query needs to be re-run.

So, Cube.js defines a `refreshKey` for each cube. [refreshKeys](cube#parameters-refresh-key) can be evaluated by Cube.js to assess if cube data has changed.

__Note__: Cube.js *also caches* the results of `refreshKeys` for a fixed time interval in order to avoid issuing them too often. If you need Cube.js to immediately respond to changes in data, see the [Force Query Renewal](#in-memory-cache-force-query-renewal) section.

When a query's result needs to be refreshed, Cube.js will re-execute the query in the foreground and repopulate the cache.
This means that cached results may still be served to users requesting them while `refreshKey` values aren't changed from Cube.js perspective.
The cache entry will be refreshed in the foreground if one of the two following conditions is met:

- Query cache entry is expired. The default expiration time is 6 hours for cubes with default `refreshKey` and 24 hours where it was set.
- The result of the `refreshKey` SQL query is different from the previous one. At this stage `refreshKey` won't be refreshed in foreground if it's available in cache.

### Refresh Key Implementation

In order for Cube.js to properly expire cache entries and refresh in the background, Cube.js needs a value to track through time. There's a built in default `refreshKey` query strategy that works the following way:

1. Check used pre-aggregations for query and use [pre-aggregations refreshKey](pre-aggregations#refresh-key), if none pre-aggregations are used…
2. Check the `max` of time dimensions with `updated` in the name, if none exist…
3. Check the `max` of any existing time dimension, if none exist…
4. Check the row count for this cube.

You can set up a custom refresh check SQL by changing [refreshKey](cube#parameters-refresh-key) property on the cube level though. There are situations where the default strategy doesn't work, like:

 - forecasting data or other timeseries data where the timestamps being queried are always the same or always purely dependent on the query
 - non-timeseries data like lists of customers or list of suppliers that doesn't have a time dimension
 - other data that may only get `UPDATE`s and few `INSERT`s, meaning the total row count doesn't change often.

In these instances, Cube.js needs a query crafted to detect updates to the rows that power the cubes. Often, a `MAX(updated_at_timestamp)` for OLTP data will accomplish this, or examining a metadata table for whatever system is managing the data to see when it last ran.

Note that the result of `refreshKey` query itself is cached for 10 seconds for RDBMS backends and for 2 minutes for big data backends by default.
You can change it by passing [refreshKey every](cube#parameters-refresh-key) parameter.
This cache is useful so that Cube.js can build query result cache keys without issuing database queries and respond to cached requests very quickly.

### Force Query Renewal

If you need to force a specific query to load fresh data from the database (if it is available), e.g. some real time metrics, you can use the `renewQuery` option in the query. To use it add `renewQuery: true` to your Cube.js query as shown below:

```javascript
{
  measures: ['Orders.count'],
  dimensions: ['Orders.status'],
  renewQuery: true
}
```

The `renewQuery` option applies to the `refreshKey` caching system mentioned above, *not* the actual query result cache. If `renewQuery` is passed, Cube.js will always re-execute the `refreshKey` query, skipping that layer of caching, but, if the result of the `refreshKey` query is the same as the last time it ran, that indicates any current query result cache entries are valid, and they will be served. This means that cached data may still be served by Cube.js even if `renewQuery` is passed. This is a good thing: if the underlying data hasn't changed, the expensive query doesn't need to be re-run, and the database doesn't have to work as hard. This does mean that the `refreshKey` SQL must accurately report data freshness for the `renewQuery` to actually work and renew the query.

For situations like real-time analytics or responding to live user changes to underlying data, the `refreshKey` query cache can prevent fresh data from showing up immediately.
For these situations, you can mostly disable the `refreshKey` cache by setting the [refreshKey every](cube#parameters-refresh-key) parameter to something very low, like `1 second`.
This means Cube.js will always check the data freshness before executing a query, and notice any changed data underneath.

## Pre-Aggregations

The **pre-aggregation** engine builds a layer of aggregated data in your database during the runtime and maintains it to be up-to-date.

<img
src="https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/pre-aggregations-schema.png"
style="border: none"
/>

Upon an incoming request, Cube.js will first look for a relevant pre-aggregation. If it cannot find any, it will build a new one. Once the pre-aggregation is built, all the subsequent requests will go to the pre-aggregated layer instead of hitting the raw data. It could speed the response time by hundreds or even thousands of times.

Pre-aggregations are materialized query results persisted as tables. In order to start using pre-aggregations, Cube.js should have write access to the `stb_pre_aggregations` schema where pre-aggregation tables will be stored.

Pre-aggregations are defined in the data schema. Below is an example of `rollup`
pre-aggregation. You can learn about defining pre-aggregations in [schema
reference.](pre-aggregations)


```javascript
cube(`Orders`, {
  // ...

  preAggregations: {
    amountByCreated: {
      type: `rollup`,
      measureReferences: [amount],
      timeDimensionReference: createdAt,
      granularity: `month`
    }
  }
});
```

### Refresh Strategy

Refresh strategy can be customized by setting the [refreshKey](pre-aggregations#refresh-key) property for the pre-aggregation.

The default value of the `refreshKey` is the same as for cube that defines pre-aggregation.

It can be redefined either by providing SQL

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
        sql: `SELECT MAX(created_at) FROM orders`
      }
    }
  }
});
```

or by providing refresh time interval

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
        every: `12 hour`
      }
    }
  }
});
```

## Keeping Cache Up-to-Date

Cube.js cache is lazy populated on user requests so additional care should be taken to pre-populate it and keep cache data up-to-date.
In order to implement it simple CRON jobs can be used to issue background queries with `renewQuery: true` flag.
For example if you have user dashboard you want to keep fresh, CRON job that issues every query this dashboard have with `renewQuery: true` flag should be run at desired interval to keep this dashboard up-to-date.
If you want to continously look for a new data you can use [Real Time Data Fetch](real-time-data-fetch) for that.
Please note `renewQuery` also triggers pre-aggregations refresh as well.

To keep pre-aggregations fresh Refresh Scheduler can be used.
All pre-aggregations intended to be refreshed during scheduled refresh run should be marked with [scheduledRefresh](pre-aggregations#scheduled-refresh) parameter.
There're `CubejsServerCore.runScheduledRefresh(context, queryingOptions)` and `CubejsServer.runScheduledRefresh(context, queryingOptions)` methods that should be invoked at least once a minute to populate queue for pre-aggregations refresh.

You can use [scheduledRefreshTimer](@cubejs-backend-server-core#options-reference-scheduled-refresh-timer) to trigger Refresh Scheduler or you can do it manually by using simple timer for example:

```javascript
setInterval(() => server.runScheduledRefresh(), 5000);
```

in your **index.js:**
```javascript
const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

setInterval(() => server.runScheduledRefresh(), 5000);

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube.js server (${version}) is listening on ${port}`);
}).catch(e => {
  console.error('Fatal error during server start: ');
  console.error(e.stack || e);
});
```

There's also [REST API](REST-API#api-reference-v-1-run-scheduled-refresh) available to trigger run.

> **NOTE:** `runScheduledRefresh()` call is idempotent and just updates pre-aggregations if required by `refreshKey`. It always uses refreshKey to check if refresh is required or not. In the case `refreshKey` doesn't change it's value it doesn't matter how often you call `runScheduledRefresh()`: such pre-aggregation won't be refreshed.
