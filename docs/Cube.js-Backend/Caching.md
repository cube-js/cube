---
title: Caching
permalink: /caching
category: Cube.js Backend
menuOrder: 4
---

Cube.js provides a two-level caching system. The first level is **in-memory** cache and is active by default. In-memory cache
requires [Redis](https://redis.io) in [production mode](deployment).

To reset **in-memory** cache in development mode just restart the server.

The second in-database level is called **pre-aggregations** and requires explicit configuration to activate.

## In-memory Cache

Cube.js caches the results of executed queries using in-memory cache. The cache
key is generated SQL with any existing query dependent pre-aggregations.

Upon incoming request Cube.js first checks the cache using this key. If nothing
is found it executes the query in database, returns result back alongside writing to the cache.
If Cube.js detects existing cache entry for the key it schedules a background cache refresh check.


### Refresh Strategy

Background refresh check decides whether the cache entry should be refreshed or
not. The cache entry will be refreshed in the background, if one of the two following conditions is met:

- Cache is expired. The default expiration time is 6 hours.
- The result of the `refreshKey` SQL query is different from the previous one.

You can set up a custom refresh check SQL by changing [refreshKey](cube#parameters-refresh-key) property on the cube level. The default strategy works the following way:

1. Check the `max` of time dimensions with `updated` in the name, if none exist…
2. Check the `max` of any existing time dimension, if none exist…
3. Check the count of rows for this cube.

Result of `refreshKey` query itself is cached for 2 minutes by default. You can
change it by passing [refreshKeyRenewalThreshold](@cubejs-backend-server-core#cubejs-server-core-create-options-orchestrator-options) option when configuring
Cube.js Server.

### Force Query Renewal

If you need a specific query to bypass cache and return data from the
database, e.g. some real time metrics, you can use `renewQuery` option in the
query. To use it add `renewQuery: true` to your Cube.js query as shown below:

```javascript
{
  measures: ['Orders.count'],
  dimensions: ['Orders.status'],
  renewQuery: true
}
```

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
preAggregations: {
  amountByCreated: {
    type: `rollup`,
    measureReferences: [amount],
    timeDimensionReference: createdAt,
    granularity: `month`
  }
}
```

### Refresh Strategy

Every two minutes on a new request Cube.js will initiate the refresh
check. Refresh strategy could be customized by setting the `refreshKey` property
for the pre-aggregation.

The default value of the `refreshKey` is `select date_trunc('hour', now())`. It means
that by default pre-aggregations would refresh **every hour**.

```javascript
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
```

