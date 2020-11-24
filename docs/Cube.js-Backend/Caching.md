---
title: Caching
permalink: /caching
category: Cube.js Backend
menuOrder: 6
---

[link-cube-cloud]: https://cube.dev/cloud

Cube.js provides a two-level caching system. The first level is **in-memory** cache and is active by default. We recommend using [Redis](https://redis.io) for in-memory cache when [running Cube.js in production](/deployment/production-checklist).

Cube.js [in-memory cache](#in-memory-cache) acts as a buffer for your database when there's a burst of requests hitting the same data from multiple concurrent users while [pre-aggregations](#pre-aggregations) are designed to provide the right balance between time to insight and querying performance.

To reset the **in-memory** cache in development mode, just restart the server.

The second level of caching is called **pre-aggregations**, and requires explicit configuration to activate.

## In-memory Cache

Cube.js caches the results of executed queries using in-memory cache. The cache
key is a generated SQL statement with any existing query-dependent
pre-aggregations.

Upon receiving an incoming request, Cube.js first checks the cache using this key.
If nothing is found in the cache, the query is executed in the database and the result set
is returned as well as updating the cache.

If an existing value is present in the cache and the `refreshKey` value for
the query hasn't changed, the cached value will be returned. Otherwise, a
[query renewal](#in-memory-cache-force-query-renewal) will be performed.

### Refresh Keys

Cube.js takes great care to prevent unnecessary queries from hitting your database. The first stage caching system caches query results in Redis (or in the in-memory store in development), but Cube.js needs a way to know if the data powering that query result has changed. If the underlying data isn't any different, the cached result is valid and can be returned skipping that expensive query, but if there is a difference, the query needs to be re-run and its' result cached.

To aid with this, Cube.js defines a `refreshKey` for each cube. [Refresh keys](cube#parameters-refresh-key) can be evaluated by Cube.js to assess if cube data has changed.

[[info | Please note]]
| Cube.js **caches** the result of a `refreshKey` for a fixed time interval in order to avoid issuing them too often. If you need Cube.js to immediately respond to changes in data, see the [Force Query Renewal](#in-memory-cache-force-query-renewal) section.

When the result of a query needs to be refreshed, Cube.js will re-execute the query in the foreground and re-populate the cache.
This means that cached results may still be served to users requesting them while `refreshKey` values aren't changed from Cube.js perspective.
The cache entry will be refreshed in the foreground if one of the two following conditions is met:

- The result of the `refreshKey` SQL query is different from the previous one. At this stage `refreshKey` won't be refreshed in foreground if it's available in the cache.
- The query cache entry has expired. The default expiration time for cubes with a default `refreshKey` is 6 hours, cubes specifying their own expire in 24 hours.

### Refresh Key Implementation

In order for Cube.js to properly expire cache entries and refresh in the background, Cube.js needs a value to track through time. There's a built in default `refreshKey` query strategy that works the following way:

1. Check used pre-aggregations for query and use [pre-aggregations refreshKey](pre-aggregations#refresh-key), if no pre-aggregations are used then…
2. Check the `max` of time dimensions with `updated` in the name, if none exist then…
3. Check the `max` of any existing time dimension, if none exist then…
4. Check the row count for this cube.

You can set up a custom refresh check SQL by changing [refreshKey](cube#parameters-refresh-key) property in a cube's Data Schema. There are situations where the default strategy doesn't work, such as:

 - forecasting data or other time series data where the timestamps being queried are always the same or always purely dependent on the query
 - non-time series data like lists of customers or list of suppliers that doesn't have a time dimension
 - other data that may only get `UPDATE`s and few `INSERT`s, meaning the total row count doesn't change very often.

In these instances, Cube.js needs a query crafted to detect updates to the rows that power the cubes. Often, a `MAX(updated_at_timestamp)` for OLTP data will accomplish this, or examining a metadata table for whatever system is managing the data to see when it last ran.


[[info | Please note]]
| The result of the `refreshKey` query is cached for 10 seconds for RDBMS backends and for 2 minutes for big data backends by default. You can change it by passing [refreshKey every](cube#parameters-refresh-key) parameter. See [refreshKey every](cube#parameters-refresh-key) doc to learn more about the implementation. This cache is useful so Cube.js can build query result cache keys without issuing database queries and respond to cached requests very quickly.

### Force Query Renewal

If you need to force a specific query to load fresh data from the database (if it is available), e.g. some real time metrics, you can use the `renewQuery` option in the query. To use it, add `renewQuery: true` to your Cube.js query as shown below:

```javascript
{
  measures: ['Orders.count'],
  dimensions: ['Orders.status'],
  renewQuery: true
}
```

The `renewQuery` option applies to the `refreshKey` caching system mentioned above, *not* the actual query result cache. If `renewQuery` is passed, Cube.js will always re-execute the `refreshKey` query, skipping that layer of caching, but, if the result of the `refreshKey` query is the same as the last time it ran, that indicates any current query result cache entries are valid, and they will be served. This means that cached data may still be served by Cube.js even if `renewQuery` is passed. This is a good thing: if the underlying data hasn't changed, the expensive query doesn't need to be re-run, and the database doesn't have to work as hard. This does mean that the `refreshKey` SQL must accurately report data freshness for the `renewQuery` to actually work and renew the query.

For situations like real-time analytics or responding to live user changes to underlying data, the `refreshKey` query cache can prevent fresh data from showing up immediately.
For these situations, the `refreshKey` cache can effectively be disabled by setting the [refreshKey every](cube#parameters-refresh-key) parameter to something very low, like `1 second`.
This means Cube.js will always check the data freshness before executing a query, and notice any changed data underneath.

### How to disable the cache?

There's no straightforward way to disable caching in Cube.js.
The reason for it is Cube.js is not just stores cached values but uses the cache as a point of synchronization and coordination between nodes in a cluster.
For the sake of design simplicity, Cube.js doesn't distinguish client invocations, and all calls to the data load API are idempotent.
This provides excellent reliability and scalability but has some drawbacks.
One of those load data calls can't be traced to specific clients, and as a consequence, there's no guaranteed way for a client to initiate a new data loading query or know if the current invocation wasn't initiated earlier by another client.
Only Refresh Key freshness guarantees are provided in this case.

If you find yourself in a situation that you want to disable the cache, it usually means you'd like to revisit your Refresh Key strategy.
Great place to start is to enable [scheduledRefreshTimer](/config#options-reference-scheduled-refresh-timer) and set [refreshKey every](cube#parameters-refresh-key) on cubes of interest.
This way, Refresh Scheduler will keep the `refreshKey` result up-to-date so queries won't hit stale results.

## Pre-Aggregations

The **pre-aggregation** engine builds a layer of aggregated data in your database during runtime and maintains it to be up-to-date.

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

The default value of the `refreshKey` is the same its' cube. It can be redefined either by providing SQL:

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
        every: `12 hour`
      }
    }
  }
});
```

### Keeping Pre-aggregations Up-to-Date

You can use the Refresh Scheduler to keep pre-aggregations fresh.
All pre-aggregations intended to be refreshed during a scheduled refresh run should be marked with [`scheduledRefresh`](pre-aggregations#scheduled-refresh) parameter.

```js
cube(`Orders`, {
  // ...

  preAggregations: {
    amountByCreated: {
      type: `rollup`,
      measureReferences: [amount],
      timeDimensionReference: createdAt,
      granularity: `month`,
      scheduledRefresh: true
    }
  }
});
```

You can set [scheduledRefreshTimer](config#options-reference-scheduled-refresh-timer) option to trigger Refresh Scheduler.
For serverless deployments [REST API](rest-api#api-reference-v-1-run-scheduled-refresh) should be used instead of timer.

## Inspecting Queries
To inspect whether the query hits in-memory cache, pre-aggregation, or the underlying data source, you can use the Playground or [Cube Cloud][link-cube-cloud].

The Playground can be used to inspect a single query. To do that, click the "cache" button after executing the query. It will show you the information about the `refreshKey` for the query and whether the query uses any pre-aggregations. To inspect multiple queries or list existing pre-aggregations, you can use Cube Cloud.

[[info | ]]
| [Cube Cloud][link-cube-cloud] currently is in early access. If you don't have an account yet, you can [sign up to the waitlist here](https://cube.dev/cloud).

To inspect queries in the Cube Cloud, navigate to the "History" page. You can filter queries by multiple parameters on this page, including whether they hit the cache, pre-aggregations, or raw data. Additionally, you can click on the query to see its details, such as time spent in the database, the database queue's size at the point of query execution, generated SQL, query timeline, and more. It will also show you the optimal pre-aggregations that could be used for this query.

To see existing pre-aggregations, navigate to the "Pre-Aggregations" page in the Cube Cloud. The table shows all the pre-aggregations, the last refresh timestamp, and the time spent to build the pre-aggregation. You can also inspect every pre-aggregation's details: the list of queries it serves and all its versions.
