---
title: Getting Started with Pre-Aggregations
permalink: /caching/pre-aggregations/getting-started
category: Caching
menuOrder: 2
---

Often at the beginning of an analytical application's lifecycle - when there is
a smaller dataset that queries execute over - the application works well and
delivers responses within acceptable thresholds. However, as the size of the
dataset grows, the time-to-response from a user's perspective can often suffer
quite heavily. This is true of both application and purpose-built data
warehousing solutions.

This leaves us with a chicken-and-egg problem; application databases can deliver
low-latency responses with small-to-large datasets, but struggle with massive
analytical datasets; data warehousing solutions _usually_ make no guarantees
except to deliver a response, which means latency can vary wildly on a
query-to-query basis.

| Database Type                  | Low Latency? | Massive Datasets? |
| ------------------------------ | ------------ | ----------------- |
| Application (Postgres/MySQL)   | ✅           | ❌                |
| Analytical (BigQuery/Redshift) | ❌           | ✅                |

Cube.js provides a solution to this problem: pre-aggregations. In layman's
terms, a pre-aggregation is a condensed version of the source data. It specifies
attributes from the source, which Cube.js uses to condense (or crunch) the data.
This simple yet powerful optimization can reduce the size of the dataset by
several orders of magnitude, and ensures subsequent queries can be served by the
same condensed dataset if any matching attributes are found.

[Pre-aggregations are defined within each cube's data
schema][ref-schema-preaggs], and cubes can have as many pre-aggregations as they
require. The pre-aggregated data [can be stored either alongside the source data
in the same database, in an external database][ref-schema-preaggs-extvsint] that
is supported by Cube.js, [or in Cube Store, a dedicated pre-aggregation storage
layer][ref-caching-preaggs-cubestore].

## Pre-Aggregations without Time Dimension

To illustrate pre-aggregations with an example, let's use a sample e-commerce
database. We have a schema representing all our `Orders`:

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt],
    },
  },

  dimensions: {
    status: {
      sql: `status`,
      type: `string`,
    },

    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`,
    },
  },
});
```

Some sample data from this table might look like:

| **id** | **status** | **completed_at**        |
| ------ | ---------- | ----------------------- |
| 1      | completed  | 2021-02-15T12:21:11.290 |
| 2      | completed  | 2021-02-25T18:15:12.369 |
| 3      | shipped    | 2021-03-15T20:40:57.404 |
| 4      | processing | 2021-03-13T10:30:21.360 |
| 5      | completed  | 2021-03-10T18:25:32.109 |

Our first requirement is to populate a dropdown in our front-end application
which shows all possible statuses. The Cube.js query to retrieve this
information might look something like:

```json
{
  "dimensions": ["Orders.status"]
}
```

```javascript
cube(`Orders`, {
  // Same content as before, but including the following:
  preAggregations: {
    orderStatuses: {
      type: `rollup`,
      measureReferences: [status],
    },
  },
});
```

## Pre-Aggregations with Time Dimension

Using the same schema as before, we are now finding that users frequently query
for the number of orders completed per day, and that this query is performing
poorly. This query might look something like:

```json
{
  "measures": ["Orders.count"],
  "timeDimensions": ["Orders.completedAt"]
}
```

In order to improve the performance of this query, we can add another
pre-aggregation definition to the `Orders` schema:

```javascript
cube(`Orders`, {
  // Same content as before, but including the following:
  preAggregations: {
    ordersByCompletedAt: {
      type: `rollup`,
      measureReferences: [count],
      timeDimensionReference: completedAt,
      granularity: `month`,
    },
  },
});
```

Note that we have added a `granularity` property with a value of `month` to this
definition. [This allows Cube.js to aggregate the dataset to a single entry for
each month][ref-schema-preaggs-examples].

The next time the API receives the same JSON query, Cube.js will build (if it
doesn't already exist) the pre-aggregated dataset, store it in the source
database server and use that dataset for any subsequent queries. A sample of the
data in this pre-aggregated dataset might look like:

| **completed_at**        | **count** |
| ----------------------- | --------- |
| 2021-02-01T00:00:00.000 | 2         |
| 2021-03-01T00:00:00.000 | 3         |

## Keeping pre-aggregations up-to-date

Pre-aggregations can become out-of-date or out-of-sync if the original dataset
changes. [Cube.js uses a refresh key to check the freshness of the
data][ref-caching-preaggs-refresh]; if a change in the refresh key is detected,
the pre-aggregations are rebuilt.

These refreshes can be done on-demand, or [in the background as a scheduled
process][ref-caching-preaggs-bk-refresh].

[ref-schema-preaggs-examples]: /pre-aggregations#rollup-rollup-examples
[ref-caching-preaggs-cubestore]:
  /caching/using-pre-aggregations#pre-aggregations-storage
[ref-caching-preaggs-bk-refresh]:
  /caching/using-pre-aggregations#background-refresh
[ref-caching-preaggs-refresh]: /caching/using-pre-aggregations#refresh-strategy
[ref-schema-preaggs]: /pre-aggregations
[ref-schema-preaggs-extvsint]: /pre-aggregations#external-vs-internal
