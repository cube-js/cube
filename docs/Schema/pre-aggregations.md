---
title: Pre-aggregations
permalink: /pre-aggregations
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 8
---

Pre-aggregations are materialized query results persisted as tables.
In order to start using pre-aggregations Cube.js should have write access to `stb_pre_aggregations` schema, or whatever your [preAggregationsSchema](@cubejs-backend-server-core#options-reference-pre-aggregations-schema) setting is set to, where pre-aggregation tables will be stored.
Cube.js has an ability to analyze queries against defined set of pre-aggregation rules in order to choose optimal one that will be used to create pre-aggregation table.

If Cube.js finds suitable pre-aggregation rule database querying becomes multi-stage.
First it checks if up-to-date copy of pre-aggregation exists.
If not found or outdated it'll create new pre-aggregation table.
As a second step Cube.js will issue query against pre-aggregated tables instead of querying raw data.

Pre-aggregation rules are defined by `preAggregations` cube parameter.
Any pre-aggregation should have name and type.
Pre-aggregation name together with cube name will be used as a prefix for pre-aggregation tables.

Pre-aggregations names should:
- Be unique within a cube
- Start with a lowercase letter

You can use `0-9`,`_` and letters when naming pre-aggregations.

Pre-aggregations must include all dimensions, measures, and filters you will query with. They will not join to other cubes at query-time.

## Original SQL

Original SQL pre-aggregation is a simplest type of pre-aggregation.
As the name states it just persists SQL of a cube where it's defined.

For example you can materialize all completed `Orders` as following:
```javascript
cube(`Orders`, {
  sql: `select * from orders where completed = true`,

  preAggregations: {
    main: {
      type: `originalSql`
    }
  }
});
```

## Rollup

Rollup pre-aggregations are most effective way to boost performance of any analytical application.
Blazing fast performance of tools like Google Analytics or Mixpanel are backed by a similar concept.
The theory behind it lies in multi-dimensional analysis and Rollup pre-aggregation is in fact the result of [Roll-up Operation on a OLAP cube](https://en.wikipedia.org/wiki/OLAP_cube#Operations).
Rollup pre-aggregation is basically summarized data of the original cube grouped by selected dimensions of interest.

The most winning type of Rollup pre-aggregation is Additive Rollup: all measures of which are based on [Decomposable aggregate functions](https://en.wikipedia.org/wiki/Aggregate_function#Decomposable_aggregate_functions).
Additive measure types are: `count`, `sum`, `min`, `max` or `countDistinctApprox`.
Performance boost in this case is based on two main properties of Additive Rollup pre-aggregation:
1. Rollup pre-aggregation table usually contains many fewer rows than an original fact table.
Less dimensions you selected to roll-up means less rows you get.
Less rows means less time to query Rollup pre-aggregation tables.
2. If your query is in fact a subset of dimensions and measures of Additive Rollup then it can be used to calculate such query without accessing raw data.
More dimensions and measures you select to roll-up more queries you can cover with this particular Rollup.

### Rollup selection rules

Rollup pre-aggregation defines a set of measures and dimensions used to construct the query for pre-aggregation table.
Each query issued against cube where pre-aggregation is defined will be checked if specific rollup pre-aggregation can be used by following algorithm:
1. Determine the type of a query as one of *Additive*, *Leaf Measure Additive*, *Not Additive*.
2. If the query is *Additive* check if rollup contains all dimensions, filter dimensions and measures used in query.
3. If query is *Leaf Measure Additive* check if rollup contains all dimensions, filter dimensions and *Leaf Measures* used in query.
4. If query is *Not Additive* check if query time dimension granularity is set, all query filter dimensions are included in query dimensions, rollup defines exact set of dimensions used in query and rollup contains all measures used in query.

Here:
- Query is *Additive* if all of it's measures are either `count`, `sum`, `min`, `max` or `countDistinctApprox` type.
- Query is *Leaf Measure Additive* if all of it's *Leaf Measures* are either `count`, `sum`, `min`, `max` or `countDistinctApprox` type.
- Query is *Not Additive* if it's not *Additive* and not *Leaf Measure Additive*.
- *Leaf Measures* are measures that do not reference any other measures in it's definition.
- Time dimension together with granularity constitute dimension.

### Rollup examples

There're two types of definitions allowed for rollup pre-aggregation: with or without time dimension.

Let's consider an example:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  measures: {
    count: {
      type: `count`
    },

    revenue: {
      sql: `amount`,
      type: `sum`
    },
    
    averageRevenue: {
      sql: `${revenue} / ${count}`,
      type: `number`
    }
  },

  dimensions: {
    category: {
      sql: `category`,
      type: `string`
    },

    customerName: {
      sql: `customer_name`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },

  preAggregations: {
    categoryAndDate: {
      type: `rollup`,
      measureReferences: [Orders.count, revenue],
      dimensionReferences: [category],
      timeDimensionReference: createdAt,
      granularity: `day`
    }
  }
});
```

Granularity can be either `hour`, `day`, `week` or `month`.
If `timeDimensionReference` is set, `granularity` should be set as well otherwise it should be omitted.

In this particular example these queries will use `categoryAndDate` pre-aggregation:
- Order Revenue by Category this month
- Order Count by Created At Day this year
- Order Count for all time
- Order Average Revenue by Category this month
- Order Revenue by Created At Week this year
- Order Revenue by Created At Month this year

These queries won't use `categoryAndDate` pre-aggregation:
- Order Count by Customer Name this year

### Time partitioning

Any rollup and auto rollup pre-aggregation can be partitioned by time using `partitionGranularity` property:

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
      partitionGranularity: `month`
    }
  }
});
```

`partitionGranularity` can be either `day`, `week` or `month`.
For example if `partitionGranularity` is set to `month` Cube.js will generate separate `rollup` table for each month.
This can reduce rollup refreshing time and cost significantly.
Partitioned rollups currently cannot be used by queries without time dimensions.


### Segment Partitioning

Any rollup can be auto-filtered to some segments by using the `segmentReferences` property:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  segments: {
    toys: {
        sql: `category = 'toys'`
    }
  },

  preAggregations: {
    categoryAndDate: {
      type: `rollup`,
      measureReferences: [Orders.count, revenue],
      segmentReferences: [toys],
      timeDimensionReference: createdAt,
      granularity: `day`,
      partitionGranularity: `month`
    }
  }
});
```

## Auto Rollup

**Enterprise Feature Only**

Auto rollup is an extension to rollup which instructs Cube.js to select rollup measures and dimensions at query time.
Cube.js uses query history to select optimal set of measures and dimensions for a given query.

You can set it up as following:
```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  preAggregations: {
    main: {
      type: `autoRollup`
    }
  }
});
```

You can also limit number of rollup tables that will be created using `maxPreAggregations` property:
```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  preAggregations: {
    main: {
      type: `autoRollup`,
      maxPreAggregations: 20
    }
  }
});
```

`maxPreAggregations` sets trade-off between initial waiting time and average response times. More rollup tables you have more time is required to refresh them. On other hand more granular rollup tables reduce average response times. In some cases column count in rollup can affect it's refresh performance as well.

## External Pre-Aggregations

You should use this option for scenarios where you need to handle high throughput for big data backend.
It allows to download rollups and original sql pre-aggregations prepared in big data backends such as AWS Athena, BigQuery, Presto, Hive and others to low latency databases such as MySQL for actual querying.
While big data backends aren't very suitable for handling massive amounts of concurrent queries even on pre-aggregated data most of single node RDBMS can do it very well if cardinality of data not so big.
Leveraging this nuance allows to create setup where you can query huge amounts of data with subsecond response times at cost of pre-aggregation download time.

To setup it just add `external` param to your pre-aggregation:

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
      external: true
    }
  }
});
```

In order to make external pre-aggregations work you should set
[externalDriverFactory](@cubejs-backend-server-core#external-driver-factory) and [externalDbType](@cubejs-backend-server-core#external-db-type) params while creating your server instance.

## refreshKey

Cube.js also takes care of keeping pre-aggregations up to date. Every two minutes on a new request Cube.js will initiate the refresh check.

By default pre-aggregations are refreshed **every hour**.

You can set up a custom refresh check strategy by using `refreshKey`.

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  preAggregations: {
    main: {
      type: `autoRollup`,
      maxPreAggregations: 20,
      refreshKey: {
        sql: `SELECT MAX(created_at) FROM orders`
      }
    }
  }
});
```

## scheduledRefresh

To keep pre-aggregations always up-to-date you can mark them as `scheduledRefresh: true`.
This instructs `RefreshScheduler` to refresh this pre-aggregation every time it's run.
`refreshKey` is used to determine if there's a need to update specific pre-aggregation on each scheduled refresh run.
For partitioned pre-aggregations `min` and `max` dates for `timeDimensionReference` are fetched to determine range for refresh.

> **NOTE:** Refresh Scheduler isn't enabled by default. You should trigger it externally. [Learn how to do it here](caching#keeping-cache-up-to-date).

Example usage:
```javascript
cube(`Orders`, {
  sql: `select * from orders`,
  
  // ...

  preAggregations: {
    categoryAndDate: {
      type: `rollup`,
      measureReferences: [Orders.count, revenue],
      timeDimensionReference: createdAt,
      granularity: `day`,
      partitionGranularity: `month`,
      scheduledRefresh: true
    }
  }
});
```

