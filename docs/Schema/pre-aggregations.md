---
title: Pre-aggregations
permalink: /pre-aggregations
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 8
---

Pre-aggregations are materialized query results persisted as tables.
In order to start using pre-aggregations Cube.js should have write access to `stb_pre_aggregations` schema where pre-aggregation tables will be stored.
Cube.js has an ability to analyze queries against defined set of pre-aggregation rules in order to choose optimal one that will be used to create pre-aggregation table.

If Cube.js finds suitable pre-aggregation rule database querying becomes multi-stage.
First it checks if up-to-date copy of pre-aggregation exists.
If not found or outdated it'll create new pre-aggregation table.
As a second step Cube.js will issue query against pre-aggregated tables instead of querying raw data.

Pre-aggregation rules are defined by `preAggregations` cube parameter.
Any pre-aggregation should have name and type.
Pre-aggregation name together with cube name will be used as a prefix for pre-aggregation tables.

Pre-aggreations names should:
- Be unique within a cube
- Start with a lowercase letter

You can use `0-9`,`_` and letters when naming pre-aggregations.

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
Blazing fast performance of tools like Google Analytics or Mixpanel backed by similar concept.
Theory behind it lies in multi-dimensional analysis and Rollup pre-aggregation is in fact result of [Roll-up Operation on a OLAP cube](https://en.wikipedia.org/wiki/OLAP_cube#Operations).
Rollup pre-aggregation is basically summarized data of original cube grouped by selected dimensions of interest.

The most winning type of Rollup pre-aggregation is Additive Rollup: all measures of which are based on [Decomposable aggregate functions](https://en.wikipedia.org/wiki/Aggregate_function#Decomposable_aggregate_functions).
Additive measure types are: `count`, `sum`, `min`, `max` or `countDistinctApprox`.
Performance boost in this case is based on two main properties of Additive Rollup pre-aggregation:
1. Rollup pre-aggregation table usually contains much less rows than an original fact table.
Less dimensions you selected to roll-up less rows you get.
Less rows means less time to query Rollup pre-aggregation tables.
2. If your query is in fact subset of dimensions and measures of Additive Rollup then it can be used to calculate such query without accessing raw data.
More dimensions and measures you select to roll-up more queries you can cover with this particular Rollup.

### Rollup selection rules

Rollup pre-aggregation defines set of measures and dimensions used to construct query for pre-aggregation table.
Each query issued against cube where pre-aggregation is defined will be checked if specific rollup pre-aggregation can be used by following algorithm:
1. Determine type of a query as one of *Additive*, *Leaf Measure Additive*, *Not Additive*.
2. If query is *Additive* check if rollup contains all dimensions, filter dimensions and measures used in query.
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

These queries won't use `categoryAndDate` pre-aggregation:
- Order Revenue by Created At Week this year
- Order Revenue by Created At Month this year
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
Partitioned rollups currently cannot be used without time dimensions. 

## Auto Rollup

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

## External Rollup

You should use this option for scenarios where you need to handle high throughput for big data backend.
It allows to download rollups prepared in big data backends such as AWS Athena, BigQuery, Presto, Hive and others to low latency databases such as MySQL for actual querying.
While big data backends aren't very suitable for handling massive amounts of concurrent queries even on pre-aggregated data most of single node RDBMS can do it very well if cardinality of data not so big.
Leveraging this nuance allows to create setup where you can query huge amounts of data with subsecond response times at cost of rollup download time.

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
