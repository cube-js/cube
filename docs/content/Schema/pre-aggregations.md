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
- For long pre-aggregations names, you can set the `sqlAlias` attribute

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

Rollup definitions can contain members from a single cube as well as from multiple cubes.
In case of multiple cubes are involved, join query will be built according to standard rules of cubes joining.

### Rollup selection rules

Rollup pre-aggregation defines a set of measures and dimensions used to construct the query for pre-aggregation table.
Each query issued against cube where pre-aggregation is defined will be checked if specific rollup pre-aggregation can be used by following algorithm:
1. Determine the type of a query as one of *Leaf Measure Additive* or *Not Additive*.
2. If query is *Leaf Measure Additive* check if rollup contains all dimensions, filter dimensions and *Leaf Measures* are used in query and measures aren't multiplied.
3. If query is *Not Additive* check if query time dimension granularity is set, all query filter dimensions are included in query dimensions, rollup defines exact set of dimensions used in query and rollup contains all measures used in query.

Here:
- Query is *Leaf Measure Additive* if all of it's *Leaf Measures* are either `count`, `sum`, `min`, `max` or `countDistinctApprox` type.
- Query is *Not Additive* if it's not *Additive* and not *Leaf Measure Additive*.
- *Leaf Measures* are measures that do not reference any other measures in it's definition.
- Time dimension together with granularity constitute dimension. If date range isn't aligned with granularity common granularity is used. To match granularity date range should match it's start and end. For example for month it's `['2020-01-01T00:00:00.000', '2020-01-31T23:59:59.999']` and for day it's `['2020-01-01T00:00:00.000', '2020-01-01T23:59:59.999']`. Date ranges are inclusive. Minimum granularity is `second`.
- Multiplied measures are measures of cubes that define `hasMany` relation involved in pre-aggregation definition join.

Also order of pre-aggregations definition in cube matters.
First matched pre-aggregation wins. 
Cubes of a measures and then cubes of dimensions are checked to find a matching `rollup`.
However `rollup` pre-aggregations always have priority over `originalSql`.
Thus if you have both `originalSql` and `rollup` defined, Cube.js will try to find matching `rollup` before trying to find matching `originalSql`.
More over you can instruct Cube.js to use original sql pre-aggregations using [useOriginalSqlPreAggregations](#use-original-sql-pre-aggregations).

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
[externalDriverFactory](@cubejs-backend-server-core#external-driver-factory) and [externalDbType](@cubejs-backend-server-core#external-db-type) params while creating your server instance. Also, you can set these params via [environment variables](connecting-to-the-database#configuring-a-connection-to-an-external-database).

Note that by default, Cube.js materializes the pre-aggregration query results as new tables in the source database. For external pre-aggregations, these source tables are temporary - once downloaded and uploaded to the external database, they are cleaned-up.

## Read Only Data Source Pre-Aggregations

In some cases it may not be possible to stage pre-aggregation query results in materialized tables in the source database like this - for example, if the driver doesn't support it, or if your source database is read-only. To fallback to a strategy where the pre-aggreation query results are downloaded without first being materialized, set the `readOnly` param of [driverFactory](@cubejs-backend-server-core#driver-factory) in your configuration:

```javascript
const CubejsServer = require('@cubejs-backend/server');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

const options = {
  driverFactory: () => new PostgresDriver({
    readOnly: true
  }),
  externalDbType: 'postgres',
  externalDriverFactory: () => new PostgresDriver({
    host: 'my_host',
    database: 'my_db',
    user: 'my_user',
    password: 'my_pw'
  })
};
```

## refreshKey

Cube.js also takes care of keeping pre-aggregations up to date.

By default pre-aggregations use same `refreshKey` as it's cube defines.

You can set up a custom refresh check strategy by using `refreshKey`:

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

As in case of cube pre-aggregations `refreshKey` can define `every` parameter which can be used to refresh pre-aggregations based on time interval.

[[warning | Attention]]
| `every` parameter doesn't force Cube.js to fetch `refreshKey` based on it's interval. It generates SQL which result set change at least once per defined interval and adjusts `refreshKeyRenewalThreshold` accordingly. [Learn more](cube#parameters-refresh-key).

For example:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  preAggregations: {
    main: {
      type: `originalSql`,
      refreshKey: {
        every: `1 day`
      }
    }
  }
});
```

For possible `every` parameter values please refer to [refreshKeys](cube#parameters-refresh-key).

In case of partitioned rollups incremental `refreshKey` can be used as follows:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  // ...

  preAggregations: {
    main: {
      type: `rollup`,
      measureReference: [count],
      timeDimensionReference: createdAt,
      granularity: `day`,
      partitionGranularity: `day`,
      refreshKey: {
        every: `1 day`,
        incremental: true,
        updateWindow: `7 day`
      }
    }
  }
});
```

`incremental: true` flag generates special `refreshKey` SQL.
It triggers refresh for partitions where end date lies within `updateWindow` from current time.
In provided example it'll refresh today's and last 7 days of partitions.
Partitions before `7 day` interval won't be refreshed once they built until rollup SQL is changed.

### Original SQL with incremental refreshKey

Original SQL pre-aggregation can be used with time partitioning and incremental `refreshKey`.

In this case, it can be used as follows:
```javascript
cube(`Orders`, {
  sql: `select * from visitors WHERE ${FILTER_PARAMS.visitors.created_at.filter('created_at')}`,

  preAggregations: {
    main: {
      type: `originalSql`,
      timeDimensionReference: created_at,
      partitionGranularity: `month`,
      refreshKey: {
        every: `1 day`,
        incremental: true,
        updateWindow: `7 day`
      }
    }
  },

  dimensions: {
    id: {
      type: 'number',
      sql: 'id',
      primaryKey: true
    }, 
    created_at: {
      type: 'time',
      sql: 'created_at'
    },
  }
});
```

## useOriginalSqlPreAggregations

Cube.js supports multi-stage pre-aggregations by reusing original sql pre-aggregations in rollups through `useOriginalSqlPreAggregations` param.
It's helpful in case you want to re-use some heavy SQL query calculation in multiple rollups.
Without `useOriginalSqlPreAggregations` set to `true` Cube.js will always redo all underlying SQL calculations every time it builds new rollup table.

```javascript
cube(`Orders`, {
  sql: `
    select * from orders1
    UNION ALL
    select * from orders2
    UNION ALL
    select * from orders3
    `,

  // ...

  preAggregations: {
    main: {
      type: `originalSql`
    },
    category: {
      type: `rollup`,
      measureReferences: [Orders.count, revenue],
      dimensionReferences: [category],
      useOriginalSqlPreAggregations: true
    },
    date: {
      type: `rollup`,
      measureReferences: [Orders.count],
      timeDimensionReference: date,
      granularity: `day`
      useOriginalSqlPreAggregations: true
    }
  }
});
```

## scheduledRefresh

To keep pre-aggregations always up-to-date you can mark them as `scheduledRefresh: true`.
This instructs `RefreshScheduler` to refresh this pre-aggregation every time it's run.
Without this flag pre-aggregations are always built on-demand.
`refreshKey` is used to determine if there's a need to update specific pre-aggregation on each scheduled refresh run.
For partitioned pre-aggregations `min` and `max` dates for `timeDimensionReference` are fetched to determine range for refresh.

[[warning | Note]]
| Refresh Scheduler isn't enabled by default. You should trigger it externally. [Learn how to do it here](caching#keeping-cache-up-to-date).

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

Refresh range for partitioned pre-aggregations can be controlled using `refreshRangeStart` and `refreshRangeEnd` params:

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
      scheduledRefresh: true,
      refreshRangeStart: {
        sql: `SELECT NOW() - interval '300 day'`
      },
      refreshRangeEnd: {
        sql: `SELECT NOW()`
      }
    }
  }
});
```

## Indexes

In case of pre-aggregation table has quite significant cardinality you might want to create indexes for such pre-aggregation in databases which support it.
This is can be done as following:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  // ...

  preAggregations: {
    categoryAndDate: {
      type: `rollup`,
      measureReferences: [Orders.count, revenue],
      dimensionReferences: [category],
      timeDimensionReference: createdAt,
      granularity: `day`,
      indexes: {
        main: {
          columns: [category]
        }
      }
    }
  }
});
```

For `originalSql` pre-aggregations original column names as strings can be used:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  // ...

  preAggregations: {
    main: {
      type: `originalSql`,
      indexes: {
        time: {
          columns: ['timestamp']
        }
      }
    }
  }
});
```

## Pre-aggregations Garbage Collection

When pre-aggregations are refreshed Cube.js will create new pre-aggregation table each time it's version change.
It allows to seamlessly hot swap tables transparently for users for any database even for those without DDL transactions support.
It leads to orphaned tables which need to be collected over time though.
By default Cube.js will store all content versions for 10 minutes and all structure versions for 7 days. 
Then it'll retain only the most recent ones and orphaned tables are dropped from database.

