---
title: Pre-aggregations
permalink: /schema/reference/pre-aggregations
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 8
redirect_from:
  - /pre-aggregations
---

<!-- prettier-ignore-start -->
[[info |]]
| To start building pre-aggregations, Cube.js requires write access to the
| [pre-aggregations schema][ref-config-preagg-schema] in the source database.
| Cube.js first builds pre-aggregations as tables in the source database and
| then exports them into the pre-aggregations storage.
<!-- prettier-ignore-end -->

Pre-aggregations are materialized query results persisted as tables. Cube.js has
an ability to analyze queries against a defined set of pre-aggregation rules in
order to choose the optimal one that will be used to create pre-aggregation
table.

If Cube.js finds a suitable pre-aggregation rule, database querying becomes a
multi-stage process:

1. Cube.js checks if an up-to-date copy of the pre-aggregation exists.

2. Cube.js will execute a query against the pre-aggregated tables instead of the
   raw data.

Pre-aggregations can be defined in the `preAggregations` available on each cube.

## Naming

Pre-aggregations must have, at minimum, a name and a type. This name, along with
the name of the cube will be used as a prefix for pre-aggregation tables created
in the database.

<!-- prettier-ignore-start -->
[[warning | ]]
| Some databases have trouble with long table names. You can work around this
| by specifying the [`sqlAlias`][ref-sqlalias] property on the cube and on
| the pre-aggregation definition.
<!-- prettier-ignore-end -->

Pre-aggregation names should:

- Be unique within a cube
- Start with a lowercase letter
- Consist of numbers, letters and `_`

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  preAggregations: {
    main: {
      sqlAlias: `original`,
      type: `originalSql`,
    },
  },
});
```

Pre-aggregations must include all dimensions, measures, and filters you will
query with.

## Rollup

Rollup pre-aggregations are the most effective way to boost performance of any
analytical application. The blazing fast performance of tools like Google
Analytics or Mixpanel are backed by a similar concept. The theory behind it lies
in multi-dimensional analysis and Rollup pre-aggregation is in fact the result
of [Roll-up Operation on a OLAP cube][wiki-olap-ops]. A rollup pre-aggregation
is essentially the summarized data of the original cube grouped by selected
dimensions of interest.

The most winning type of Rollup pre-aggregation is Additive Rollup: all measures
of which are based on [decomposable aggregate
functions][wiki-composable-agg-fn]. Additive measure types are: `count`, `sum`,
`min`, `max` or `countDistinctApprox`. The performance boost in this case is
based on two main properties of Additive Rollup pre-aggregations:

1. A rollup pre-aggregation table usually contains many fewer rows than its'
   corresponding original fact table. The fewer dimensions that are selected for
   roll-up means fewer rows in the materialized result. A smaller number of rows
   therefore means less time to query rollup pre-aggregation tables.

2. If your query is a subset of dimensions and measures of an additive rollup,
   then it can be used to calculate a query without accessing the raw data. The
   more dimensions and measures are selected for roll-up, the more queries can
   use this particular rollup.

Rollup definitions can contain members from a single cube as well as from
multiple cubes. In case of multiple cubes being involved, the join query will be
built according to the standard rules of cubes joining.

### Rollup selection rules

Rollups are selected based on the properties found in queries made to the
Cube.js REST API. A thorough explanation can be found under [Getting Started
with Pre-Aggregations][ref-caching-preaggs-target].

## Original SQL

As the name suggests, it persists the results of the `sql` property of the cube.
Pre-aggregations of type `originalSql` should **only** be used when the cube's
`sql` is a complex query (i.e. nested, window functions and/or multiple joins).
We **strongly** recommend only persisting results of `originalSql` back to the
source database i.e. [set `internal: true`][ref-caching-using-preaggs-internal].
They often do not provide much in the way of performance directly, but there are
two specific applications:

1. They can be used in tandem with the
   [`useOriginalSqlPreAggregations`][self-origsql-preaggs] option in other
   rollup pre-aggregations.

2. Situations where it is not possible to use a `rollup` pre-aggregations, such
   as [funnels][ref-schema-funnels].

For example, to pre-aggregate all completed orders, you could do the following:

```javascript
cube(`CompletedOrders`, {
  sql: `select * from orders where completed = true`,

  preAggregations: {
    main: {
      type: `originalSql`,
      internal: true,
    },
  },
});
```

## rollupJoin

<!-- prettier-ignore-start -->
[[warning | 🐣 &nbsp;&nbsp; Preview]]
| `rollupJoin` is currently in Preview, and the API may change in a
| future version.
<!-- prettier-ignore-end -->

Cube.js is capable of performing joins between separate pre-aggregations,
thereby avoiding a call to the source database. This functionality also allows
for cross-database joins; you can have a data schema for a MySQL database,
another for Postgres, and then use `rollupJoin` to join their pre-aggregations:

```javascript
// A schema representing all companies, retrieved from MySQL
cube(`Companies`, {
  dataSource: 'mysql',
  sql: `SELECT * from ecom.companies`,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    name: {
      sql: `name`,
      type: `string`,
      primaryKey: true,
      shown: true,
    },
  },

  preAggregations: {
    companiesRollup: {
      type: `rollup`,
      dimensionReferences: [Companies.name],
      external: true,
    },
  },
});

// A schema representing all users, retrieved from Postgres
cube('Users', {
  dataSource: 'postgres',
  sql: `select * from users`,
  joins: {
    Companies: {
      relationship: `belongsTo`,
      sql: `${CUBE}.company = ${Companies.name}`,
    },
  },
  measures: {
    count: {
      type: `count`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    company: {
      sql: `company`,
      type: `string`,
    },
  },
  preAggregations: {
    usersRollup: {
      type: `rollup`,
      measureReferences: [Users.count],
      dimensionReferences: [Users.company],
      external: true,
    },
    // Here we add a new pre-aggregation of type `rollupJoin`
    joinedWithCompaniesRollup: {
      type: `rollupJoin`,
      measureReferences: [Users.count],
      dimensionReferences: [Companies.name],
      rollupReferences: [Companies.companiesRollup, Users.usersRollup],
      external: true,
    },
  },
});
```

## refreshKey

Cube.js can also take care of keeping pre-aggregations up to date with the
`refreshKey` property. By default, it is set to `every: '1 hour'`. You can set
up a custom refresh check strategy by using `refreshKey`:

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders`,

  preAggregations: {
    main: {
      type: `rollup`,
      measureReferences: [Orders.count],
      refreshKey: {
        sql: `SELECT MAX(created_at) FROM orders`,
      },
    },
  },
});
```

As in the case of cube pre-aggregations, the `refreshKey` can define an `every`
property which can be used to refresh pre-aggregations based on a time interval.

<!-- prettier-ignore-start -->
[[warning | ]]
| The `every` parameter **does not** force Cube.js to fetch `refreshKey` based
| on an interval. It instead generates a SQL query whose result should change
| at least once per defined interval and adjusts `refreshKeyRenewalThreshold`
| accordingly. [Learn more][ref-cube-refreshkey].
<!-- prettier-ignore-end -->

For example:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  preAggregations: {
    main: {
      type: `originalSql`,
      refreshKey: {
        every: `1 day`,
      },
    },
  },
});
```

For possible `every` parameter values please refer to
[`refreshKey`][ref-cube-refreshkey] documentation.

## Incremental refresh

You can incrementally refresh partitioned rollups.

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
        updateWindow: `7 day`,
      },
    },
  },
});
```

The `incremental: true` flag generates a special `refreshKey` SQL query which
triggers a refresh for partitions where the end date lies within the
`updateWindow` from the current time. In the provided example, it will refresh
today's and the last 7 days of partitions once a day. Partitions before the
`7 day` interval **will not** be refreshed once they are built unless the rollup
SQL is changed.

Partition tables are refreshed as a whole. When new partition table is available
it replaces the old one. Old partition tables are collected by [Garbage
Collection][ref-garbage-collection]. Append is never used to add new rows to the
existing tables.

An original SQL pre-aggregation can also be used with time partitioning and
incremental `refreshKey`. It requires using `FILTER_PARAMS` inside the Cube's
`sql` property.

Below you can find an example of the partitioned `originalSql` pre-aggregation.

```javascript
cube(`Orders`, {
  sql: `select * from visitors WHERE ${FILTER_PARAMS.visitors.created_at.filter(
    'created_at'
  )}`,

  preAggregations: {
    main: {
      type: `originalSql`,
      timeDimensionReference: created_at,
      partitionGranularity: `month`,
      refreshKey: {
        every: `1 day`,
        incremental: true,
        updateWindow: `7 day`,
      },
    },
  },

Partition tables are refreshed as a whole. When a new partition table is
available, it replaces the old one. Old partition tables are collected by
[Garbage Collection][ref-caching-garbage-collection]. Append is never used to
add new rows to the existing tables.

## useOriginalSqlPreAggregations

Cube.js supports multi-stage pre-aggregations by reusing original SQL
pre-aggregations in rollups through the `useOriginalSqlPreAggregations`
property. It is helpful in cases where you want to re-use a heavy SQL query
calculation in multiple `rollup` pre-aggregations. Without
`useOriginalSqlPreAggregations` enabled, Cube.js will always re-execute all
underlying SQL calculations every time it builds new rollup tables.

<!-- prettier-ignore-start -->
[[warning |]]
| `originalSql` pre-aggregations **must only** be used when [storing
| pre-aggregations on the source database][ref-caching-using-preaggs-internal].
| This also means that `originalSql` pre-aggregations require
| [`readOnly: false`][ref-caching-readonly].
<!-- prettier-ignore-end -->

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
      type: `originalSql`,
    },
    category: {
      type: `rollup`,
      measureReferences: [Orders.count, revenue],
      dimensionReferences: [category],
      useOriginalSqlPreAggregations: true,
    },
    date: {
      type: `rollup`,
      measureReferences: [Orders.count],
      timeDimensionReference: date,
      granularity: `day`,
      useOriginalSqlPreAggregations: true,
    },
  },
});
```

## scheduledRefresh

To always keep pre-aggregations up-to-date, you can mark them as
`scheduledRefresh: true`. Without this flag, pre-aggregations are always built
on-demand. The `refreshKey` is used to determine if there's a need to update
specific pre-aggregations on each scheduled refresh run. For partitioned
pre-aggregations, `min` and `max` dates for `timeDimensionReference` are checked
to determine range for the refresh.

Each time a scheduled refresh is run, it takes every pre-aggregation partition
starting with most recent ones in time and checks if its `refreshKey` has
changed. If a change was detected, then that partition will be refreshed.

In development mode, Cube.js runs the background refresh by default and will
refresh all the pre-aggregations marked with `scheduledRefresh` parameter.

Please consult [Production Checklist][ref-production-checklist-refresh] for best
practices on running background refresh in production environments.

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
      scheduledRefresh: true,
    },
  },
});
```

## buildRangeStart and buildRangeEnd

The build range defines what partitions should be built by a scheduled refresh.
Scheduled refreshes will **never** look beyond this range.

It can be used together with `updateWindow` to define granular update settings.
Set the `updateWindow` property to the interval in which your data can change
and `buildRangeStart` to the earliest point of time when history should be
available. For example if `updateWindow` is `1 week` and `buildRangeStart` is
`SELECT NOW() - interval '365 day'` scheduled refresh will build historic
partitions for 365 days in past and will refresh only one last week according to
the `refreshKey` setting.

The refresh range for partitioned pre-aggregations can be controlled using
`buildRangeStart` and `buildRangeEnd` properties:

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
      buildRangeStart: {
        sql: `SELECT NOW() - interval '300 day'`,
      },
      buildRangeEnd: {
        sql: `SELECT NOW()`,
      },
    },
  },
});
```

## Indexes

In case of pre-aggregation tables having significant cardinality, you might want
to create indexes for them in databases which support it. This is can be done as
follows:

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
          columns: [category],
        },
      },
    },
  },
});
```

For `originalSql` pre-aggregations, the original column names as strings can be
used:

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  // ...

  preAggregations: {
    main: {
      type: `originalSql`,
      indexes: {
        time: {
          columns: ['timestamp'],
        },
      },
    },
  },
});
```

[ref-caching-garbage-collection]:
  /caching/using-pre-aggregations#caching-garbage-collection
[ref-caching-preaggs-target]:
  /caching/pre-aggregations/getting-started#ensuring-pre-aggregations-are-targeted-by-queries
[ref-caching-readonly]: /caching/using-pre-aggregations#read-only-data-source
[ref-caching-using-preaggs-internal]:
  /caching/using-pre-aggregations#pre-aggregations-storage
[ref-connect-db-ext]:
  /connecting-to-the-database#external-pre-aggregations-database
[ref-config-driverfactory]: /config/#options-reference-driver-factory
[ref-config-preagg-schema]: /config#options-reference-pre-aggregations-schema
[ref-cube-refreshkey]: /schema/reference/cube#parameters-refresh-key
[ref-production-checklist-refresh]:
  /deployment/production-checklist#set-up-refresh-worker
[ref-sqlalias]: /schema/reference/cube#parameters-sql-alias
[ref-schema-funnels]: /funnels
[self-origsql-preaggs]: #use-original-sql-pre-aggregations
[wiki-olap-ops]: https://en.wikipedia.org/wiki/OLAP_cube#Operations
[wiki-composable-agg-fn]:
  https://en.wikipedia.org/wiki/Aggregate_function#Decomposable_aggregate_functions
