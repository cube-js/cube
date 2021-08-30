---
title: Data Schema Concepts
permalink: /schema/fundamentals/concepts
category: Data Schema
subCategory: Fundamentals
menuOrder: 2
---

Cube.js borrows a lot of terminology from data science and [OLAP
theory][wiki-olap], and this document is intended for both newcomers and regular
users to refresh their understanding.

We'll use a sample e-commerce database with two tables, `orders` and
`line_items` to illustrate the concepts throughout this page:

**`orders`**

| **id** | **status** | **completed_at**           | **created_at**             |
| ------ | ---------- | -------------------------- | -------------------------- |
| 1      | completed  | 2019-01-05 00:00:00.000000 | 2019-01-02 00:00:00.000000 |
| 2      | shipped    | 2019-01-17 00:00:00.000000 | 2019-01-02 00:00:00.000000 |
| 3      | completed  | 2019-01-27 00:00:00.000000 | 2019-01-02 00:00:00.000000 |
| 4      | shipped    | 2019-01-09 00:00:00.000000 | 2019-01-02 00:00:00.000000 |
| 5      | processing | 2019-01-29 00:00:00.000000 | 2019-01-02 00:00:00.000000 |

**`line_items`**

| **id** | **product_id** | **order_id** | **quantity** | **price** | **created_at**             |
| ------ | -------------- | ------------ | ------------ | --------- | -------------------------- |
| 1      | 31             | 1            | 1            | 275       | 2019-01-31 00:00:00.000000 |
| 2      | 49             | 2            | 6            | 248       | 2021-01-20 00:00:00.000000 |
| 3      | 89             | 3            | 6            | 197       | 2021-11-25 00:00:00.000000 |
| 4      | 71             | 4            | 8            | 223       | 2019-12-23 00:00:00.000000 |
| 5      | 64             | 5            | 5            | 75        | 2019-04-20 00:00:00.000000 |

## Cubes

A cube represents a dataset in Cube.js, and is conceptually similar to a [view
in SQL][wiki-view-sql]. Cubes are typically declared in separate files with one
cube per file. Within each cube are definitions of [dimensions][self-dimensions]
and [measures][self-measures]. Typically, a cube points to a single table in
your database using the [`sql` property][ref-schema-ref-sql]:

```javascript
cube('Orders', {
  sql: `SELECT * FROM orders`,
});
```

The `sql` property of a cube is flexible enough to accommodate more complex SQL
queries too:

```javascript
cube('Orders', {
  sql: `
SELECT
  *
FROM
  orders,
  line_items
WHERE
  orders.id = line_items.order_id
  `,
});
```

## Dimensions

Dimensions represent the properties of a **single** data point in the cube.
[The `orders` table](#top) contains only dimensions, so representing them in the
`Orders` cube is straightforward:

```javascript
cube('Orders', {

  ...,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      // Here we explicitly let Cube.js know this field is the primary key
      // This is required for de-duplicating results when using joins
      primaryKey: true
    },
    status: {
      sql: `status`,
      type: `string`
    },
  },
});
```

[The `line_items` table](#top) also has a couple of dimensions which can be
represented as follows:

```javascript
cube('LineItems', {

  ...,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      // Again, we explicitly let Cube.js know this field is the primary key
      // This is required for de-duplicating results when using joins
      primaryKey: true
    },
  },
});
```

Dimensions can be of different types, and you can find them all
[here][ref-schema-dimension-types].

### Time Dimensions

Time-based properties should be represented as dimensions with type `time`. Time
dimensions allow grouping the result set by a unit of time (e.g. hours, days,
weeks). In analytics, this is also known as "granularity".

We can add the necessary time dimensions to both schemas as follows:

```javascript
cube('Orders', {

  ...,

  dimensions: {

    ...,

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`,
    },
  },
});
```

```javascript
cube('LineItems', {

  ...,

  dimensions: {

    ...,

    createdAt: {
      sql: `created_at`,
      type: `time`
    },
  },
});
```

Time dimensions are essential to enabling performance boosts such as
[partitioned pre-aggregations][ref-caching-use-preaggs-partition-time] and
[incremental refreshes][ref-tutorial-incremental-preagg].

## Measures

Measures represent the properties of a **set of data points** in the cube. To
add a measure called `count` to our `Orders` cube, for example, we can do the
following:

```javascript
cube('Orders', {

  ...,

  measures: {
    count: {
      type: `count`,
    },
  },
});
```

In our `LineItems` cube, we can also create a measure to sum up the total value
of line items sold:

```javascript
cube('LineItems', {

  ...,

  measures: {
    total: {
      sql: `price`,
      type: `sum`,
    },
  },
})
```

Measures can be of different types, and you can find them all
[here][ref-schema-measure-types].

## Joins

Joins define the relationships between cubes, which then allows accessing and
comparing properties from two or more cubes at the same time. In Cube.js, all
joins are `LEFT JOIN`s.

<!-- prettier-ignore-start -->
[[info | ]]
| An `INNER JOIN` can be replicated with Cube.js; when making a Cube.js query,
| add a filter for `IS NOT NULL` on the required column.
<!-- prettier-ignore-end -->

In the following example, we are left-joining the `LineItems` cube onto our
`Orders` cube:

```javascript
cube('Orders', {

  ...,

  joins: {
    LineItems: {
      relationship: `belongsTo`,
      // Here we use the `CUBE` global to refer to the current cube,
      // so the following is equivalent to `Orders.id = LineItems.order_id`
      sql: `${CUBE}.id = ${LineItems}.order_id`,
    },
  },
});
```

There are three kinds of join relationships:

- `belongsTo`
- `hasOne`
- `hasMany`

More information can be found in the [Joins reference
documentation][ref-schema-ref-joins-types].

## Segments

Segments are filters that are predefined in the schema instead of [a Cube.js
query][ref-backend-query-filters]. They allow simplifying Cube.js queries and
make it easy to re-use common filters across a variety of queries.

To add a segment which limits results to completed orders, we can do the
following:

```javascript
cube('Orders', {
  ...,
  segments: {
    onlyCompleted: {
      sql: `${CUBE}.status = 'completed'`
    },
  },
});
```

## Pre-Aggregations

Pre-aggregations are a powerful way of caching frequently-used, expensive
queries and keeping the cache up-to-date on a periodic basis. Within a data
schema, they are defined under the `preAggregations` property:

```javascript
cube('Orders', {

  ...,

  preAggregations: {
    main: {
      measures: [CUBE.count],
      dimensions: [CUBE.status],
      timeDimension: CUBE.createdAt,
      granularity: 'day',
    },
  },
});
```

A more thorough introduction can be found in [Getting Started with
Pre-Aggregations][ref-caching-preaggs-intro].

[ref-backend-query-filters]: /query-format#filters-format
[ref-caching-preaggs-intro]: /caching/pre-aggregations/getting-started
[ref-caching-use-preaggs-partition-time]:
  /caching/using-pre-aggregations#partitioning-time-partitioning
[ref-schema-dimension-types]:
  /docs/schema/reference/types-and-formats#dimensions-types
[ref-schema-measure-types]:
  /docs/schema/reference/types-and-formats#measures-types
[ref-schema-ref-joins-types]: /schema/reference/joins#parameters-relationship
[ref-schema-ref-sql]: /schema/reference/cube#parameters-sql
[ref-tutorial-incremental-preagg]: /incremental-pre-aggregations
[self-dimensions]: #dimensions
[self-measures]: #measures
[wiki-olap]: https://en.wikipedia.org/wiki/Online_analytical_processing
[wiki-view-sql]: https://en.wikipedia.org/wiki/View_(SQL)
