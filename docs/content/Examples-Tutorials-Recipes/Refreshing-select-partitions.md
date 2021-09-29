---
title: Refreshing Select Partitions
permalink: /recipes/refreshing-select-partitions
category: Examples & Tutorials
subCategory: Query acceleration
menuOrder: 6
---

## Use case

We have a dataset with orders and we want to aggregate data while having
decent performance. Orders have a created time, so we can use
[partitioning](https://cube.dev/docs/caching/using-pre-aggregations#partitioning)
by time to optimize pre-aggregations build and refresh time. The problem is the
order can change status after a long time. In this case we want to rebuild only
partitions associated with this order.

In the recipe below, we'll learn how to use the
[`refreshKey`](https://cube.dev/docs/schema/reference/pre-aggregations#parameters-refresh-key-sql)
together with the
[`FITER_PARAMS`](https://cube.dev/docs/schema/reference/cube#filter-params) for
partition separately.

## Data schema

Let's explore the `Orders` cube data that contains various information about
orders, including number and status:

| id  | number | status     | created_at          | updated_at          |
| --- | ------ | ---------- | ------------------- | ------------------- |
| 1   | 1      | processing | 2021-08-10 14:26:40 | 2021-08-10 14:26:40 |
| 2   | 2      | completed  | 2021-08-20 13:21:38 | 2021-08-22 13:10:38 |
| 3   | 3      | shipped    | 2021-09-01 10:27:38 | 2021-09-02 01:12:38 |
| 4   | 4      | completed  | 2021-09-20 10:27:38 | 2021-09-20 10:27:38 |
| ... | ...    | ...        | ...                 | ...                 |

In our case, each order has `created_at` and `update_at` properties. The
`update_at` property is the last order update timestamp. To create a
pre-aggregation splitting to partitions, we need to specify the
[`partitionGranularity` property](https://cube.dev/docs/schema/reference/pre-aggregations#partition-granularity).
Partitions will be split monthly by the `created_at` dimension.

```javascript
preAggregations: {
    orders: {
      type: `rollup`,
      external: true,
      dimensions: [CUBE.number, CUBE.status, CUBE.createdAt, CUBE.updatedAt],
      timeDimension: CUBE.createdAt,
      granularity: `day`,
      partitionGranularity: `month`,
      refreshKey: {
        sql: `SELECT max(updated_at) FROM public.orders`
      },
    },
  },
```

As you can see, we defined custom
[`refreshKey`](https://cube.dev/docs/schema/reference/pre-aggregations#parameters-refresh-key-sql)
that will check a new value of the `updated_at` property. The refresh key is
evaluated for each partition separately. For example, if we will update order
from august and update their `update_at` property, the current refresh key will
update **for all partitions**. There is how it looks in the Cube logs:

```bash
Executing SQL: 5b4c517f-b496-4c69-9503-f8cd2b4c73b6
--
  SELECT max(updated_at) FROM public.orders
--
Performing query completed: 5b4c517f-b496-4c69-9503-f8cd2b4c73b6 (15ms)
Performing query: 5b4c517f-b496-4c69-9503-f8cd2b4c73b6
Performing query: 5b4c517f-b496-4c69-9503-f8cd2b4c73b6
Executing SQL: 5b4c517f-b496-4c69-9503-f8cd2b4c73b6
--
  select min(("orders".created_at::timestamptz AT TIME ZONE 'UTC')) from public.orders AS "orders"
--
Executing SQL: 5b4c517f-b496-4c69-9503-f8cd2b4c73b6
--
  select max(("orders".created_at::timestamptz AT TIME ZONE 'UTC')) from public.orders AS "orders"
--
```

Note that the query for two partitions is the same. It's the reason why **all
partitions** will be update.

Well, how to fix this and update only august partition? We can use the
[`FITER_PARAMS`](https://cube.dev/docs/schema/reference/cube#filter-params) for
that!

Let's update our pre-aggregation definition:

```javascript
preAggregations: {
    orders: {
      type: `rollup`,
      external: true,
      dimensions: [CUBE.number, CUBE.status, CUBE.createdAt, CUBE.updatedAt],
      timeDimension: CUBE.createdAt,
      granularity: `day`,
      partitionGranularity: `month`,
      refreshKey: {
        sql: `SELECT max(updated_at) FROM public.orders WHERE ${FILTER_PARAMS.Orders.createdAt.filter('created_at')}`
      },
    },
  },
```

Cube will filter data by `created_at` property and apply the refresh key for it.
There is how it looks in the Cube logs:

```bash
Executing SQL: e1155b2f-859b-4e61-a760-17af891f5f0b
--
  select min(("updated_orders".created_at::timestamptz AT TIME ZONE 'UTC')) from public.orders AS "updated_orders"
--
Executing SQL: e1155b2f-859b-4e61-a760-17af891f5f0b
--
  select max(("updated_orders".created_at::timestamptz AT TIME ZONE 'UTC')) from public.orders AS "updated_orders"
--
Performing query completed: e1155b2f-859b-4e61-a760-17af891f5f0b (10ms)
Performing query completed: e1155b2f-859b-4e61-a760-17af891f5f0b (13ms)
Performing query: e1155b2f-859b-4e61-a760-17af891f5f0b
Performing query: e1155b2f-859b-4e61-a760-17af891f5f0b
Executing SQL: e1155b2f-859b-4e61-a760-17af891f5f0b
--
  SELECT max(updated_at) FROM public.orders WHERE created_at >= '2021-08-01T00:00:00.000Z'::timestamptz AND created_at <= '2021-08-31T23:59:59.999Z'::timestamptz
--
Executing SQL: e1155b2f-859b-4e61-a760-17af891f5f0b
--
  SELECT max(updated_at) FROM public.orders WHERE created_at >= '2021-09-01T00:00:00.000Z'::timestamptz AND created_at <= '2021-09-30T23:59:59.999Z'::timestamptz
```

Note that Cube checks the refresh key value using date range over the `created_at` property. With this refresh key only one partition will be update.
