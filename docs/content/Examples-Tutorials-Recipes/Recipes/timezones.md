---
title: Using timezones
permalink: /recipes/timezones
category: Examples & Tutorials
subCategory: Queries
menuOrder: 4
---

## Use case

We want users from multiple timezones to retrieve metrics that account for these timezones, e.g., online store managers from different locations might want to view sales stats in their timezones.

We would also like our queries to use pre-aggregations to get great performance.

## Data schema

Let's explore the `Orders` cube that contains information about
orders in various statuses, including the transaction date
in the `createdAt` time dimension.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    status: {
      sql: `status`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },

    createdAtConverted: {
      sql: SQL_UTILS.convertTz(`created_at`),
      type: `time`,
    },
  },
});
```

## Query

To query the API with respect to a timezone, we need to provide the timezone via the [`timezone` property](https://cube.dev/docs/query-format#query-properties). The sales stats will be translated to reflect the point of view of a person from that location, e.g., for an online store manager from New York, let's pass `America/New_York`:

```javascript
{
  "dimensions": [
    "Orders.status",
    "Orders.createdAt",
    "Orders.createdAtConverted"
  ],
  "timeDimensions": [ {
    "dimension": "Orders.createdAt",
    "granularity": "day"
  } ],
  "order": {
    "Orders.createdAt": "desc"
  },
  "limit": 3,
  "timezone": "America/New_York"
}
```

## Result

Let's explore the retrieved data:

```javascript
[
  {
    "Orders.status": "shipped",
    "Orders.createdAt": "2023-11-05T00:00:00.000",
    "Orders.createdAtConverted": "2023-11-04T20:00:00.000",
    "Orders.createdAt.day": "2023-11-04T00:00:00.000"
  },
  {
    "Orders.status": "shipped",
    "Orders.createdAt": "2023-11-04T00:00:00.000",
    "Orders.createdAtConverted": "2023-11-03T20:00:00.000",
    "Orders.createdAt.day": "2023-11-03T00:00:00.000"
  },
  {
    "Orders.status": "completed",
    "Orders.createdAt": "2023-11-04T00:00:00.000",
    "Orders.createdAtConverted": "2023-11-03T20:00:00.000",
    "Orders.createdAt.day": "2023-11-03T00:00:00.000"
  }
]
```

The `Orders.createdAt` time dimension was provided in the `dimensions` part of the query. So, its values were returned "as is", in the UTC timezone. (Apparently, all orders were made at midnight.)

Also, check out the `Orders.createdAt.day` values in the result. They were returned because we've provided `Orders.createdAt` in the `timeDimensions` part of the query. So, they were translated to the New York timezone (shifted 4 hours back from UTC) and also truncated to the start of the day since we've specified the daily `granularity` in the query.

We also added the `Orders.createdAtConverted` to `dimensions` in the query. The respective values were also translated to the New York timezone but not truncated with respect to the granularity. Please check that the `createdAtConverted` dimension is defined using the [`SQL_UTILS.convertTz` method](https://cube.dev/docs/schema/reference/cube#convert-tz) that does the timezone translation.

## Configuration

To allow Cube to build pre-aggregations for timezones that can be specified in queries, we need to provide a list of such timezones via the `scheduledRefreshTimeZones` configuration option:

```javascript
module.exports = {
  scheduledRefreshTimeZones: ['America/New_York'],
};
```

## Source code

Please feel free to check out the [full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/timezones) or run it with the `docker-compose up` command. You'll see the result, including queried data, in the console.
