---
title: Cubes
permalink: /cube
scope: cubejs
category: Reference
menuOrder: 2
subCategory: Reference
proofread: 06/18/2019
---

A `cube` represents a table of data in Cube.js. Cubes are typically declared in separate files with one cube per file. Within each cube are definitions of measures, dimensions, and joins between cubes. A cube should always be declared with a capital letter.

```javascript
cube(`Users`, {
  sql: `select * from users`,

  joins: {
    Organizations: {
      relationship: `belongsTo`,
      sql: `${Users}.organization_id = ${Organizations}.id`
    }
  },

  measures: {
    count: {
      type: `count`,
      sql: `id`
    }
  },

  dimensions: {
    createdAt: {
      type: `time`,
      sql: `created_at`
    },

    country: {
      type: `string`,
      sql: `country`
    }
  }
});
```

## Naming

There are certain rules to follow for a cube and cube member names. 
You can use only `0-9`, `_`, and letter characters when naming a cube or a cube member.
Names should always start with a letter.

As a convention cube names start with upper case letters and member names with lower case letters.
As in case of JavaScript camel case is used for multi-word cube and member names.

## Parameters

### sql

The `sql` parameter specifies the SQL that will be used to generate a table that
will be queried by a cube. It can be any valid SQL query, but usually it takes the
form of a `select * from my_table` query. Please note that you don't need to use
`GROUP BY` in a SQL query on the cube level. This query should return a plain table,
without aggregations.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders`
});
```

You can reference others’ cubes SQL statement for code reuse.
```javascript
cube(`Companies`, {
  sql: `SELECT users.company_name, users.company_id FROM ${Users.sql()} AS users`
});
```

### title
Use `title` to change the display name of the cube.
By default, Cube.js will humanize the cube's name, so for instance, `UsersOrder`
would become `Users Orders`. If default humanizing doesn't work in your case, please use the title parameter. It is highly recommended to give human readable names to your cubes.
It will help everyone on a team better understand the data structure and will help maintain a consistent set of definitions across an organization.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders`,

  title: `Product Orders`,
});
```

### description
Use a description in your cubes to allow your team to better understand what this cube is about. It is a very simple and yet useful tool that gives a hint to everyone and makes sure data is interpreted correctly by users.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders`,

  title: `Product Orders`,
  description: `All orders related information`,
});
```


### extends

You can extend cubes in order to reuse all declared members of a cube.
In the example below, `ExtendedOrderFacts` will reuse the `sql` and `count` measures from `OrderFacts`:

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`

  measures: {
    count: {
      type: `count`,
      sql: `id`
    }
  }
});

cube(`ExtendedOrderFacts`, {
  extends: OrderFacts,

  measure: {
    doubleCount: {
      type: `number`,
      sql: `${count} * 2`
    }
  }
});
```

### refreshKey

Cube.js caching layer uses `refreshKey` queries to get the current version of content for a specific cube.
If a query result changes, Cube.js will invalidate all queries that rely on that cube.
If the `refreshKey` is not set, Cube.js will use the default strategy:

1. Check used pre-aggregations for query and use [pre-aggregations refreshKey](pre-aggregations#refresh-key), if none pre-aggregations are used…
2. Check the `max` of time dimensions with `updated` in the name, if none exist…
3. Check the `max` of any existing time dimension, if none exist…
4. Check the row count for this cube.

The result of the `refreshKey` query itself is cached for 2 minutes by default. You can
change it by passing the [refreshKeyRenewalThreshold](@cubejs-backend-server-core#cubejs-server-core-create-options-orchestrator-options) option when configuring the Cube.js Server.

You can use an existing timestamp from your tables. Make sure to select max
timestamp in that case.

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`

  refreshKey: {
    sql: `SELECT MAX(created_at) FROM orders`
  }
});
```

Or you can set it to be refreshed, for example every hour.

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`

  refreshKey: {
    sql: `SELECT date_trunc('hour', NOW())`
  }
});
```

## Context Variables

### Filter Params

`FILTER_PARAMS` allows you to use filter values during SQL generation. You can add it for any valid SQL expression as in the case of dimensions.

It has the following structure:

```javascript
FILTER_PARAMS.<CUBE_NAME>.<FILTER_NAME>.filter(expression)
```

The `filter` function accepts the expression, which could be either `String` or `Function`. See the
examples below.

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders WHERE ${FILTER_PARAMS.OrderFacts.date.filter('date')}`,

  dimensions: {
    date: {
      sql: `date`,
      type: `time`
    }
  }
});
```

This will generate the following SQL:

```sql
SELECT * FROM orders WHERE date >= '2018-01-01 00:00:00' and date <= '2018-12-31 23:59:59'
```

for the `['2018-01-01', '2018-12-31']` date range passed for the `OrderFacts.date` dimension.

You can also pass a function instead of an SQL expression as a `filter()` argument.
This way you can add BigQuery sharding filtering for events, which will reduce your billing cost.

```javascript
cube(`Events`, {
  sql: `
  SELECT * FROM schema.\`events*\`
  WHERE ${FILTER_PARAMS.Events.date.filter((from, to) =>
    `_TABLE_SUFFIX >= FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(${from})) AND _TABLE_SUFFIX <= FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(${to}))`
  )}
  `,

  dimensions: {
    date: {
      sql: `date`,
      type: `time`
    }
  }
});
```

### User Context

`USER_CONTEXT` is a user security object that is passed by the Cube.js Client.

User context is suitable for the row level security implementation.
For example, if you have an `orders` table that contains an `email` field you can restrict all queries to render results that belong only to the current user as follows:

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders WHERE ${USER_CONTEXT.email.filter('email')}`,

  dimensions: {
    date: {
      sql: `date`,
      type: `time`
    }
  }
});
```
