---
title: Cubes
permalink: /cube
scope: cubejs
category: Reference
menuOrder: 2
subCategory: Reference
---

A `cube` represents a table of data in Cube.js. Cubes are typically declared in separate files with one cube per file. Within each cube are definitions of measures, dimensions and joins between cubes. Cube should always be declared with capital letter.

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

## Parameters

### sql

The `sql` parameter specifies the SQL that will be used to generate a table that
will be queried by cube.  It can be any valid SQL query, but usually it takes
form of `select * from my_table` query. Please note, that you don't need to use
`GROUP BY` in SQL query on cube level. This query should return a plain table,
without aggregations.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders`
});
```

You can reference other's cubes SQL statement for code reuse.
```javascript
cube(`Companies`, {
  sql: `SELECT users.company_name, users.company_id FROM ${Users.sql()} AS users`
});
```

### title
Use `title` to change display name of the cube.
By default Cube.js will humanize the cube's name, so for instance, `UsersOrder`
would become `Users Orders`. If default humanizing doesn't work in you case please use title parameter. It is highly recommended to give human readable names for your cubes.
It would help everyone on a team better understand data structure and will help maintain consistent set of definitions across organization.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders`,

  title: `Product Orders`,
});
```

### description
Use description in your cubes to allow your team better understand what this cube is about. It is very simple and yet useful tool that gives a hint to everyone and makes sure data is interpreted correctly by users.

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders`,

  title: `Product Orders`,
  description: `All orders related information`,
});
```


### extends

You can extend cubes in order to reuse all declared members of a cube.
In the example below `ExtendedOrderFacts` will reuse `sql` and `count` measure from `OrderFacts`:

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

Cube.js caching layer uses `refreshKey` queries to get current version of content for specific cube.
If query result changes Cube.js will invalidate all queries that rely on that cube.
If `refreshKey` is not set Cube.js will try to use time dimension which have `'updated'` substring in name,
then will check if any other time dimension exists.
If time dimension is found then `max` value of this time dimension will be used as `refreshKey`.
Otherwise count of rows for this cube will be used as a `refreshKey` by default.
Result of `refreshKey` query itself is cached for 2 minutes by default.

You can use existing timestamp from your tables. Make sure to select max
timestamp in that case.

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`

  refreshKey: {
    sql: `SELECT MAX(created_at) FROM orders`
  }
});
```

Or you can set it to be refreshed for example every hour.

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

Filter params allows you to use filter values selected by user during SQL generation.

You can add it for any valid SQL expression as in case of dimension.

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

It'll generate the following SQL

```sql
SELECT * FROM orders WHERE date >= '2018-01-01 00:00:00' and date <= '2018-12-31 23:59:59'
```

for `['2018-01-01', '2018-12-31']` date range passed for `OrderFacts.date` dimension.

You can also pass function instead of SQL expression as `filter()` argument.
This way you can add BigQuery sharding filtering for events which will reduce your billing cost.

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

`USER_CONTEXT` is user security object that passed by Cube.js Client.

User context is suitable for the row level security implementation.
For example if you have `orders` table which contains `email` field you can restrict all queries to render results that belong only to current user as following:

```javascript
cube(`Oreders`, {
  sql: `SELECT * FROM orders WHERE ${USER_CONTEXT.email.filter('email')}`,

  dimensions: {
    date: {
      sql: `date`,
      type: `time`
    }
  }
});
```
