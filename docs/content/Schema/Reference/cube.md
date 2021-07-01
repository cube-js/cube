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

## Members and Referencing

Cubes have three types of members: measures, dimensions and segments.
Each member can be referenced by either fully qualified name `<CubeName>.<memberName>` or by short version `<memberName>` if member within same cube is referenced.
There's also handy `CUBE` context variable which references to the current cube.
Important difference between same cube references is `CUBE.<memberName>` references are resolved runtime as opposed to compile time `<memberName>` references.

Referencing cubes directly renders it's alias. For example it's handy to avoid name ambiguity in complex expressions:

```javascript
cube(`Users`, {
  sql: `select * from users`,

  joins: {
    Contacts: {
      sql: `${CUBE}.contact_id = ${Contacts}.id`,
      relationship: `hasOne`
    }
  }

  dimensions: {
    // primary key,

    name: {
      sql: `COALESCE(${CUBE}.name, ${Contacts}.name)`,
      type: `string`
    }
  }
});

cube(`Contacts`, {
  sql: `select * from contacts`

  // primary key
});
```

Referencing foreign cube in sql parameter instructs Cube.js to build implicit join to this cube.
For previous example following query

```javascript
{
  dimensions: ['Users.name']
}
```

leads to a join

```sql
select COALESCE("users".name, "contacts".name) "users__name"
FROM users "users"
LEFT JOIN contacts "contacts" ON "users".contact_id = "contacts".id
```

## Abstract cubes

Abstract cubes can be defined by simply omitting the first parameter to the
`cube()` function. Cubes defined in this way can still be extended, but will be
"hidden" from the [Developer Playground][ref-dev-playground] and calls to the
[`/meta` API endpoint][ref-rest-api-meta].

[ref-dev-playground]: /dev-tools/dev-playground
[ref-rest-api-meta]: /rest-api#api-reference-v-1-meta

```javascript
const Users = cube({
  sql: `select * from users`,

  dimensions: {
    // primary key,

    name: {
      sql: `${CUBE}.name`,
      type: `string`
    }
  }
});

cube(`Contacts`, {
  extends: Users,
});
```

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

  measures: {
    doubleCount: {
      type: `number`,
      sql: `${count} * 2`
    }
  }
});
```

You can also omit the cube name while defining it.
This way Cube.js doesn't register this cube globally but instead it returns reference to it which you can use while combining cubes.
It makes sense to use it for dynamic schema generation and reusing with `extends`.
Previous example without defining `OrderFacts` cube globally:

```javascript
const OrderFacts = cube({
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

  measures: {
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


The default values for `refreshKey` are
 * `every: '2 minute'` for BigQuery, Athena, Snowflake, and Presto.
 * `every: '10 second'` for all other databases.

Refresh key of a query is a concatenation of all cubes refresh keys involved in query.
For rollup queries pre-aggregation table name is used as a refresh key.

You can set up a custom refresh check SQL by changing `refreshKey` property. Often, a `MAX(updated_at_timestamp)` for OLTP data is a viable option, or examining a metadata table for whatever system is managing the data to see when it last ran.
timestamp in that case.

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`,

  // With this refreshKey Cube.js will only refresh the data if
  // the value of previous MAX(updated_at_timestamp) changed
  // By default Cube.js will check this refreshKey every 10 seconds
  refreshKey: {
    sql: `SELECT MAX(updated_at_timestamp) FROM orders`
  }
});
```

You can use interval based `refreshKey`.
For example:

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`,

  refreshKey: {
    every: `1 hour`
  }
});
```


`every` - can be set as an interval with granularities `second`, `minute`, `hour`, `day`, and `week` or accept CRON string with some limitations.
If you set `every` as CRON string, you can use the `timezone` property.

For example:

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`,
  refreshKey: {
    every: '30 5 * * 5',
    timezone: 'America/Los_Angeles'
  }
});
```

`every` can accept only equal time intervals - so "Day of month" and "month" intervals in CRON expressions are not supported.

<!-- prettier-ignore-start -->
[[warning |]]
| Cube.js supports two different formats of CRON expressions: standard and advanced with support for seconds.
<!-- prettier-ignore-end -->

Such `refreshKey` is just a syntactic sugar over `refreshKey` SQL.
It's guaranteed that `refreshKey` change it's value at least once during `every` interval.
It will be converted to appropriate SQL select which value will change over time based on interval value.
Values of interval based `refreshKey` are tried to be checked ten times within defined interval but not more than once per `1 second` and not less than once per `5 minute`.
For example if interval is `10 minute` it's `refreshKeyRenewalThreshold` will be 60 seconds and generated `refreshKey` SQL (Postgres) would be:

```sql
SELECT FLOOR(EXTRACT(EPOCH FROM NOW()) / 600)
```

For `5 second` interval `refreshKeyRenewalThreshold` will be just 1 second and SQL will be:

```sql
SELECT FLOOR(EXTRACT(EPOCH FROM NOW()) / 5)
```

### Supported cron formats

* Standard cron syntax

```
*    *    *    *    *
┬    ┬    ┬    ┬    ┬
│    │    │    │    |
│    │    │    │    └ day of week (0 - 7) (0 or 7 is Sun)
│    │    │    └───── month (1 - 12)
│    │    └────────── day of month (1 - 31, L)
│    └─────────────── hour (0 - 23)
└──────────────────── minute (0 - 59)
```

* Advanced cron format with support for seconds

```
*    *    *    *    *    *
┬    ┬    ┬    ┬    ┬    ┬
│    │    │    │    │    |
│    │    │    │    │    └ day of week (0 - 7) (0 or 7 is Sun)
│    │    │    │    └───── month (1 - 12)
│    │    │    └────────── day of month (1 - 31, L)
│    │    └─────────────── hour (0 - 23)
│    └──────────────────── minute (0 - 59)
└───────────────────────── second (0 - 59, optional)
```

### dataSource

Each cube in schema can have it's own `dataSource` name to support scenarios where data should be fetched from multiple databases.
Value of `dataSource` parameter will be passed to [`dbType`][ref-config-dbtype] and
[`driverFactory`][ref-config-driverfactory] functions as part of the context parameter.
By default, each cube has a `default` value for it's `dataSource`.
To override it you can use:

[ref-config-dbtype]: /config#options-reference-db-type
[ref-config-driverfactory]: /config#options-reference-driver-factory

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`,

  dataSource: `prod_db`
});
```

### sqlAlias

Use `sqlAlias` when auto-generated cube alias prefix is too long and truncated by DB such as Postgres:

```javascript
cube(`OrderFacts`, {
  sql: `SELECT * FROM orders`,

  sqlAlias: `ofacts`,

  // ...
});
```

It'll generate aliases for members such as `ofacts__count`.
`sqlAlias` affects all member names including pre-aggregation table names.

### rewriteQueries

Set this flag to true if you want Cube.js to rewrite your queries after final SQL has been generated.
This may be helpful to apply filter pushdown optimizations or reduce unnecessary query nesting.
For example:

```javascript
cube(`Tickets`, {
  rewriteQueries: true,

  // ...
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

  measures: {
    count: {
      type: `count`
    }
  },

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

for the `['2018-01-01', '2018-12-31']` date range passed for the `OrderFacts.date` dimension as in following query:

```javascript
{
  measures: ['OrderFacts.count'],
  timeDimensions: [{
    dimension: 'OrderFacts.date',
    granularity: 'day',
    dateRange: ['2018-01-01', '2018-12-31']
  }]
}
```

You can also pass a function instead of an SQL expression as a `filter()` argument.
This way you can add BigQuery sharding filtering for events, which will reduce your billing cost.

> **NOTE:** When you're passing function to the `filter()` function, params are passed as string parameters from driver and it's your responsibility to handle type conversions in this case.

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

### Security Context

`SECURITY_CONTEXT` is a user security object that is passed by the Cube.js Client.

Please see [Security Context section](security#security-context) on how to set `SECURITY_CONTEXT` value.

Security context is suitable for the row level security implementation.
For example, if you have an `orders` table that contains an `email` field you can restrict all queries to render results that belong only to the current user as follows:

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders WHERE ${SECURITY_CONTEXT.email.filter('email')}`,

  dimensions: {
    date: {
      sql: `date`,
      type: `time`
    }
  }
});
```

To ensure filter value presents for all requests `requiredFilter` can be used:

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM orders WHERE ${SECURITY_CONTEXT.email.requiredFilter('email')}`,

  dimensions: {
    date: {
      sql: `date`,
      type: `time`
    }
  }
});
```

### Unsafe Value

[[warning | Note]]
| Use of this feature entails SQL injection security risk. Use it with caution.

You can access values of context variables directly in javascript in order to use it during your SQL generation.
For example:

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM ${SECURITY_CONTEXT.type.unsafeValue() === 'employee' ? 'employee' : 'public'}.orders`,

  dimensions: {
    date: {
      sql: `date`,
      type: `time`
    }
  }
});
```

### SQL Utils
#### convertTz

In case you need to convert your timestamp to user request timezone in cube or member SQL you can use `SQL_UTILS.convertTz()` method. Note that Cube.js will automatically convert timezones for `timeDimensions` fields in [queries](Query-Format#query-properties).

[[warning | Note]]
| Dimensions that use `SQL_UTILS.convertTz()` should not be used as `timeDimensions` in queries. Doing so will apply the conversion multiple times and yield wrong results.

In case the same database field needs to be queried in `dimensions` and `timeDimensions`, create dedicated dimensions in the cube definition for the respective use:

```javascript
cube(`visitors`, {
  // ...

  dimensions: {
    createdAtConverted: { // do not use in timeDimensions query property
      type: 'time',
      sql: SQL_UTILS.convertTz(`created_at`)
    },
    createdAt: { // use in timeDimensions query property
      type: 'time',
      sql: `created_at`
    },
  }
})
```

### Compile context

There's global `COMPILE_CONTEXT` that captured as [RequestContext](@cubejs-backend-server-core#request-context) at the time of schema compilation.
It contains `securityContext` and any other variables provided by [extendContext](@cubejs-backend-server-core#options-reference-extend-context).

[[warning | Note]]
| While `securityContext` defined in `COMPILE_CONTEXT` it doesn't change it's value for different users. It may change however for different tenants.

```javascript
const { securityContext: { deploymentId } } = COMPILE_CONTEXT;

const schemaName = `user_${deploymentId}`;

cube(`Users`, {
  sql: `select * from ${schemaName}.users`,

  // ...
});
```
