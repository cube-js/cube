---
title: Additional Concepts
permalink: /schema/fundamentals/additional-concepts
category: Data Schema
subCategory: Fundamentals
menuOrder: 3
redirect_from:
  - /drill-downs
  - /subquery
  - /working-with-string-time-dimensions
---

## Drilldowns

Drilldowns are a powerful feature to facilitate data exploration. It allows
building an interface to let users dive deeper into visualizations and data
tables. See [`ResultSet.drillDown()`][ref-cubejs-client-ref-resultset-drilldown]
on how to use this feature on the client side.

A drilldown is defined on the [measure][ref-schema-ref-measures] level in your
data schema. It’s defined as a list of dimensions called **drill members**. Once
defined, these drill members will always be used to show underlying data when
drilling into that measure.

Let’s consider the following example of our imaginary e-commerce store. We have
an Orders cube, which describes orders in our store. It’s connected to Users and
Products.

```javascript
cube(`Orders`, {
  sql: `select * from orders`,

  joins: {
    Users: {
      type: `belongsTo`,
      sql: `${Orders}.user_id = ${Users}.id`,
    },

    Products: {
      type: `belongsTo`,
      sql: `${Orders}.product_id = ${Products}.id`,
    },
  },

  measures: {
    count: {
      type: `count`,
      // Here we define all possible properties we might want
      // to "drill down" on from our front-end
      drillMembers: [id, status, Products.name, Users.email],
    },
  },

  dimensions: {
    id: {
      type: `number`,
      sql: `id`,
      primaryKey: true,
      shown: true,
    },

    status: {
      type: `string`,
      sql: `status`,
    },
  },
});
```

You can follow [this tutorial][blog-drilldown-api] to learn more about building
a UI for drilldowns.

## Subquery

You can use subquery dimensions to reference [measures][ref-schema-ref-measures]
from other cubes inside a [dimension][ref-schema-ref-dimensions]. Under the
hood, it behaves [as a correlated subquery][wiki-correlated-subquery], but is
implemented via joins for optimal performance and portability.

<!-- prettier-ignore-start -->
[[warning |]]
| You cannot use subquery dimensions to reference measures from the same cube.
<!-- prettier-ignore-end -->

Consider the following tables, where we have `deals` and `sales_managers`.
`deals` belong to `sales_managers` and have the `amount` dimension. What we want
is to calculate the amount of deals for `sales_managers`:

<div
  style="text-align: center"
>
  <img
    alt="Subquery Example with Deals and SalesManager cubes"
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Schema/Fundamentals/subquery.png"
    style="border: none"
    width="100%"
  />
</div>

To calculate the deals amount for sales managers in pure SQL, we can use a
correlated subquery, which will look like this:

```sql
SELECT
  id,
  (SELECT SUM(amount) FROM deals WHERE deals.sales_manager_id = sales_managers.id) AS deals_amount
FROM sales_managers
GROUP BY 1
```

Cube.js makes subqueries easy and efficient. Subqueries are defined as regular
dimensions with the parameter `subQuery` set to true.

```javascript
cube(`Deals`, {
  sql: `SELECT * FROM deals`,

  measures: {
    amount: {
      sql: `amount`,
      type: `sum`,
    },
  },
});

cube(`SalesManagers`, {
  sql: `SELECT * FROM sales_managers`,

  joins: {
    Deals: {
      relationship: `hasMany`,
      sql: `${SalesManagers}.id = ${Deals}.sales_manager_id`,
    },
  },

  measures: {
    averageDealAmount: {
      sql: `${dealsAmount}`,
      type: `avg`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true,
    },

    dealsAmount: {
      sql: `${Deals.amount}`,
      type: `number`,
      subQuery: true,
    },
  },
});
```

A subquery requires referencing at least one [measure][ref-schema-ref-measures]
in its definition. Generally speaking, all the columns used to define a subquery
dimension should first be defined as [measures][ref-schema-ref-measures] on
their respective cubes and then referenced from a subquery dimension over a
[join][ref-schema-ref-joins]. For example the following schema will **not**
work:

```javascript
cube(`Deals`, {
  sql: `select * from deals`,

  measures: {
    count: {
      type: `count`,
    },
  },
});

cube(`SalesManagers`, {
  // ...
  dimensions: {
    // ...
    dealsAmount: {
      sql: `SUM(${Deals}.amount)`, // Doesn't work, because `amount` is not a measure on `Deals`
      type: `number`,
      subQuery: true,
    },
  },
});
```

You can reference subquery dimensions in measures as usual
[dimensions][ref-schema-ref-dimensions]. The example below shows the definition
of an average deal amount per sales manager:

```javascript
cube(`SalesManagers`, {
  measures: {
    averageDealsAmount: {
      sql: `${dealsAmount}`,
      type: `avg`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true,
    },
    dealsAmount: {
      sql: `${Deals.amount}`,
      type: `number`,
      subQuery: true,
    },
  },
});
```

### <--{"id" : "Subquery"}--> Under the hood

Based on the subquery dimension definition, Cube.js will create a query that
will include the primary key dimension of the main cube and all
[measures][ref-schema-ref-measures] and [dimensions][ref-schema-ref-dimensions]
included in the SQL definition of the subquery dimension.

This query will be joined as a `LEFT JOIN` onto the main SQL query. For example,
when using the `SalesManagers.dealsAmount` subquery dimension, the following
query will be generated:

```json
{
  "measures": ["SalesManagers.dealsAmount"],
  "dimensions": ["SalesManagers.id"]
}
```

If a query includes the `SalesManagers.averageDealAmount` measure, the following
SQL will be generated:

```sql
SELECT
  AVG(sales_managers__average_deal_amount)
FROM sales_managers
LEFT JOIN (
  SELECT
    sales_managers.id sales_managers__id,
    SUM(deals.amount) sales_managers__average_deal_amount
  FROM sales_managers
  LEFT JOIN deals
    ON sales_managers.id = deals.sales_manager_id
  GROUP BY 1
) sales_managers__average_deal_amount_subquery
  ON sales_managers__average_deal_amount_subquery.sales_managers__id = sales_managers.id
```

## String Time Dimensions

Cube.js always expects a timestamp with timezone (or compatible type) as an
input to the time dimension. However, there are a lot of cases when the
underlying table's datetime information is stored as a string. Most SQL
databases support datetime parsing which allows converting strings to
timestamps. Let's consider an example cube for BigQuery:

```javascript
cube(`Events`, {
  sql: `SELECT * FROM schema.events`,

  // ...

  dimensions: {
    date: {
      sql: `PARSE_TIMESTAMP('%Y-%m-%d', date)`,
      type: `time`,
    },
  },
});
```

In this particular cube, the `date` column will be parsed using the `%Y-%m-%d`
format. Please note that as we do not pass timezone parameter to
[`PARSE_TIMESTAMP`][bq-parse-timestamp], it will set `UTC` as the timezone by
default. You should always set timezone appropriately for parsed timestamps as
Cube.js always does timezone conversions according to user settings.

Although query performance of big data backends like BigQuery or Presto won't
likely suffer from date parsing, performance of RDBMS backends like Postgres
most likely will. Adding timestamp columns with indexes should strongly be
considered in this case.

[blog-drilldown-api]:
  https://cube.dev/blog/introducing-a-drill-down-table-api-in-cubejs/
[bq-parse-timestamp]:
  https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#parse_timestamp
[ref-cubejs-client-ref-resultset-drilldown]:
  /@cubejs-client-core#drill-down
[ref-schema-ref-dimensions]: /schema/reference/dimensions
[ref-schema-ref-joins]: /schema/reference/joins
[ref-schema-ref-measures]: /schema/reference/measures
[wiki-correlated-subquery]: https://en.wikipedia.org/wiki/Correlated_subquery
