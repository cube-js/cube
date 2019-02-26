---
title: Subquery
permalink: /subquery
scope: cubejs
category: Guides
---

[comment]: # (PROOFREAD: DONE)

You can use subquery dimensions to **reference measures from other cubes inside a dimension**. Under the hood, it behaves [like the correlated subquery](https://en.wikipedia.org/wiki/Correlated_subquery), but is implemented via joins for performance optimization and portability.

Consider the following data schema, where we have `Deals` and `Sales Managers`. `Deals` belong to `Sales Managers` and have the `amount` dimension. What we want is to calculate the amount of deals for `Sales Managers`.

[[https://github.com/statsbotco/cube.js/blob/master/docs/Guides/subquery-1.png|alt=subquery-schema]]


To calculate the deals amount for sales managers in pure SQL, we can use the correlated subquery, which will look like this:

```sql
SELECT
   id,
   (SELECT sum(amount) FROM deals WHERE deals.sales_manager_id = sales_managers.id) as deals_amount
FROM sales_managers
GROUPD BY 1
```

Cube.js makes subqueries easy and efficient. Subqueries are defined as regular dimensions with the parameter `subQuery` set to true.

```javascript

cube(`Deals`, {
  sql: `select * from deals`,

  measures: {
    amount: {
      sql: `amount`,
      type: `sum`
    }
  }
});

cube(`SalesManagers`, {
  sql: `select * from sales_managers`,

  joins: {
    Deals: {
      relationship: `hasMany`,
      sql: `${SalesManagers}.id = ${Deals}.sales_manager_id`
    }
  },

  dimensions: {
    dealsAmount: {
      sql: `${Deals.amount}`,
      type: `number`,
      subQuery: true
    }
  }
});
```
You can **reference subquery dimensions in measures as usual dimensions**. The example below shows the definition of an average deal amount per sales manager:

```javascript

cube(`SalesManagers`, {
   measures: {
      averageDealsAmount: {
        sql: `${dealsAmount}`,
        type: `avg`
      }
   }
});
```

You can find a real-world example of using subquery in the [Events Analytics tutorial](event-analytics#connecting-events-to-sessions) to calculate an average session duration.

