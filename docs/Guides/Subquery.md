---
title: Subquery
permalink: /subquery
scope: cubejs
category: Guides
subCategory: Tutorials
menuOrder: 16
---

[comment]: # (PROOFREAD: DONE)

You can use subquery dimensions to **reference measures from other cubes inside a dimension**. Under the hood, it behaves [like the correlated subquery](https://en.wikipedia.org/wiki/Correlated_subquery), but is implemented via joins for performance optimization and portability.

Consider the following data schema, where we have `Deals` and `Sales Managers`. `Deals` belong to `Sales Managers` and have the `amount` dimension. What we want is to calculate the amount of deals for `Sales Managers`.

![subquery-1.png](https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/Guides/subquery-1.png)


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
  
  measures: {
    averageDealAmount: {
      sql: `${dealsAmount}`,
      type: `avg`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true
    },

    dealsAmount: {
      sql: `${Deals.amount}`,
      type: `number`,
      subQuery: true
    }
  }
});
```

Sub query requires you to reference at least one measure in the definition. 
Generally speaking all measures involved in defining particular sub query dimension should be defined as measures first and then referenced from a sub query dimension.
For example the following schema **will not work**:

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
  // ...
  dimensions: {
    // ...
    dealsAmount: {
      sql: `sum(${Deals}.amount)`, // !!! Doesn't work!
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
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true
    }
  }
});
```

## Under the hood

Based on sub query dimension definition, Cube.js will create a query that will include primary key dimension of main cube and all measures and dimensions included in sql definition of sub query dimension.
This query will be joined as a left join to the main SQL query.
For example for `SalesManagers.dealsAmount` sub query dimension following query will be generated:

```javascript
{
  measures: ['SalesManagers.dealsAmount'],
  dimensions: ['SalesManagers.id']
}
```

In case of `{ measures: ['SalesManagers.averageDealAmount'] }` query following SQL will be generated:

```javascript
SELECT avg(sales_managers__average_deal_amount) FROM sales_managers
LEFT JOIN (
  SELECT sales_managers.id sales_managers__id, sum(deals.amount) sales_managers__average_deal_amount FROM sales_managers
  LEFT JOIN deals ON sales_managers.id = deals.sales_manager_id
  GROUP BY 1
) sales_managers__average_deal_amount_subquery ON sales_managers__average_deal_amount_subquery.sales_managers__id = sales_managers.id
```
