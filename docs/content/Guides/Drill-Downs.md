---
title: Drill Downs
permalink: /drill-downs
category: Guides
subCategory: Tutorials
menuOrder: 27
---

Drill Down is a powerful feature to facilitate data exploration. It allows to
build an interface to let users dive deeper into visualizations and data tables.
See [ResultSet.drillDown()](@cubejs-client-core#result-set-drill-down) on how to
use this feature on the client side.

You can follow
[this tutorial](https://cube.dev/blog/introducing-a-drill-down-table-api-in-cubejs/)
to learn more about building drill downs UI.

## Defining a Drill Down in Schema

A drill down is defined on the [measure](/schema/reference/measures) level in
your data schema. It’s defined as a list of dimensions called **drill members**.
Once defined, these drill members will always be used to show underlying data
when drilling into that measure.

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
