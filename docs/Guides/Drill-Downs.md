---
title: Drill Downs
permalink: /drill-downs
category: Guides
menuOrder: 12
---

Drill Down is a powerful feature to facilitate data exploration. It allows to build an interface to let users dive deeper into visualizations and data tables.


## Defining a Drill Down in Schema

A drill down is defined on the [measure](/measures) level in your data schema. It’s defined as a list of dimensions called __drill members__. Once defined, these drill members will always be used to show underlying data when drilling into that measure.

Let’s consider the following example of our imaginary e-commerce store. We have an Orders cube, which describes orders in our store. It’s connected to Users and Products.

```javascript
  cube(`Orders`, {
    sql: `select * from orders`,

    joins: {
      Users: {
        type: `belongsTo`,
        sql: `${Orders}.user_id = ${Users}.id`
      },

      Products: {
        type: `belongsTo`,
        sql: `${Orders}.product_id = ${Products}.id`
      }
    },

    measures: {
      count: {
        type: `count`,
        drillMembers: [id, status, Products.name, Users.email]
      }
    },

    dimensions: {
      id: {
        type: `number`,
        sql: `id`,
        primaryKey: true,
        shown: true
      },

      status: {
        type: `string`,
        sql: `status`
      }
    }
  });
```
