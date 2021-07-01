---
title: 'Code Reusability: Extending Cubes'
permalink: /extending-cubes
scope: cubejs
category: Guides
subCategory: Tutorials
menuOrder: 21
---

[comment]: # 'PROOFREAD: DONE'

Cube.js supports the
[extends feature](/schema/reference/cube#parameters-extends), which allows you
to reuse all declared members of a cube. This is a foundation for building
reusable data schemas.

Cubes in Cube.js are represented as
[Javascript objects](https://www.w3schools.com/js/js_objects.asp) with such
properties as measures, dimensions, and segments. Extending in Cube.js works
similarly to JavaScript’s prototype inheritance. Measures, dimensions, and
segments are merged as separate objects. So if the base cube defines measure A
and the extending cube defines measure B, the resulting cube will have both
measures A and B.

The usual pattern is to **extract common measures, dimensions, and joins into
the base cube** and then **extend from the base cube**. This helps to prevent
code duplication and makes code easier to maintain and refactor. The base cube
is usually placed into a separate file and excluded from the [context](context)
for end users.

In the example below, the `BaseEvents` cube defines the common events measures,
dimensions, and a join to the `Users` cube.

```javascript
cube(`BaseEvents`, {
  sql: `select * from events`,

  joins: {
    Users: {
      relationship: `belongsTo`,
      sql: `${CUBE}.user_id = ${Users}.id`,
    },
  },

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    timestamp: {
      sql: `time`,
      type: `time`,
    },
  },
});
```

<div class="block attention-block">

It’s important to use the `${CUBE}` reference instead of the `${BaseEvents}`
reference when referencing the current cube, which is going to be extended.
`${BaseEvents}` would not work in this case, when the cube will be extended.

</div>

The `ProductPurchases` and `PageViews` cubes are extended from `BaseEvents` and
define only the specific dimensions – `productName` for product purchases and
`pagePath` for page views.

```javascript
cube(`ProductPurchases`, {
  sql: `select * from product_purchases`,
  extends: BaseEvents,

  dimensions: {
    productName: {
      sql: `product_name`,
      type: `string`,
    },
  },
});

cube(`PageViews`, {
  sql: `select * from page_views`,
  extends: BaseEvents,

  dimensions: {
    pagePath: {
      sql: `page_path`,
      type: `string`,
    },
  },
});
```
