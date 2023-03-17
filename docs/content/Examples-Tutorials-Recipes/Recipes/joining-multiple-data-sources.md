---
title: Joining Data from Multiple Data Sources
permalink: /recipes/joining-multiple-data-sources
category: Examples & Tutorials
subCategory: Query acceleration
menuOrder: 6
---

## Use case

Let's imagine we store information about products and their suppliers in
separate databases. We want to aggregate data from these data sources while
having decent performance. In the recipe below, we'll learn how to create a
[rollup join](https://cube.dev/docs/schema/reference/pre-aggregations#parameters-type-rollupjoin)
between two databases to achieve our goal.

## Configuration

First of all, we should define our database connections with the `dataSource`
option:

```javascript
const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  driverFactory: ({ dataSource }) => {
    if (dataSource === 'suppliers') {
      return new PostgresDriver({
        database: 'recipes',
        host: 'demo-db-recipes.cube.dev',
        user: 'cube',
        password: '12345',
        port: '5432',
      });
    }

    if (dataSource === 'products') {
      return new PostgresDriver({
        database: 'ecom',
        host: 'demo-db-recipes.cube.dev',
        user: 'cube',
        password: '12345',
        port: '5432',
      });
    }

    throw new Error('dataSource is undefined');
  },
};
```

## Data schema

First, we'll define
[rollup](https://cube.dev/docs/schema/reference/pre-aggregations#parameters-type-rollup)
pre-aggregations for `Products` and `Suppliers`.

```javascript
preAggregations: {
  productsRollup: {
    type: `rollup`,
    external: true,
    dimensions: [CUBE.name, CUBE.supplierId],
    indexes: {
      categoryIndex: {
        columns: [CUBE.supplierId],
      }
    }
  },
```

```javascript
preAggregations: {
  suppliersRollup: {
    type: `rollup`,
    external: true,
    dimensions: [CUBE.id, CUBE.company, CUBE.email],
    indexes: {
      categoryIndex: {
        columns: [CUBE.id],
      }
    }
  }
}
```

Then, we'll also define a `rollupJoin` pre-aggregation. It will enable Cube to
aggregate data from multiple data sources. Note that the joined rollups should
contain dimensions on which they're joined. In our case, it's the `supplierId`
dimension in the `Products` cube:

```javascript
combinedRollup: {
  type: `rollupJoin`,
  dimensions: [Suppliers.email, Suppliers.company, CUBE.name],
  rollups: [Suppliers.suppliersRollup, CUBE.productsRollup],
  external: true,
}
```

## Query

Let's get the product names and their suppliers' info, such as company name and
email, with the following query:

```javascript
{
  "order": {
    "Products.name": "asc"
  },
  "dimensions": [
    "Products.name",
    "Suppliers.company",
    "Suppliers.email"
  ],
  "limit": 3
}
```

## Result

We'll get the data from two pre-aggregations joined into one `rollupJoin`:

```javascript
[
  {
    "Products.name": "Awesome Cotton Sausages",
    "Suppliers.company": "Justo Eu Arcu Inc.",
    "Suppliers.email": "id.risus@luctuslobortisClass.net"
  },
  {
    "Products.name": "Awesome Fresh Keyboard",
    "Suppliers.company": "Quisque Purus Sapien Limited",
    "Suppliers.email": "Cras@consectetuercursuset.co.uk"
  },
  {
    "Products.name": "Awesome Rubber Soap",
    "Suppliers.company": "Tortor Inc.",
    "Suppliers.email": "Mauris@ac.com"
  }
]

// Names of the used pre-aggregations

{
  "dev_pre_aggregations.products_products_rollup": {
    "targetTableName": "dev_pre_aggregations.products_products_rollup_jdm0assd_jnwrwqag_1gk0duh"
  },
  "dev_pre_aggregations.suppliers_suppliers_rollup": {
    "targetTableName": "dev_pre_aggregations.suppliers_suppliers_rollup_j5cd0gsr_jf5ivbmx_1gk0b7s"
  }
}
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/joining-multiple-datasources-data)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
