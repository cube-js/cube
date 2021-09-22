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

<GitHubCodeBlock
  href="https://github.com/cube-js/cube.js/blob/master/examples/recipes/joining-multiple-datasources-data/cube.js"
  titleSuffixCount={2}
  part=""
  lang="js"
/>

## Data schema

First, we'll define
[rollup](https://cube.dev/docs/schema/reference/pre-aggregations#parameters-type-rollup)
pre-aggregations for `Products` and `Suppliers`.

<GitHubCodeBlock
  href="https://github.com/cube-js/cube.js/blob/master/examples/recipes/joining-multiple-datasources-data/schema/Products.js"
  titleSuffixCount={2}
  part="productsRollup"
  lang="js"
/>

<GitHubCodeBlock
  href="https://github.com/cube-js/cube.js/blob/master/examples/recipes/joining-multiple-datasources-data/schema/Suppliers.js"
  titleSuffixCount={2}
  part="suppliersRollup"
  lang="js"
/>

Then, we'll also define a `rollupJoin` pre-aggregation. It will enable Cube to
aggregate data from multiple data sources. Note that the joined rollups should
contain dimensions on which they're joined. In our case, it's the `supplierId`
dimension in the `Products` cube:

<GitHubCodeBlock
  href="https://github.com/cube-js/cube.js/blob/master/examples/recipes/joining-multiple-datasources-data/schema/Products.js"
  titleSuffixCount={2}
  part="combinedRollup"
  lang="js"
/>

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

<CubeQueryResultSet
api="https://amber-bear.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1"
token=""
query={{
    "order": {
      "Products.name": "asc"
    },
    "dimensions": [
      "Products.name",
      "Suppliers.company",
      "Suppliers.email"
    ],
    "limit": 3
}} />

```javascript
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

Please feel free to check out the full source code or run it with the
`docker-compose up` command. You'll see the result, including queried data, in
the console.

<GitHubFolderLink
  href="https://github.com/cube-js/cube.js/blob/master/examples/recipes/joining-multiple-datasources-data"
/>
