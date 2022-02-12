---
title: Using originalSql and rollup Pre-aggregations Effectively
permalink: /recipes/using-originalsql-and-rollups-effectively
category: Examples & Tutorials
subCategory: Query acceleration
menuOrder: 6
---

## Use case

For cubes that are built from an expensive SQL query, we can optimize
pre-aggregation builds so that they don't have to re-run the SQL query.

## Configuration

We can do this by creating a pre-aggregation of type
[`originalSql`][ref-schema-ref-preaggs-type-origsql] on the source (also known
as internal) database, and then configuring our existing `rollup`
pre-aggregations to use the `originalSql` pre-aggregation with the
[`useOriginalSqlPreAggregations` property][ref-schema-ref-preaggs-use-origsql].

<WarningBox>

Storing pre-aggregations on an internal database requires write-access. Please
ensure that your database driver is not configured with `readOnly: true`.

</WarningBox>

```javascript
cube('Orders', {
  sql: `<YOUR_EXPENSIVE_SQL_QUERY HERE>`,

  preAggregations: {
    base: {
      type: `originalSql`,
      external: false,
    },

    main: {
      dimensions: [CUBE.id, CUBE.name],
      measures: [CUBE.count],
      timeDimension: CUBE.createdAt,
      granularity: `day`,
      useOriginalSqlPreAggregations: true,
    },
  },

  ...,
})
```

## Result

With the above schema, the `main` pre-aggregation is built from the `base`
pre-aggregation.

[ref-schema-ref-preaggs-type-origsql]:
  /schema/reference/pre-aggregations#parameters-type-originalsql
[ref-schema-ref-preaggs-use-origsql]:
  https://cube.dev/docs/schema/reference/pre-aggregations#use-original-sql-pre-aggregations
