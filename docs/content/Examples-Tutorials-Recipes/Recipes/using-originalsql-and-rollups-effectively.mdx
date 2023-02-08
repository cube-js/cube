---
title: Using original_sql and rollup Pre-aggregations Effectively
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
[`original_sql`][ref-schema-ref-preaggs-type-origsql] on the source (also known
as internal) database, and then configuring our existing `rollup`
pre-aggregations to use the `original_sql` pre-aggregation with the
[`use_original_sql_pre_aggregations`
property][ref-schema-ref-preaggs-use-origsql].

<WarningBox>

Storing pre-aggregations on an internal database requires write-access. Please
ensure that your database driver is not configured with `readOnly: true`.

</WarningBox>

```javascript
cube('Orders', {
  sql: `<YOUR_EXPENSIVE_SQL_QUERY HERE>`,

  pre_aggregations: {
    base: {
      type: `original_sql`,
      external: false,
    },

    main: {
      dimensions: [CUBE.id, CUBE.name],
      measures: [CUBE.count],
      time_dimension: CUBE.createdAt,
      granularity: `day`,
      use_original_sql_pre_aggregations: true,
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
