---
title: Incrementally Building Pre-aggregations for a Date Range
permalink: /recipes/incrementally-building-pre-aggregations-for-a-date-range
category: Examples & Tutorials
subCategory: Query acceleration
tags: FILTER_PARAMS,incremental,pre-aggregations,partitions
menuOrder: 2
---

## Use case

In scenarios where a large dataset spanning multiple years is pre-aggregated
with partitioning, it is often useful to only rebuild pre-aggregations between a
certain date range (and therefore only a subset of all the partitions). This is
because recalculating all partitions is often an expensive and/or time-consuming
process.

This is most beneficial when using data warehouses with partitioning support
(such as [AWS Athena][self-config-aws-athena] and [Google
BigQuery][self-config-google-bigquery]).

## Data schema

Let's use an example of a cube with a nested SQL query:

```javascript
cube('UsersWithOrganizations', {

  sql: `
WITH users AS (
    SELECT
      md5(company) AS organization_id,
      id AS user_id,
      created_at
    FROM public.users
),
organizations AS (
  (
    SELECT
      md5(company) AS id,
      company AS name,
      MIN(created_at)
    FROM
      public.users
    GROUP BY
      1,
      2
  )
)
SELECT
  users.*,
  organizations.name AS org_name
FROM
  users
LEFT JOIN organizations
  ON users.organization_id = organizations.id
`,

  preAggregations: {
    main: {
      dimensions: [CUBE.id, CUBE.organizationId]
      timeDimension: CUBE.createdAt,
      incremental: true,
      granularity: `day`,
      partitionGranularity: `month`,
      buildRangeStart: { sql: `SELECT DATE('2021-01-01')` },
      buildRangeEnd: { sql: `SELECT NOW()` },
    },
  },

  ...,

});
```

The cube above pre-aggregates the results of the `sql` property, and is
configured to incrementally build them as long as the date range is not before
January 1st, 2021.

However, if we only wanted to build pre-aggregations between a particular date
range within the users table, we would be unable to as the current configuration
only applies the date range to the final result of the SQL query defined in
`sql`.

In order to do the above, we'll "push down" the predicates to the inner SQL
query using [`FILTER_PARAMS`][ref-schema-ref-cube-filterparam] in conjunction
with the [`buildRangeStart` and `buildRangeStart`
properties][ref-schema-ref-preagg-buildrange]:

```javascript
cube('UsersWithOrganizations', {
  sql: `
WITH users AS (
    SELECT
      md5(company) AS organization_id,
      id AS user_id,
      created_at
    FROM public.users
    WHERE ${FILTER_PARAMS.UsersWithOrganizations.createdAt.filter('created_at')}
),
organizations AS (
  (
    SELECT
      md5(company) AS id,
      company AS name,
      MIN(created_at)
    FROM
      public.users
    GROUP BY
      1,
      2
  )
)
SELECT
  users.*,
  organizations.name AS org_name
FROM
  users
LEFT JOIN organizations
  ON users.organization_id = organizations.id
`,
});
```

## Result

By adding `FILTER_PARAMS` to the subquery inside the `sql` property, we now
limit the initial size of the dataset by applying the filter as early as
possible. When the pre-aggregations are incrementally built, the same filter is
used to apply the build ranges as defined by `buildRangeStart` and
`buildRangeEnd`.

[ref-schema-ref-preagg-buildrange]:
  /schema/reference/pre-aggregations#parameters-build-range-start-and-build-range-end
[ref-schema-ref-cube-filterparam]: /schema/reference/cube#filter-params
[self-config-aws-athena]: /config/databases/aws-athena/
[self-config-google-bigquery]: /config/databases/google-bigquery
