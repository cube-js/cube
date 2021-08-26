---
title: Accelerating Non-Additive Measures
permalink: /recipes/non-additivity
category: Examples & Tutorials
subCategory: Query acceleration
menuOrder: 6
---

## Use case

We want to run queries against
[pre-aggregations](https://cube.dev/docs/caching#pre-aggregations) only to ensure our
application's superior performance. Usually, accelerating a query is as simple as
including its measures and dimensions to the pre-aggregation
[definition](https://cube.dev/docs/schema/reference/pre-aggregations#parameters-measures).

[Non-additive](https://cube.dev/docs/caching/pre-aggregations/getting-started#ensuring-pre-aggregations-are-targeted-by-queries-non-additivity)
measures (e.g., average values or distinct counts) are a special case.
Pre-aggregations with such measures are less likely to be
[selected](https://cube.dev/docs/caching/pre-aggregations/getting-started#ensuring-pre-aggregations-are-targeted-by-queries-selecting-the-pre-aggregation)
to accelerate a query. However, there are a few ways to work around that.

## Data schema

Let's explore the `Users` cube that contains various measures describing users' age:

- count of unique age values (`distinctAges`)
- average age (`avgAge`)
- 90th [percentile](https://cube.dev/docs/recipes/percentiles) of age (`p90Age`)


```javascript
    distinctAges: {
      sql: `age`,
      type: `countDistinct`,
    },

    avgAge: {
      sql: `age`,
      type: `avg`,
    },

    p90Age: {
      sql: `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY age)`,
      type: `number`,
    },
```

All of these measures are non-additive. Practically speaking, it means that the pre-aggregation below would only accelerate a query that fully matches its
definition:

```javascript
    main: {
      measures: [
        CUBE.distinctAges,
        CUBE.avgAge,
        CUBE.p90Age
      ],
      dimensions: [
        CUBE.gender
      ]
    },
```

This query will match the pre-aggregation above and, thus, will be accelerated:

```javascript
{
  "measures": [
    "Users.distinctAges",
    "Users.avgAge",
    "Users.p90Age"
  ],
  "dimensions": [
    "Users.gender"
  ]
}
```

Meanwhile, the query below won't match the same pre-aggregation because it contains non-additive measures and omits the `gender` dimension. It won't be accelerated:

```javascript
{
  "measures": [
    "Users.distinctAges",
    "Users.avgAge",
    "Users.p90Age"
  ]
}
```

Let's explore some possible workarounds.

### Replacing with approximate additive measures

Often, non-additive `countDistinct` measures can be changed to have the [`countDistinctApprox` type](https://cube.dev/docs/schema/reference/types-and-formats#measures-types-count-distinct-approx)
which will make them additive and orders of magnitude more performant. This
`countDistinctApprox` measures can be used in pre-aggregations. However, there are two
drawbacks:

- This type is approximate, so the measures might yield slightly different results compared to their `countDistinct` counterparts. Please comsult with your database's
documentation to learn more.
- The `countDistinctApprox` is not supported with all databases. Currently, Cube supports it for Athena, BigQuery, and Snowflake.

For example, the `distinctAges` measure can be rewritten as follows:

```javascript
    distinctAges: {
      sql: `age`,
      type: `countDistinctApprox`,
    },
```

### Decomposing into a formula with additive measures

Non-additive `avg` measures can be rewritten as
[calculated measures](https://cube.dev/docs/schema/reference/measures#calculated-measures)
that reference additive measures only. Then, this additive measures can be used in
pre-aggregations.

For example, the `avgAge` measure can be rewritten as follows:

```javascript
    avgAge: {
      sql: `${CUBE.ageSum} / ${CUBE.count}`,
      type: `number`,
    },

    ageSum: {
      sql: `age`,
      type: `sum`,
    },

    count: {
      type: `count`,
    },
```

### Providing multiple pre-aggregations

If the two workarounds described above don't apply to your use case, feel free to create
additional pre-aggregations with definitions that fully match your queries with
non-additive measures. You will get a performance boost at the expense of a slightly
increased overall pre-aggregation build time and space consumed.

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/non-additivity)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
