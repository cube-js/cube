---
title: Query Format
permalink: /query-format
category: Cube.js Backend
menuOrder: 2
---

Cube Queries are plain JavaScript objects, describing an analytics query. The
basic elements of a query (query members) are `measures`, `dimensions`, and
`segments`.

The query member format name is `CUBE_NAME.MEMBER_NAME`, for example the
dimension `email` in the Cube `Users` would have the name `Users.email`.

In the case of dimension of type `time` granularity could be optionally added to
the name, in the following format `CUBE_NAME.TIME_DIMENSION_NAME.GRANULARITY`,
ex: `Stories.time.month`.

Supported granularities: `second`, `minute`, `hour`, `day`, `week`, `month`,
`quarter` and `year`.

The Cube.js client also accepts an array of queries. By default it will be
treated as a [Data Blending](/recipes/data-blending) query.

## Query Properties

A Query has the following properties:

- `measures`: An array of measures.
- `dimensions`: An array of dimensions.
- `filters`: An array of objects, describing filters. Learn about
  [filters format](#filters-format).
- `timeDimensions`: A convenient way to specify a time dimension with a filter.
  It is an array of objects in [timeDimension format.](#time-dimensions-format)
- `segments`: An array of segments. A segment is a named filter, created in the
  Data Schema.
- `limit`: A row limit for your query. The default value is `10000`. The maximum
  allowed limit is `50000`.
- `offset`: The number of initial rows to be skipped for your query. The default
  value is `0`.
- `order`: An object, where the keys are measures or dimensions to order by and
  their corresponding values are either `asc` or `desc`. The order of the fields
  to order on is based on the order of the keys in the object.
- `timezone`: All time based calculations performed within Cube.js are
  timezone-aware. This property is applied to all time dimensions during
  aggregation and filtering. It isn't applied to the time dimension referenced
  in a `dimensions` query property unless granularity or date filter is
  specified. Using this property you can set your desired timezone in
  [TZ Database Name](https://en.wikipedia.org/wiki/Tz_database) format, e.g.:
  `America/Los_Angeles`. The default value is `UTC`.
- `renewQuery`: If `renewQuery` is set to `true`, Cube.js will renew all
  `refreshKey` for queries and query results in the foreground. However if the
  `refreshKey` or `refreshKeyRenewalThreshold` don't indicate that there's a
  need for an update this setting has no effect. The default value is `false`.
  > **NOTE**: Cube.js provides only eventual consistency guarantee. Using too
  > small `refreshKeyRenewalThreshold` values together with `renewQuery` in
  > order to achieve immediate consistency can lead to endless refresh loops and
  > overall system instability.
- `ungrouped`: If `ungrouped` is set to `true` no `GROUP BY` statement will be
  added to the query. Instead, the raw results after filtering and joining will
  be returned without grouping. By default `ungrouped` queries require a primary
  key as a dimension of every cube involved in the query for security purposes.
  To disable this behavior please see the
  [allowUngroupedWithoutPrimaryKey](@cubejs-backend-server-core#allow-ungrouped-without-primary-key)
  server option. In case of `ungrouped` query measures will be rendered as
  underlying `sql` of measures without aggregation and time dimensions will be
  truncated as usual however not grouped by.

```js
{
  measures: ['Stories.count'],
  dimensions: ['Stories.category'],
  filters: [{
    member: 'Stories.isDraft',
    operator: 'equals',
    values: ['No']
  }],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }],
  limit: 100,
  offset: 50,
  order: {
    'Stories.time': 'asc',
    'Stories.count': 'desc'
  },
  timezone: 'America/Los_Angeles'
}
```

### <--{"id" : "Query Properties"}--> Default order

If the `order` property is not specified in the query, Cube.js sorts results by
default using the following rules:

- The first time dimension with granularity, ascending. If no time dimension
  with granularity exists...
- The first measure, descending. If no measure exists...
- The first dimension, ascending.

### <--{"id" : "Query Properties"}--> Alternative order format

Also you can control the ordering of the `order` specification, Cube.js support
alternative order format - array of tuples:

```js
{
  ...,
  order: [
      ['Stories.time', 'asc'],
      ['Stories.count', 'asc']
    ]
  },
  ...
}
```

## Filters Format

A filter is a Javascript object with the following properties:

- `member`: Dimension or measure to be used in the filter, for example:
  `Stories.isDraft`. See below on difference between filtering dimensions vs
  filtering measures.
- `operator`: An operator to be used in the filter. Only some operators are
  available for measures. For dimensions the available operators depend on the
  type of the dimension. Please see the reference below for the full list of
  available operators.
- `values`: An array of values for the filter. Values must be of type String. If
  you need to pass a date, pass it as a string in `YYYY-MM-DD` format.

#### Filtering Dimensions vs Filtering Measures

Filters are applied differently to dimensions and measures.

When you filter on a dimension, you are restricting the raw data before any
calculations are made. When you filter on a measure, you are restricting the
results after the measure has been calculated.

## Filters Operators

Only some operators are available for measures. For dimensions, the available
operators depend on the
[type of the dimension](/schema/reference/types-and-formats#types).

### <--{"id" : "Filters Operators"}--> equals

Use it when you need an exact match. It supports multiple values.

- Applied to measures.
- Dimension types: `string`, `number`, `time`.

```js
{
  member: "Users.country",
  operator: "equals",
  values: ["US", "Germany", "Israel"]
}
```

### <--{"id" : "Filters Operators"}--> notEquals

The opposite operator of `equals`. It supports multiple values.

- Applied to measures.
- Dimension types: `string`, `number`, `time`.

```js
{
  member: "Users.country",
  operator: "notEquals",
  values: ["France"]
}
```

### <--{"id" : "Filters Operators"}--> contains

The `contains` filter acts as a wildcard case insensitive `LIKE` operator. In
the majority of SQL backends it uses `ILIKE` operator with values being
surrounded by `%`. It supports multiple values.

- Dimension types: `string`.

```js
{
  member: "Posts.title",
  operator: "contains",
  values: ["serverless", "aws"]
}
```

### <--{"id" : "Filters Operators"}--> notContains

The opposite operator of `contains`. It supports multiple values.

- Dimension types: `string`.

```js
{
  member: "Posts.title",
  operator: "notContains",
  values: ["ruby"]
}
```

### <--{"id" : "Filters Operators"}--> gt

The `gt` operator means **greater than** and is used with measures or dimensions
of type `number`.

- Applied to measures.
- Dimension types: `number`.

```js
{
  member: "Posts.upvotesCount",
  operator: "gt",
  values: ["100"]
}
```

### <--{"id" : "Filters Operators"}--> gte

The `gte` operator means **greater than or equal to** and is used with measures
or dimensions of type `number`.

- Applied to measures.
- Dimension types: `number`.

```js
{
  member: "Posts.upvotesCount",
  operator: "gte",
  values: ["100"]
}
```

### <--{"id" : "Filters Operators"}--> lt

The `lt` operator means **less than** and is used with measures or dimensions of
type `number`.

- Applied to measures.
- Dimension types: `number`.

```js
{
  member: "Posts.upvotesCount",
  operator: "lt",
  values: ["10"]
}
```

### <--{"id" : "Filters Operators"}--> lte

The `lte` operator means **less than or equal to** and is used with measures or
dimensions of type `number`.

- Applied to measures.
- Dimension types: `number`.

```js
{
  member: "Posts.upvotesCount",
  operator: "lte",
  values: ["10"]
}
```

### <--{"id" : "Filters Operators"}--> set

Operator `set` checks whether the value of the member **is not** `NULL`. You
don't need to pass `values` for this operator.

- Applied to measures.
- Dimension types: `number`, `string`, `time`.

```js
{
  member: "Posts.authorName",
  operator: "set"
}
```

### <--{"id" : "Filters Operators"}--> notSet

An opposite to the `set` operator. It checks whether the value of the member
**is** `NULL`. You don't need to pass `values` for this operator.

- Applied to measures.
- Dimension types: `number`, `string`, `time`.

```js
{
  member: "Posts.authorName",
  operator: "notSet"
}
```

### <--{"id" : "Filters Operators"}--> inDateRange

The operator `inDateRange` is used to filter a time dimension into a specific
date range. The values must be an array of dates with the following format
'YYYY-MM-DD'. If only one date specified the filter would be set exactly to this
date.

There is a convient way to use date filters with grouping -
[learn more about timeDimensions query property here](#time-dimensions-format)

- Dimension types: `time`.

```js
{
  member: "Posts.time",
  operator: "inDateRange",
  values: ['2015-01-01', '2015-12-31']
}
```

### <--{"id" : "Filters Operators"}--> notInDateRange

An opposite operator to `inDateRange`, use it when you want to exclude specific
dates. The values format is the same as for `inDateRange`.

- Dimension types: `time`.

```js
{
  member: "Posts.time",
  operator: "notInDateRange",
  values: ['2015-01-01', '2015-12-31']
}
```

### <--{"id" : "Filters Operators"}--> beforeDate

Use it when you want to retreive all results before some specific date. The
values should be an array of one element in `YYYY-MM-DD` format.

- Dimension types: `time`.

```js
{
  member: "Posts.time",
  operator: "beforeDate",
  values: ['2015-01-01']
}
```

### <--{"id" : "Filters Operators"}--> afterDate

The same as `beforeDate`, but is used to get all results after a specific date.

- Dimension types: `time`.

```js
{
  member: "Posts.time",
  operator: "afterDate",
  values: ['2015-01-01']
}
```

## Boolean logical operators

Filters can contain `or` and `and` logical operators. Logical operators have
only one of the following properties:

- `or` An array with two or more filters or other logical operators
- `and` An array with two or more filters or other logical operators

```js
{
  or: [
    {
      member: 'visitors.source',
      operator: 'equals',
      values: ['some'],
    },
    {
      and: [
        {
          member: 'visitors.source',
          operator: 'equals',
          values: ['other'],
        },
        {
          member: 'visitor_checkins.cardsCount',
          operator: 'equals',
          values: ['0'],
        },
      ],
    },
  ];
}
```

> **Note:** You can not put dimensions and measures filters in the same logical
> operator.

## Time Dimensions Format

Since grouping and filtering by a time dimension is quite a common case, Cube.js
provides a convenient shortcut to pass a dimension and a filter as a
`timeDimension` property.

- `dimension`: Time dimension name.
- `dateRange`: An array of dates with the following format `YYYY-MM-DD` or in
  `YYYY-MM-DDTHH:mm:ss.SSS` format. Values should always be local and in query
  `timezone`. Dates in `YYYY-MM-DD` format are also accepted. Such dates are
  padded to the start and end of the day if used in start and end of date range
  interval accordingly. If only one date is specified it's equivalent to passing
  two of the same dates as a date range. You can also pass a string instead of
  array with relative date range, for example: `last quarter`, `last 360 days`,
  or `next 2 months`.
- `compareDateRange`: An array of date ranges to compare a measure change over
  previous period
- `granularity`: A granularity for a time dimension. It supports the following
  values `second`, `minute`, `hour`, `day`, `week`, `month`, `year`. If you pass
  `null` to the granularity, Cube.js will only perform filtering by a specified
  time dimension, without grouping.

```js
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}
```

You can use compare date range queries when you want to see, for example, how a
metric performed over a period in the past and how it performs now. You can pass
two or more date ranges where each of them is in the same format as a
`dateRange`

```js
// ...
const resultSet = cubejsApi.load({
  measures: ['Stories.count'],
  timeDimensions: [
    {
      dimension: 'Stories.time',
      compareDateRange: ['this week', ['2020-05-21', '2020-05-28']],
      granularity: 'month',
    },
  ],
});
// ...
```

You can also set a relative `dateRange`, e.g. `today`, `yesterday`, `tomorrow`,
`last year`, `next month`, or `last 6 months`.

```js
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: 'last week',
    granularity: 'day'
  }]
}
```

Be aware that e.g. `Last 7 days` or `Next 2 weeks` do not include the current
date. If you need the current date also you can use `from N days ago to now` or
`from now to N days from now`.

```js
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: 'from 6 days ago to now',
    granularity: 'day'
  }]
}
```
