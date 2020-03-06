---
title: Query Format
permalink: /query-format
category: Cube.js Frontend
menuOrder: 1
---

Query is plain JavaScript object, describing an analytics query. The basic elements of query (query members) are `measures`, `dimensions`, and `segments`.

The query member format name is `CUBE_NAME.MEMBER_NAME`, for example dimension email in the Cube Users would have the following name `Users.email`.

In a case of dimension of type time granularity could be optionally added to the name, in the following format `CUBE_NAME.TIME_DIMENSION_NAME.GRANULARITY`, ex: `Stories.time.month`.

Supported granularities: `hour`, `day`, `week`, `month`.

## Query Properties

Query has the following properties:

- `measures`: An array of measures.
- `dimensions`: An array of dimensions.
- `filters`: An array of objects, describing filters. Learn about [filters format](#filters-format).
- `timeDimensions`: A convient way to specify a time dimension with a filter. It is an array of objects in [timeDimension format.](#time-dimensions-format)
- `segments`: An array of segments. Segment is a named filter, created in the Data Schema.
- `limit`: A row limit for your query. The default value is `10000`. The
    maximum allowed limit is `50000`.
- `offset`: Number of first rows to be skipped for your query. The default value is `0`.
- `order`: An object, where keys are measures or dimensions to order by and
    their corresponding values are either `asc` or `desc`. The order of the
    fields to order is based on the order of the keys in the object.
- `timezone`: All time based calculations performed within Cube.js are timezone-aware. Using this property you can set your desired timezone in [TZ Database Name](https://en.wikipedia.org/wiki/Tz_database) format, e.g.: `America/Los_Angeles`. The default value is `UTC`.
- `renewQuery`: If `renewQuery` is set to `true`, query will always refresh cache and return the latest data from the database. The default value is `false`.
- `ungrouped`: If `ungrouped` is set to `true` no `GROUP BY` statement will be added to the query and raw results after filtering and joining will be returned without grouping. 
By default `ungrouped` query requires to pass primary key as a dimension of every cube involved in query for security purpose.
To disable this behavior please see [allowUngroupedWithoutPrimaryKey](@cubejs-backend-server-core#options-reference-allow-ungrouped-without-primary-key) server option. 

```js
{
  measures: ['Stories.count'],
  dimensions: ['Stories.category'],
  filters: [{
    dimension: 'Stories.isDraft',
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

### Default order

If `order` property is not specified in the query, Cube.js sorts results by default using the following rules:

- The first time dimension with granularity, ascending. If no time dimension with granularity exists...
- The first measure, descending. If no measure exists...
- The first dimension, ascending.

## Filters Format

A filter is a Javascript object with the following properties:

- `member`: Dimension or measure to be used in the filter, for example: `Stories.isDraft`. See below on difference on filtering dimensions vs filtering measures.
- `operator`: An operator to be used in filter. Only some operators are available for measures, for dimensions available operators depend on the type
    of the dimension. Please see the reference below for the full list of available
    operators.
- `values`: An array of values for the filter. Values must be of type String. If you need to pass a date, pass it as a string in `YYYY-MM-DD`.

#### Filtering Dimensions vs Filtering Measures
Filters are applied differently to dimensions and measures.

When you filter on a dimension, you are restricting the raw data before any calculations are made.   
When you filter on a measure, you are restricting the results after the measure has been calculated.

## Filters Operators
Only some operators are available for measures. For dimensions available operators are depend on the [type of the dimension](types-and-formats#dimensions-types).

### equals

Use it when you need an exact match. It supports multiple values.

* Applied to measures.
* Dimension types: `string`, `number`, `time`.

```js
{
  member: "Users.country",
  operator: "equals",
  values: ["US", "Germany", "Israel"]
}
```

### notEquals

An opposite operator of `equals`. It supports multiple values.

* Applied to measures.
* Dimension types: `string`, `number`, `time`.

```js
{
  member: "Users.country",
  operator: "notEquals",
  values: ["France"]
}
```

### contains

`contains` filter acts as a wildcard case insensitive `LIKE` operator. In the majority of SQL backends it uses `ILIKE` operator with values being surrounded by `%`. It supports multiple values.

* Dimension types: `string`.

```js
{
  member: "Posts.title",
  operator: "contains",
  values: ["serverless", "aws"]
}
```

### notContains

An opposite operator of `contains`. It supports multiple values.

* Dimension types: `string`.

```js
{
  member: "Posts.title",
  operator: "notContains",
  values: ["ruby"]
}
```

### gt

The `gt` operator means **greater than** and is used with measures or dimensions of type number.

* Applied to measures.
* Dimension types: `number`.

```js
{
  member: "Posts.upvotesCount",
  operator: "gt",
  values: ["100"]
}
```

### gte

The `gte` operator means **greater than or equal to** and is used with measures or dimensions of type number.

* Applied to measures.
* Dimension types: `number`.

```js
{
  member: "Posts.upvotesCount",
  operator: "gte",
  values: ["100"]
}
```

### lt

The `lt` operator means **less than** and is used with measures or dimensions of type number.

* Applied to measures.
* Dimension types: `number`.

```js
{
  member: "Posts.upvotesCount",
  operator: "lt",
  values: ["10"]
}
```

### lte

The `lte` operator means **less than or equal to** and is used with measures or dimensions of type number.

* Applied to measures.
* Dimension types: `number`.

```js
{
  member: "Posts.upvotesCount",
  operator: "lte",
  values: ["10"]
}
```

### set

Operator `set` checks whether the value of the member **is not** `NULL`. You don't
need to pass `values` for this operator.

* Applied to measures.
* Dimension types: `number`, `string`, `time`.

```js
{
  member: "Posts.authorName",
  operator: "set"
}
```

### notSet

An opposite to `set` operator. It checks whether the value of the member **is** `NULL`. You don't
need to pass `values` for this operator.

* Applied to measures.
* Dimension types: `number`, `string`, `time`.

```js
{
  member: "Posts.authorName",
  operator: "notSet"
}
```

### inDateRange

Operator `inDateRange` used to filter a time dimension into specific date range. The values must be an array of dates with following format '2015-01-01'. If only one date specified the filter would be set exactly to this date.

There is a convient way to use date filters with grouping - [learn more about
timeDimensions query property here](#time-dimensions-format)

* Dimension types: `time`.

```js
{
  member: "Posts.time",
  operator: "inDateRange",
  values: ['2015-01-01', '2015-12-31']
}
```

### notInDateRange

An opposite operator to `inDateRange`, use it when you want to exclude specific dates. The values format is the same as for `inDateRange`.

* Dimension types: `time`.

```js
{
  member: "Posts.time",
  operator: "notInDateRange",
  values: ['2015-01-01', '2015-12-31']
}
```

### beforeDate

Use it when you want to retreive all results before some specific date. The
values should be an array of one element in `YYYY-MM-DD` format.

* Dimension types: `time`.

```js
{
  member: "Posts.time",
  operator: "beforeDate",
  values: ['2015-01-01']
}
```

### afterDate

The same as `beforeDate`, but used to get all results after specific date.


* Dimension types: `time`.

```js
{
  member: "Posts.time",
  operator: "afterDate",
  values: ['2015-01-01']
}
```


## Time Dimensions Format

Since grouping and filtering by a time dimension is quite a common case, Cube.js provides a convient shortcut to pass a dimension and a filter as a `timeDimension` property.

  - `dimension`: Time dimension name.
  - `dateRange`: An array of dates with the following format `YYYY-MM-DD` or in `YYYY-MM-DDTHH:mm:ss.SSS` format.
Dates in `YYYY-MM-DD` format padded to start and end of day if used in start and end of date range interval accordingly. 
If only one date specified it's equivalent to passing two same dates as a date range.
You can also pass a string instead of array with relative date range, for example: `last quarter` or `last 360 days`.
  - `granularity`: A granularity for a time dimension. It supports following values `hour`, `day`, `week`, `month`, `year`. If you pass `null` to the granularity, the Cube.js will only perform a filtering by specified time dimension, without grouping.

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

You can also set relative `dateRange`, e.g. `today`, `yesterday`, `last
year`, or `last 6 months`.

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

Be aware that e.g. `Last 7 days` does not include the current date. If you need the current date also you can use `from N days ago to now`.

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
