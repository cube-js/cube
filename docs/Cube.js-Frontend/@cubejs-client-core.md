---
title: '@cubejs-client/core'
permalink: /@cubejs-client-core
category: Cube.js Frontend
subCategory: Reference
menuOrder: 2
---

Vanilla JavaScript Cube.js client.

## cubejs

▸  **cubejs**(**apiToken**: string |  () => *Promise‹string›*, **options**: [CubeJSApiOptions](#types-cube-js-api-options)): *[CubejsApi](#cubejs-api)*

Creates an instance of the `CubejsApi`. The API entry point.

```js
import cubejs from '@cubejs-client/core';
const cubejsApi = cubejs(
  'CUBEJS-API-TOKEN',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
);
```

You can also pass an async function or a promise that will resolve to the API token

```js
import cubejs from '@cubejs-client/core';
const cubejsApi = cubejs(
  async () => await Auth.getJwtToken(),
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
);
```

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
apiToken | string &#124;  () => *Promise‹string›* | [API token](security) is used to authorize requests and determine SQL database you're accessing. In the development mode, Cube.js Backend will print the API token to the console on on startup. Can be an async function without arguments that returns the API token. |
options | [CubeJSApiOptions](#types-cube-js-api-options) | - |

▸  **cubejs**(**options**: [CubeJSApiOptions](#types-cube-js-api-options)): *[CubejsApi](#cubejs-api)*

## CubejsApi

Main class for accessing Cube.js API

### load

▸  **load**(**query**: [Query](#types-query), **options?**: [LoadMethodOptions](#types-load-method-options)): *Promise‹[ResultSet](#result-set)›*

▸  **load**(**query**: [Query](#types-query), **options?**: [LoadMethodOptions](#types-load-method-options), **callback?**: [LoadMethodCallback](#types-load-method-callback)‹[ResultSet](#result-set)›): *void*

Fetch data for the passed `query`.

```js
import cubejs from '@cubejs-client/core';
import Chart from 'chart.js';
import chartjsConfig from './toChartjsData';

const cubejsApi = cubejs('CUBEJS_TOKEN');

const resultSet = await cubejsApi.load({
 measures: ['Stories.count'],
 timeDimensions: [{
   dimension: 'Stories.time',
   dateRange: ['2015-01-01', '2015-12-31'],
   granularity: 'month'
  }]
});

const context = document.getElementById('myChart');
new Chart(context, chartjsConfig(resultSet));
```

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
query | [Query](#types-query) | [Query object](query-format)  |
options? | [LoadMethodOptions](#types-load-method-options) | - |
callback? | [LoadMethodCallback](#types-load-method-callback)‹[ResultSet](#result-set)› | - |

### meta

▸  **meta**(**options?**: [LoadMethodOptions](#types-load-method-options)): *Promise‹[Meta](#meta)›*

▸  **meta**(**options?**: [LoadMethodOptions](#types-load-method-options), **callback?**: [LoadMethodCallback](#types-load-method-callback)‹[Meta](#meta)›): *void*

Get meta description of cubes available for querying.

### sql

▸  **sql**(**query**: [Query](#types-query), **options?**: [LoadMethodOptions](#types-load-method-options)): *Promise‹[SqlQuery](#sql-query)›*

▸  **sql**(**query**: [Query](#types-query), **options?**: [LoadMethodOptions](#types-load-method-options), **callback?**: [LoadMethodCallback](#types-load-method-callback)‹[SqlQuery](#sql-query)›): *void*

Get generated SQL string for the given `query`.

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
query | [Query](#types-query) | [Query object](query-format)  |
options? | [LoadMethodOptions](#types-load-method-options) | - |
callback? | [LoadMethodCallback](#types-load-method-callback)‹[SqlQuery](#sql-query)› | - |

## HttpTransport

Default transport implementation.

### constructor

\+  **new HttpTransport**(**options**: [TransportOptions](#types-transport-options)): *[HttpTransport](#http-transport)*

### request

▸  **request**(**method**: string, **params**: any): () => *Promise‹any›*

*Implementation of ITransport*

## Meta

Contains information about available cubes and it's members.

### defaultTimeDimensionNameFor

▸  **defaultTimeDimensionNameFor**(**memberName**: string): *string*

### filterOperatorsForMember

▸  **filterOperatorsForMember**(**memberName**: string, **memberType**: [MemberType](#types-member-type)): *any*

### membersForQuery

▸  **membersForQuery**(**query**: [Query](#types-query), **memberType**: [MemberType](#types-member-type)): *any*

Get all members of a specific type for a given query.
If empty query is provided no filtering is done based on query context and all available members are retrieved.

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
query | [Query](#types-query) | context query to provide filtering of members available to add to this query  |
memberType | [MemberType](#types-member-type) | - |

### resolveMember

▸  **resolveMember**(**memberName**: string, **memberType**: [MemberType](#types-member-type)): *Object*

Get meta information for member of a cube
Member meta information contains:
```javascript
{
  name,
  title,
  shortTitle,
  type,
  description,
  format
}
```

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
memberName | string | Fully qualified member name in a form `Cube.memberName` |
memberType | [MemberType](#types-member-type) | - |

## ProgressResult

### stage

▸  **stage**(): *string*

### timeElapsed

▸  **timeElapsed**(): *string*

## ResultSet

Provides a convenient interface for data manipulation.

### constructor

\+  **new ResultSet**(**loadResponse**: [LoadResponse](#types-load-response)‹T›, **options?**: Object): *[ResultSet](#result-set)*

Creates a new instance of ResultSet based on [LoadResponse](#load-response) data.

```js
import cubejs, { ResultSet } from '@cubejs-client/core';

const cubejsApi = cubejs('CUBEJS_TOKEN');

const resultSet = await cubejsApi.load({
 measures: ['Stories.count'],
 timeDimensions: [{
   dimension: 'Stories.time',
   dateRange: ['2015-01-01', '2015-12-31'],
   granularity: 'month'
  }]
});

const copy = new ResultSet(resultSet.loadResponse);
```

### chartPivot

▸  **chartPivot**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[ChartPivotRow](#types-chart-pivot-row)[]*

Returns normalized query result data in the following format.

You can find the examples of using the `pivotConfig` [here](#pivot-config)
```js
// For the query
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}

// ResultSet.chartPivot() will return
[
  { "x":"2015-01-01T00:00:00", "Stories.count": 27120, "xValues": ["2015-01-01T00:00:00"] },
  { "x":"2015-02-01T00:00:00", "Stories.count": 25861, "xValues": ["2015-02-01T00:00:00"]  },
  { "x":"2015-03-01T00:00:00", "Stories.count": 29661, "xValues": ["2015-03-01T00:00:00"]  },
  //...
]
```

### drillDown

▸  **drillDown**(**drillDownLocator**: [DrillDownLocator](#types-drill-down-locator), **pivotConfig?**: [PivotConfig](#types-pivot-config)): *[Query](#types-query) | null*

Returns a measure drill down query.

Provided you have a measure with the defined `drillMemebers` on the `Orders` cube
```js
measures: {
  count: {
    type: `count`,
    drillMembers: [Orders.status, Users.city, count],
  },
  // ...
}
```

Then you can use the `drillDown` method to see the rows that contribute to that metric
```js
resultSet.drillDown(
  {
    xValues,
    yValues,
  },
  // you should pass the `pivotConfig` if you have used it for axes manipulation
  pivotConfig
)
```

the result will be a query with the required filters applied and the dimensions/measures filled out
```js
{
  measures: ['Orders.count'],
  dimensions: ['Orders.status', 'Users.city'],
  filters: [
    // dimension and measure filters
  ],
  timeDimensions: [
    //...
  ]
}
```

### pivot

▸  **pivot**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[PivotRow](#types-pivot-row)[]*

Base method for pivoting [ResultSet](#result-set) data.
Most of the times shouldn't be used directly and [chartPivot](#result-set-chart-pivot)
or (tablePivot)[#table-pivot] should be used instead.

You can find the examples of using the `pivotConfig` [here](#pivot-config)
```js
// For query
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-03-31'],
    granularity: 'month'
  }]
}

// ResultSet.pivot({ x: ['Stories.time'], y: ['measures'] }) will return
[
  {
    xValues: ["2015-01-01T00:00:00"],
    yValuesArray: [
      [['Stories.count'], 27120]
    ]
  },
  {
    xValues: ["2015-02-01T00:00:00"],
    yValuesArray: [
      [['Stories.count'], 25861]
    ]
  },
  {
    xValues: ["2015-03-01T00:00:00"],
    yValuesArray: [
      [['Stories.count'], 29661]
    ]
  }
]
```

### query

▸  **query**(): *[Query](#types-query)*

### rawData

▸  **rawData**(): *T[]*

### series

▸  **series**‹**SeriesItem**›(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[Series](#types-series)‹SeriesItem›[]*

Returns an array of series with key, title and series data.
```js
// For the query
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}

// ResultSet.series() will return
[
  {
    key: 'Stories.count',
    title: 'Stories Count',
    series: [
      { x: '2015-01-01T00:00:00', value: 27120 },
      { x: '2015-02-01T00:00:00', value: 25861 },
      { x: '2015-03-01T00:00:00', value: 29661 },
      //...
    ],
  },
]
```

**Type parameters:**

- **SeriesItem**

### seriesNames

▸  **seriesNames**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[SeriesNamesColumn](#types-series-names-column)[]*

Returns an array of series objects, containing `key` and `title` parameters.
```js
// For query
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}

// ResultSet.seriesNames() will return
[
  {
    key: 'Stories.count',
    title: 'Stories Count',
    yValues: ['Stories.count'],
  },
]
```

### tableColumns

▸  **tableColumns**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[TableColumn](#types-table-column)[]*

Returns an array of column definitions for `tablePivot`.

For example:
```js
// For the query
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}

// ResultSet.tableColumns() will return
[
  {
    key: 'Stories.time',
    dataIndex: 'Stories.time',
    title: 'Stories Time',
    shortTitle: 'Time',
    type: 'time',
    format: undefined,
  },
  {
    key: 'Stories.count',
    dataIndex: 'Stories.count',
    title: 'Stories Count',
    shortTitle: 'Count',
    type: 'count',
    format: undefined,
  },
  //...
]
```

In case we want to pivot the table axes
```js
// Let's take this query as an example
{
  measures: ['Orders.count'],
  dimensions: ['Users.country', 'Users.gender']
}

// and put the dimensions on `y` axis
resultSet.tableColumns({
  x: [],
  y: ['Users.country', 'Users.gender', 'measures']
})
```

then `tableColumns` will group the table head and return
```js
{
  key: 'Germany',
  type: 'string',
  title: 'Users Country Germany',
  shortTitle: 'Germany',
  meta: undefined,
  format: undefined,
  children: [
    {
      key: 'male',
      type: 'string',
      title: 'Users Gender male',
      shortTitle: 'male',
      meta: undefined,
      format: undefined,
      children: [
        {
          // ...
          dataIndex: 'Germany.male.Orders.count',
          shortTitle: 'Count',
        },
      ],
    },
    {
      // ...
      shortTitle: 'female',
      children: [
        {
          // ...
          dataIndex: 'Germany.female.Orders.count',
          shortTitle: 'Count',
        },
      ],
    },
  ],
},
// ...
```

### tablePivot

▸  **tablePivot**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *Array‹object›*

Returns normalized query result data prepared for visualization in the table format.

You can find the examples of using the `pivotConfig` [here](#pivot-config)

For example:
```js
// For the query
{
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}

// ResultSet.tablePivot() will return
[
  { "Stories.time": "2015-01-01T00:00:00", "Stories.count": 27120 },
  { "Stories.time": "2015-02-01T00:00:00", "Stories.count": 25861 },
  { "Stories.time": "2015-03-01T00:00:00", "Stories.count": 29661 },
  //...
]
```

### getNormalizedPivotConfig

▸ `static` **getNormalizedPivotConfig**(**query**: [Query](#types-query), **pivotConfig?**: Partial‹[PivotConfig](#types-pivot-config)›): *[PivotConfig](#types-pivot-config)*

## SqlQuery

### rawQuery

▸  **rawQuery**(): *[SqlData](#types-sql-data)*

### sql

▸  **sql**(): *string*

## ITransport

### request

▸  **request**(**method**: string, **params**: any): () => *Promise‹void›*

## Types

### Annotation

Name | Type |
------ | ------ |
format? | "currency" &#124; "percentage" |
shortTitle | string |
title | string |
type | string |

### ChartPivotRow

Name | Type |
------ | ------ |
x | string |
xValues | string[] |

### Column

Name | Type |
------ | ------ |
key | string |
series | [] |
title | string |

### CubeJSApiOptions

Name | Type | Description |
------ | ------ | ------ |
apiUrl | string | URL of your Cube.js Backend. By default, in the development environment it is `http://localhost:4000/cubejs-api/v1` |
credentials? | "omit" &#124; "same-origin" &#124; "include" | - |
headers? | Record‹string, string› | - |
pollInterval? | number | - |
transport? | [ITransport](#i-transport) | Transport implementation to use. [HttpTransport](#http-transport) will be used by default. |

### DrillDownLocator

Name | Type |
------ | ------ |
xValues | string[] |
yValues? | string[] |

### Filter

Name | Type |
------ | ------ |
dimension? | string |
member? | string |
operator | string |
values? | string[] |

### LoadMethodCallback

Ƭ **LoadMethodCallback**: *function*

### LoadMethodOptions

Name | Type | Description |
------ | ------ | ------ |
progressCallback? |  | - |
mutexKey? | string | Key to store the current request's MUTEX inside the `mutexObj`. MUTEX object is used to reject orphaned queries results when new queries are sent. For example: if two queries are sent with the same `mutexKey` only the last one will return results. |
mutexObj? | Object | Object to store MUTEX |
subscribe? | boolean | Pass `true` to use continuous fetch behavior. |

### LoadResponse

Name | Type |
------ | ------ |
annotation | [QueryAnnotations](#types-query-annotations) |
data | T[] |
lastRefreshTime | string |
query | [Query](#types-query) |

### MemberType

Ƭ **MemberType**: *"measures" | "dimensions" | "segments"*

### PivotConfig

Configuration object that contains information about pivot axes and other options.

Let's apply `pivotConfig` and see how it affects the axes
```js
// Example query
{
  measures: ['Orders.count'],
  dimensions: ['Users.country', 'Users.gender']
}
```
If we put the `Users.gender` dimension on **y** axis
```js
resultSet.tablePivot({
  x: ['Users.country'],
  y: ['Users.gender', 'measures']
})
```

The resulting table will look the following way

| Users Country | male, Orders.count | female, Orders.count |
| ------------- | ------------------ | -------------------- |
| Australia     | 3                  | 27                   |
| Germany       | 10                 | 12                   |
| US            | 5                  | 7                    |

Now let's put the `Users.country` dimension on **y** axis instead
```js
resultSet.tablePivot({
  x: ['Users.gender'],
  y: ['Users.country', 'measures'],
});
```

in this case the `Users.country` values will be laid out on **y** or **columns** axis

| Users Gender | Australia, Orders.count | Germany, Orders.count | US, Orders.count |
| ------------ | ----------------------- | --------------------- | ---------------- |
| male         | 3                       | 10                    | 5                |
| female       | 27                      | 12                    | 7                |

It's also possible to put the `measures` on **x** axis. But in either case it should always be the last item of the array.
```js
resultSet.tablePivot({
  x: ['Users.gender', 'measures'],
  y: ['Users.country'],
});
```

| Users Gender | measures     | Australia | Germany | US  |
| ------------ | ------------ | --------- | ------- | --- |
| male         | Orders.count | 3         | 10      | 5   |
| female       | Orders.count | 27        | 12      | 7   |

Name | Type | Description |
------ | ------ | ------ |
fillMissingDates? | boolean &#124; null | If `true` missing dates on the time dimensions will be filled with `0` for all measures.Note: the `fillMissingDates` option set to `true` will override any **order** applied to the query |
x? | string[] | Dimensions to put on **x** or **rows** axis. |
y? | string[] | Dimensions to put on **y** or **columns** axis. |

### PivotRow

Name | Type |
------ | ------ |
xValues | Array‹string &#124; number› |
yValuesArray | Array‹[string[], number]› |

### ProgressResponse

Name | Type |
------ | ------ |
stage | string |
timeElapsed | number |

### Query

Name | Type |
------ | ------ |
dimensions? | string[] |
filters? | [Filter](#types-filter)[] |
limit? | number |
measures? | string[] |
offset? | number |
order? | object |
renewQuery? | boolean |
segments? | string[] |
timeDimensions? | [TimeDimension](#types-time-dimension)[] |
timezone? | string |
ungrouped? | boolean |

### QueryAnnotations

Name | Type |
------ | ------ |
dimensions | Record‹string, [Annotation](#types-annotation)› |
measures | Record‹string, [Annotation](#types-annotation)› |
timeDimensions | Record‹string, [Annotation](#types-annotation)› |

### QueryOrder

Ƭ **QueryOrder**: *"asc" | "desc"*

### Series

Name | Type |
------ | ------ |
key | string |
series | T[] |
title | string |

### SeriesNamesColumn

Name | Type |
------ | ------ |
key | string |
title | string |
yValues | string[] |

### SqlApiResponse

Name | Type |
------ | ------ |
sql | [SqlData](#types-sql-data) |

### SqlData

Name | Type |
------ | ------ |
aliasNameToMember | Record‹string, string› |
cacheKeyQueries | object |
dataSource | boolean |
external | boolean |
sql | [SqlQueryTuple](#types-sql-query-tuple) |

### SqlQueryTuple

Ƭ **SqlQueryTuple**: *[string, boolean | string | number]*

### TableColumn

Name | Type |
------ | ------ |
children? | [TableColumn](#types-table-column)[] |
dataIndex | string |
format? | any |
key | string |
meta | any |
shortTitle | string |
title | string |
type | string &#124; number |

### TimeDimension

Name | Type |
------ | ------ |
dateRange? | string &#124; string[] |
dimension | string |
granularity? | [TimeDimensionGranularity](#types-time-dimension-granularity) |

### TimeDimensionGranularity

Ƭ **TimeDimensionGranularity**: *"hour" | "day" | "week" | "month" | "year"*

### TransportOptions

Name | Type | Description |
------ | ------ | ------ |
apiUrl | string | path to `/cubejs-api/v1` |
authorization | string | [jwt auth token](security) |
credentials? | "omit" &#124; "same-origin" &#124; "include" | - |
headers? | Record‹string, string› | custom headers |
