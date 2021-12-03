---
title: '@cubejs-client/core'
permalink: /@cubejs-client-core
category: Frontend Integrations
subCategory: Reference
menuOrder: 2
---

Vanilla JavaScript Cube.js client.

## cubejs

>  **cubejs**(**apiToken**: string |  () => *Promise‹string›*, **options**: [CubeJSApiOptions](#types-cube-js-api-options)): *[CubejsApi](#cubejs-api)*

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
apiToken | string &#124;  () => *Promise‹string›* | [API token](security) is used to authorize requests and determine SQL database you're accessing. In the development mode, Cube.js Backend will print the API token to the console on startup. In case of async function `authorization` is updated for `options.transport` on each request. |
options | [CubeJSApiOptions](#types-cube-js-api-options) | - |

>  **cubejs**(**options**: [CubeJSApiOptions](#types-cube-js-api-options)): *[CubejsApi](#cubejs-api)*

## defaultHeuristics

>  **defaultHeuristics**(**newQuery**: [Query](#types-query), **oldQuery**: [Query](#types-query), **options**: [TDefaultHeuristicsOptions](#types-t-default-heuristics-options)): *any*

## defaultOrder

>  **defaultOrder**(**query**: [Query](#types-query)): *object*

## movePivotItem

>  **movePivotItem**(**pivotConfig**: [PivotConfig](#types-pivot-config), **sourceIndex**: number, **destinationIndex**: number, **sourceAxis**: TSourceAxis, **destinationAxis**: TSourceAxis): *[PivotConfig](#types-pivot-config)*

## CubejsApi

Main class for accessing Cube.js API

### <--{"id" : "CubejsApi"}-->  dryRun

>  **dryRun**(**query**: [Query](#types-query) | [Query](#types-query)[], **options?**: [LoadMethodOptions](#types-load-method-options)): *Promise‹[TDryRunResponse](#types-t-dry-run-response)›*

>  **dryRun**(**query**: [Query](#types-query) | [Query](#types-query)[], **options**: [LoadMethodOptions](#types-load-method-options), **callback?**: [LoadMethodCallback](#types-load-method-callback)‹[TDryRunResponse](#types-t-dry-run-response)›): *void*

Get query related meta without query execution

### <--{"id" : "CubejsApi"}-->  load

>  **load**(**query**: [Query](#types-query) | [Query](#types-query)[], **options?**: [LoadMethodOptions](#types-load-method-options)): *Promise‹[ResultSet](#result-set)›*

>  **load**(**query**: [Query](#types-query) | [Query](#types-query)[], **options?**: [LoadMethodOptions](#types-load-method-options), **callback?**: [LoadMethodCallback](#types-load-method-callback)‹[ResultSet](#result-set)›): *void*

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
query | [Query](#types-query) &#124; [Query](#types-query)[] | [Query object](query-format)  |
options? | [LoadMethodOptions](#types-load-method-options) | - |
callback? | [LoadMethodCallback](#types-load-method-callback)‹[ResultSet](#result-set)› | - |

### <--{"id" : "CubejsApi"}-->  meta

>  **meta**(**options?**: [LoadMethodOptions](#types-load-method-options)): *Promise‹[Meta](#meta)›*

>  **meta**(**options?**: [LoadMethodOptions](#types-load-method-options), **callback?**: [LoadMethodCallback](#types-load-method-callback)‹[Meta](#meta)›): *void*

Get meta description of cubes available for querying.

### <--{"id" : "CubejsApi"}-->  sql

>  **sql**(**query**: [Query](#types-query) | [Query](#types-query)[], **options?**: [LoadMethodOptions](#types-load-method-options)): *Promise‹[SqlQuery](#sql-query)›*

>  **sql**(**query**: [Query](#types-query) | [Query](#types-query)[], **options?**: [LoadMethodOptions](#types-load-method-options), **callback?**: [LoadMethodCallback](#types-load-method-callback)‹[SqlQuery](#sql-query)›): *void*

Get generated SQL string for the given `query`.

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
query | [Query](#types-query) &#124; [Query](#types-query)[] | [Query object](query-format)  |
options? | [LoadMethodOptions](#types-load-method-options) | - |
callback? | [LoadMethodCallback](#types-load-method-callback)‹[SqlQuery](#sql-query)› | - |

### <--{"id" : "CubejsApi"}-->  subscribe

>  **subscribe**(**query**: [Query](#types-query) | [Query](#types-query)[], **options**: [LoadMethodOptions](#types-load-method-options) | null, **callback**: [LoadMethodCallback](#types-load-method-callback)‹[ResultSet](#result-set)›): *void*

Allows you to fetch data and receive updates over time. See [Real-Time Data Fetch](real-time-data-fetch)

```js
cubejsApi.subscribe(
  {
    measures: ['Logs.count'],
    timeDimensions: [
      {
        dimension: 'Logs.time',
        granularity: 'hour',
        dateRange: 'last 1440 minutes',
      },
    ],
  },
  options,
  (error, resultSet) => {
    if (!error) {
      // handle the update
    }
  }
);
```

## HttpTransport

Default transport implementation.

### <--{"id" : "HttpTransport"}-->  HttpTransport constructor

>  **new HttpTransport**(**options**: [TransportOptions](#types-transport-options)): *[HttpTransport](#http-transport)*

### <--{"id" : "HttpTransport"}-->  request

>  **request**(**method**: string, **params**: any): () => *Promise‹any›*

*Implementation of ITransport*

## Meta

Contains information about available cubes and it's members.

### <--{"id" : "Meta"}-->  defaultTimeDimensionNameFor

>  **defaultTimeDimensionNameFor**(**memberName**: string): *string*

### <--{"id" : "Meta"}-->  filterOperatorsForMember

>  **filterOperatorsForMember**(**memberName**: string, **memberType**: [MemberType](#types-member-type) | [MemberType](#types-member-type)[]): *any*

### <--{"id" : "Meta"}-->  membersForQuery

>  **membersForQuery**(**query**: [Query](#types-query) | null, **memberType**: [MemberType](#types-member-type)): *[TCubeMeasure](#types-t-cube-measure)[] | [TCubeDimension](#types-t-cube-dimension)[] | [TCubeMember](#types-t-cube-member)[]*

Get all members of a specific type for a given query.
If empty query is provided no filtering is done based on query context and all available members are retrieved.

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
query | [Query](#types-query) &#124; null | context query to provide filtering of members available to add to this query  |
memberType | [MemberType](#types-member-type) | - |

### <--{"id" : "Meta"}-->  resolveMember

>  **resolveMember**‹**T**›(**memberName**: string, **memberType**: T | T[]): *object | [TCubeMemberByType](#types-t-cube-member-by-type)‹T›*

Get meta information for a cube member
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

**Type parameters:**

- **T**: *[MemberType](#types-member-type)*

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
memberName | string | Fully qualified member name in a form `Cube.memberName` |
memberType | T &#124; T[] | - |

## ProgressResult

### <--{"id" : "ProgressResult"}-->  stage

>  **stage**(): *string*

### <--{"id" : "ProgressResult"}-->  timeElapsed

>  **timeElapsed**(): *string*

## ResultSet

Provides a convenient interface for data manipulation.

### <--{"id" : "ResultSet"}-->  annotation

>  **annotation**(): *[QueryAnnotations](#types-query-annotations)*

### <--{"id" : "ResultSet"}-->  chartPivot

>  **chartPivot**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[ChartPivotRow](#types-chart-pivot-row)[]*

Returns normalized query result data in the following format.

You can find the examples of using the `pivotConfig` [here](#types-pivot-config)
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
When using `chartPivot()` or `seriesNames()`, you can pass `aliasSeries` in the [pivotConfig](#types-pivot-config)
to give each series a unique prefix. This is useful for `blending queries` which use the same measure multiple times.

```js
// For the queries
{
  measures: ['Stories.count'],
  timeDimensions: [
    {
      dimension: 'Stories.time',
      dateRange: ['2015-01-01', '2015-12-31'],
      granularity: 'month',
    },
  ],
},
{
  measures: ['Stories.count'],
  timeDimensions: [
    {
      dimension: 'Stories.time',
      dateRange: ['2015-01-01', '2015-12-31'],
      granularity: 'month',
    },
  ],
  filters: [
    {
      member: 'Stores.read',
      operator: 'equals',
      value: ['true'],
    },
  ],
},

// ResultSet.chartPivot({ aliasSeries: ['one', 'two'] }) will return
[
  {
    x: '2015-01-01T00:00:00',
    'one,Stories.count': 27120,
    'two,Stories.count': 8933,
    xValues: ['2015-01-01T00:00:00'],
  },
  {
    x: '2015-02-01T00:00:00',
    'one,Stories.count': 25861,
    'two,Stories.count': 8344,
    xValues: ['2015-02-01T00:00:00'],
  },
  {
    x: '2015-03-01T00:00:00',
    'one,Stories.count': 29661,
    'two,Stories.count': 9023,
    xValues: ['2015-03-01T00:00:00'],
  },
  //...
];
```

### <--{"id" : "ResultSet"}-->  decompose

>  **decompose**(): *Object*

Can be used when you need access to the methods that can't be used with some query types (eg `compareDateRangeQuery` or `blendingQuery`)
```js
resultSet.decompose().forEach((currentResultSet) => {
  console.log(currentResultSet.rawData());
});
```

### <--{"id" : "ResultSet"}-->  drillDown

>  **drillDown**(**drillDownLocator**: [DrillDownLocator](#types-drill-down-locator), **pivotConfig?**: [PivotConfig](#types-pivot-config)): *[Query](#types-query) | null*

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

In case when you want to add `order` or `limit` to the query, you can simply spread it

```js
// An example for React
const drillDownResponse = useCubeQuery(
   {
     ...drillDownQuery,
     limit: 30,
     order: {
       'Orders.ts': 'desc'
     }
   },
   {
     skip: !drillDownQuery
   }
 );
```

### <--{"id" : "ResultSet"}-->  pivot

>  **pivot**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[PivotRow](#types-pivot-row)[]*

Base method for pivoting [ResultSet](#result-set) data.
Most of the times shouldn't be used directly and [chartPivot](#result-set-chart-pivot)
or (tablePivot)[#table-pivot] should be used instead.

You can find the examples of using the `pivotConfig` [here](#types-pivot-config)
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

### <--{"id" : "ResultSet"}-->  query

>  **query**(): *[Query](#types-query)*

### <--{"id" : "ResultSet"}-->  rawData

>  **rawData**(): *T[]*

### <--{"id" : "ResultSet"}-->  serialize

>  **serialize**(): *Object*

Can be used to stash the `ResultSet` in a storage and restored later with [deserialize](#result-set-deserialize)

### <--{"id" : "ResultSet"}-->  series

>  **series**‹**SeriesItem**›(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[Series](#types-series)‹SeriesItem›[]*

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

### <--{"id" : "ResultSet"}-->  seriesNames

>  **seriesNames**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[SeriesNamesColumn](#types-series-names-column)[]*

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

### <--{"id" : "ResultSet"}-->  tableColumns

>  **tableColumns**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *[TableColumn](#types-table-column)[]*

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

### <--{"id" : "ResultSet"}-->  tablePivot

>  **tablePivot**(**pivotConfig?**: [PivotConfig](#types-pivot-config)): *Array‹object›*

Returns normalized query result data prepared for visualization in the table format.

You can find the examples of using the `pivotConfig` [here](#types-pivot-config)

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

### <--{"id" : "ResultSet"}-->  deserialize

> `static` **deserialize**‹**TData**›(**data**: Object, **options?**: Object): *[ResultSet](#result-set)‹TData›*

```js
import { ResultSet } from '@cubejs-client/core';

const resultSet = await cubejsApi.load(query);
// You can store the result somewhere
const tmp = resultSet.serialize();

// and restore it later
const resultSet = ResultSet.deserialize(tmp);
```

**Type parameters:**

- **TData**

**Parameters:**

Name | Type | Description |
------ | ------ | ------ |
data | Object | the result of [serialize](#result-set-serialize)  |
options? | Object | - |

### <--{"id" : "ResultSet"}-->  getNormalizedPivotConfig

> `static` **getNormalizedPivotConfig**(**query**: [PivotQuery](#types-pivot-query), **pivotConfig?**: Partial‹[PivotConfig](#types-pivot-config)›): *[PivotConfig](#types-pivot-config)*

## SqlQuery

### <--{"id" : "SqlQuery"}-->  rawQuery

>  **rawQuery**(): *[SqlData](#types-sql-data)*

### <--{"id" : "SqlQuery"}-->  sql

>  **sql**(): *string*

## ITransport

### <--{"id" : "ITransport"}-->  request

>  **request**(**method**: string, **params**: any): () => *Promise‹void›*

## Types

### <--{"id" : "Types"}-->  Annotation

Name | Type |
------ | ------ |
format? | "currency" &#124; "percent" &#124; "number" |
shortTitle | string |
title | string |
type | string |

### <--{"id" : "Types"}-->  BinaryFilter

Name | Type |
------ | ------ |
and? | [BinaryFilter](#types-binary-filter)[] |
dimension? | string |
member? | string |
operator | [BinaryOperator](#types-binary-operator) |
or? | [BinaryFilter](#types-binary-filter)[] |
values | string[] |

### <--{"id" : "Types"}-->  BinaryOperator

> **BinaryOperator**: *"equals" | "notEquals" | "contains" | "notContains" | "gt" | "gte" | "lt" | "lte" | "inDateRange" | "notInDateRange" | "beforeDate" | "afterDate"*

### <--{"id" : "Types"}-->  ChartPivotRow

Name | Type |
------ | ------ |
x | string |
xValues | string[] |

### <--{"id" : "Types"}-->  Column

Name | Type |
------ | ------ |
key | string |
series | [] |
title | string |

### <--{"id" : "Types"}-->  CubeJSApiOptions

Name | Type | Description |
------ | ------ | ------ |
apiUrl | string | URL of your Cube.js Backend. By default, in the development environment it is `http://localhost:4000/cubejs-api/v1` |
credentials? | "omit" &#124; "same-origin" &#124; "include" | - |
headers? | Record‹string, string› | - |
parseDateMeasures? | boolean | - |
pollInterval? | number | - |
transport? | [ITransport](#i-transport) | Transport implementation to use. [HttpTransport](#http-transport) will be used by default. |

### <--{"id" : "Types"}-->  DateRange

> **DateRange**: *string | [string, string]*

### <--{"id" : "Types"}-->  DrillDownLocator

Name | Type |
------ | ------ |
xValues | string[] |
yValues? | string[] |

### <--{"id" : "Types"}-->  Filter

> **Filter**: *[BinaryFilter](#types-binary-filter) | [UnaryFilter](#types-unary-filter)*

### <--{"id" : "Types"}-->  LoadMethodCallback

> **LoadMethodCallback**: *function*

### <--{"id" : "Types"}-->  LoadMethodOptions

Name | Type | Description |
------ | ------ | ------ |
progressCallback? |  | - |
mutexKey? | string | Key to store the current request's MUTEX inside the `mutexObj`. MUTEX object is used to reject orphaned queries results when new queries are sent. For example: if two queries are sent with the same `mutexKey` only the last one will return results. |
mutexObj? | Object | Object to store MUTEX |
subscribe? | boolean | Pass `true` to use continuous fetch behavior. |

### <--{"id" : "Types"}-->  LoadResponse

Name | Type |
------ | ------ |
pivotQuery | [PivotQuery](#types-pivot-query) |
queryType | [QueryType](#types-query-type) |
results | [LoadResponseResult](#types-load-response-result)‹T›[] |

### <--{"id" : "Types"}-->  LoadResponseResult

Name | Type |
------ | ------ |
annotation | [QueryAnnotations](#types-query-annotations) |
data | T[] |
lastRefreshTime | string |
query | [Query](#types-query) |

### <--{"id" : "Types"}-->  MemberType

> **MemberType**: *"measures" | "dimensions" | "segments"*

### <--{"id" : "Types"}-->  PivotConfig

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
aliasSeries? | string[] | Give each series a prefix alias. Should have one entry for each query:measure. See [chartPivot](#result-set-chart-pivot) |
fillMissingDates? | boolean &#124; null | If `true` missing dates on the time dimensions will be filled with `0` for all measures.Note: the `fillMissingDates` option set to `true` will override any **order** applied to the query |
x? | string[] | Dimensions to put on **x** or **rows** axis. |
y? | string[] | Dimensions to put on **y** or **columns** axis. |

### <--{"id" : "Types"}-->  PivotQuery

> **PivotQuery**: *[Query](#types-query) & object*

### <--{"id" : "Types"}-->  PivotRow

Name | Type |
------ | ------ |
xValues | Array‹string &#124; number› |
yValuesArray | Array‹[string[], number]› |

### <--{"id" : "Types"}-->  ProgressResponse

Name | Type |
------ | ------ |
stage | string |
timeElapsed | number |

### <--{"id" : "Types"}-->  Query

Name | Type |
------ | ------ |
dimensions? | string[] |
filters? | [Filter](#types-filter)[] |
limit? | number |
measures? | string[] |
offset? | number |
order? | [TQueryOrderObject](#types-t-query-order-object) &#124; [TQueryOrderArray](#types-t-query-order-array) |
renewQuery? | boolean |
segments? | string[] |
timeDimensions? | [TimeDimension](#types-time-dimension)[] |
timezone? | string |
ungrouped? | boolean |

### <--{"id" : "Types"}-->  QueryAnnotations

Name | Type |
------ | ------ |
dimensions | Record‹string, [Annotation](#types-annotation)› |
measures | Record‹string, [Annotation](#types-annotation)› |
timeDimensions | Record‹string, [Annotation](#types-annotation)› |

### <--{"id" : "Types"}-->  QueryOrder

> **QueryOrder**: *"asc" | "desc"*

### <--{"id" : "Types"}-->  QueryType

> **QueryType**: *"regularQuery" | "compareDateRangeQuery" | "blendingQuery"*

### <--{"id" : "Types"}-->  Series

Name | Type |
------ | ------ |
key | string |
series | T[] |
title | string |

### <--{"id" : "Types"}-->  SeriesNamesColumn

Name | Type |
------ | ------ |
key | string |
title | string |
yValues | string[] |

### <--{"id" : "Types"}-->  SqlApiResponse

Name | Type |
------ | ------ |
sql | [SqlData](#types-sql-data) |

### <--{"id" : "Types"}-->  SqlData

Name | Type |
------ | ------ |
aliasNameToMember | Record‹string, string› |
cacheKeyQueries | object |
dataSource | boolean |
external | boolean |
sql | [SqlQueryTuple](#types-sql-query-tuple) |

### <--{"id" : "Types"}-->  SqlQueryTuple

> **SqlQueryTuple**: *[string, boolean | string | number]*

### <--{"id" : "Types"}-->  TCubeDimension

> **TCubeDimension**: *[TCubeMember](#types-t-cube-member) & object*

### <--{"id" : "Types"}-->  TCubeMeasure

> **TCubeMeasure**: *[TCubeMember](#types-t-cube-member) & object*

### <--{"id" : "Types"}-->  TCubeMember

Name | Type |
------ | ------ |
name | string |
shortTitle | string |
title | string |
isVisible? | boolean |
type | [TCubeMemberType](#types-t-cube-member-type) |

### <--{"id" : "Types"}-->  TCubeMemberByType

> **TCubeMemberByType**: *T extends "measures" ? TCubeMeasure : T extends "dimensions" ? TCubeDimension : T extends "segments" ? TCubeSegment : never*

### <--{"id" : "Types"}-->  TCubeMemberType

> **TCubeMemberType**: *"time" | "number" | "string" | "boolean"*

### <--{"id" : "Types"}-->  TCubeSegment

> **TCubeSegment**: *Pick‹[TCubeMember](#types-t-cube-member), "name" | "shortTitle" | "title"›*

### <--{"id" : "Types"}-->  TDefaultHeuristicsOptions

Name | Type |
------ | ------ |
meta | [Meta](#meta) |
sessionGranularity? | [TimeDimensionGranularity](#types-time-dimension-granularity) |

### <--{"id" : "Types"}-->  TDryRunResponse

Name | Type |
------ | ------ |
normalizedQueries | [Query](#types-query)[] |
pivotQuery | [PivotQuery](#types-pivot-query) |
queryOrder | Array‹object› |
queryType | [QueryType](#types-query-type) |

### <--{"id" : "Types"}-->  TFlatFilter

Name | Type |
------ | ------ |
dimension? | string |
member? | string |
operator | [BinaryOperator](#types-binary-operator) |
values | string[] |

### <--{"id" : "Types"}-->  TQueryOrderArray

> **TQueryOrderArray**: *Array‹[string, [QueryOrder](#types-query-order)]›*

### <--{"id" : "Types"}-->  TQueryOrderObject

### <--{"id" : "Types"}-->  TableColumn

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

### <--{"id" : "Types"}-->  TimeDimension

> **TimeDimension**: *[TimeDimensionComparison](#types-time-dimension-comparison) | [TimeDimensionRanged](#types-time-dimension-ranged)*

### <--{"id" : "Types"}-->  TimeDimensionBase

Name | Type |
------ | ------ |
dimension | string |
granularity? | [TimeDimensionGranularity](#types-time-dimension-granularity) |

### <--{"id" : "Types"}-->  TimeDimensionComparison

> **TimeDimensionComparison**: *[TimeDimensionBase](#types-time-dimension-base) & object*

### <--{"id" : "Types"}-->  TimeDimensionGranularity

> **TimeDimensionGranularity**: *"second" | "minute" | "hour" | "day" | "week" | "month" | "year"*

### <--{"id" : "Types"}-->  TimeDimensionRanged

> **TimeDimensionRanged**: *[TimeDimensionBase](#types-time-dimension-base) & object*

### <--{"id" : "Types"}-->  TransportOptions

Name | Type | Description |
------ | ------ | ------ |
apiUrl | string | path to `/cubejs-api/v1` |
authorization | string | [jwt auth token](security) |
credentials? | "omit" &#124; "same-origin" &#124; "include" | - |
headers? | Record‹string, string› | custom headers |

### <--{"id" : "Types"}-->  UnaryFilter

Name | Type |
------ | ------ |
and? | [UnaryFilter](#types-unary-filter)[] |
dimension? | string |
member? | string |
operator | [UnaryOperator](#types-unary-operator) |
or? | [UnaryFilter](#types-unary-filter)[] |
values? | never |

### <--{"id" : "Types"}-->  UnaryOperator

> **UnaryOperator**: *"set" | "notSet"*
