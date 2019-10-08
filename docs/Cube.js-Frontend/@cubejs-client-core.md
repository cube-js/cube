---
title: '@cubejs-client/core'
permalink: /@cubejs-client-core
category: Cube.js Frontend
subCategory: Reference
menuOrder: 2
---

Vanilla JavaScript Cube.js client.

## cubejs

`cubejs(apiToken, options)`

Create instance of `CubejsApi`.
API entry point.

```javascript
 import cubejs from '@cubejs-client/core';

 const cubejsApi = cubejs(
 'CUBEJS-API-TOKEN',
 { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
 );
 ```

**Parameters:**

- `apiToken` - [API token](security) is used to authorize requests and determine SQL database you're accessing.
In the development mode, Cube.js Backend will print the API token to the console on on startup.
- `options` - options object.
- `options.apiUrl` - URL of your Cube.js Backend.
By default, in the development environment it is `http://localhost:4000/cubejs-api/v1`.

**Returns:** [CubejsApi](#cubejs-api)

## CubejsApi

Main class for accessing Cube.js API

### load

`CubejsApi#load(query, options, callback)`

Fetch data for passed `query`.

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

- `query` - [Query object](query-format)
- `options`
- `callback`

**Returns:** `Promise` for [ResultSet](#result-set) if `callback` isn't passed

## ResultSet

Provides a convenient interface for data manipulation.

### chartPivot

`ResultSet#chartPivot(pivotConfig)`

Returns normalized query result data in the following format.

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

// ResultSet.chartPivot() will return
[
  { "x":"2015-01-01T00:00:00", "Stories.count": 27120 },
  { "x":"2015-02-01T00:00:00", "Stories.count": 25861 },
  { "x": "2015-03-01T00:00:00", "Stories.count": 29661 },
  //...
]
```

**Parameters:**

- `pivotConfig`



### tablePivot

`ResultSet#tablePivot(pivotConfig)`

Returns normalized query result data prepared for visualization in the table format.

For example

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

// ResultSet.tablePivot() will return
[
  { "Stories.time": "2015-01-01T00:00:00", "Stories.count": 27120 },
  { "Stories.time": "2015-02-01T00:00:00", "Stories.count": 25861 },
  { "Stories.time": "2015-03-01T00:00:00", "Stories.count": 29661 },
  //...
]
```

**Parameters:**

- `pivotConfig`

**Returns:** `Array` of pivoted rows

### tableColumns

`ResultSet#tableColumns(pivotConfig)`

Returns array of column definitions for `tablePivot`.

For example

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

// ResultSet.tableColumns() will return
[
  { key: "Stories.time", title: "Stories Time" },
  { key: "Stories.count", title: "Stories Count" },
  //...
]
```

**Parameters:**

- `pivotConfig`

**Returns:** `Array` of columns

### seriesNames

`ResultSet#seriesNames(pivotConfig)`

Returns the array of series objects, containing `key` and `title` parameters.

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
{ "key":"Stories.count", "title": "Stories Count" }
]
```

**Parameters:**

- `pivotConfig`

**Returns:** `Array` of series names

