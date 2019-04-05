---
title: '@cubejs-client/core'
permalink: /@cubejs-client-core
category: Cube.js Frontend
subCategory: Reference
menuOrder: 2
---

`@cubejs-client/core` is a Javascript client library to use with
Cube.js Backend.

## cubejs

Create instance of `CubejsApi`.

* `apiToken` - [API token](security) is used to authorize requests and determine SQL database you're accessing. In the development mode, Cube.js Backend will print the API token to the console on on startup.
* `options` - options object.
   * `apiUrl` - URL of your Cube.js Backend. By default, in the development environment it is http://localhost:4000/cubejs-api/v1.

```javascript
import cubejs from '@cubejs-client/core';

const cubejsApi = cubejs(
  'CUBEJS-API-TOKEN',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
);
```

## CubejsApi

### load

Fetch data for passed `query`. Returns promise for [ResultSet](#result-set) if `callback` isn't passed.

* `query` - analytic query. Learn more about it's format below.
* `options` - options object. Can be omitted.
    * `progressCallback(ProgressResult)` - pass function to receive real time query execution progress.
* `callback(err, ResultSet)` - result callback. If not passed `load()` will return promise.

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
})
const context = document.getElementById('myChart');
new Chart(context, chartjsConfig(resultSet));
```

## ResultSet
`ResultSet` provides a convient interface for data munipulation.

### chartPivot
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

### seriesNames

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
