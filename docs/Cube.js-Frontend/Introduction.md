---
title: Introduction
permalink: /frontend-introduction
category: Cube.js Frontend
---

Alongside with [REST API](rest-api) Cube.js comes with Javascript client and bindings for
popular frameworks such as React and Vue.

The client itself doesn't provide any visualizations and is designed to work with existing chart libraries. It provides set of methods to access Cube.js API and to work with query result.

## Installation

You can install Javascript  client with NPM or Yarn

```bash
$ npm install --save @cubejs-client/core
# or with Yarn
$ yarn add @cubejs-client/core
```

## Example Usage
First import `cubejs` from `@cubejs-client/core` and initiate client with your
Cube.js [API Token](security) and API URL. The default API URL for Cube.js Backend in development mode is `http://localhost:4000/cubejs-api/v1`.

Then, use [CubejsApi.load](http://localhost:8000/@cubejs-client-core#cubejs-api-load) to load data from the backend. The `load` method accepts a query, which is plain Javascript object. [Learn more about query format
here.](query-format)

Below example shows how to use Cube.js Javascript Client with [Echarts charting
library](http://echarts.apache.org).

```javascript
import cubejs from '@cubejs-client/core';
import echarts from 'echarts';

// initialize cubejs instance with API Token and API URL
const cubejsApi = cubejs(
  'YOUR-CUBEJS-API-TOKEN',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' },
);

// Load query for orders by created month in 2017 year
cubejsApi
  .load({
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        dateRange: ['2017-01-01', '2017-12-31'],
        granularity: 'month'
      }
    ]
  })
  .then(resultSet => {
    // initialize echarts instance with prepared DOM
    var myChart = echarts.init(document.getElementById('chart'));
    // draw chart
    myChart.setOption({
      xAxis: {
        data: resultSet.chartPivot().map(i => i.x)
      },
      yAxis: {},
      series: [
        {
          type: 'bar',
          data: resultSet.chartPivot().map(i => i['Orders.count'])
        }
      ]
    });
  });
```
