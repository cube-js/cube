---
order: 3
title: "Rendering Chart with D3.js"
---

Now, as we can build our first chart, let’s inspect the example code playground uses to render it with the D3. Before that, we need to understand how Cube.js accepts and processes a query and returns the result back.

A Cube.js query is a simple JSON object containing several properties. The main properties of the query are `measures`, `dimensions`, `timeDimensions`, and `filters`. You can learn more about the [Cube.js JSON query format and its properties here](https://cube.dev/docs/query-format). You can always inspect the JSON query in the playground by clicking the **JSON Query** button next to the chart selector.

![](/images/3-screenshot-1.png)

Cube.js backend accepts this query and then uses it and the schema we created earlier to generate an SQL query. This SQL query will be executed in our database and the result will be sent back to the client.

Although Cube.js can be queried via plain HTTP REST API, we’re going to use the Cube.js JavaScript client library. Among other things it provides useful tools to process the data after it has been returned from the backend.

Once the data is loaded, the Cube.js client creates a `ResultSet` object, which provides a set of methods to access and manipulate the data. We’re going to use two of them now: `ResultSet.series` and `ResultSet.chartPivot`. You can learn about all the features of the [Cube.js client library in the docs](https://cube.dev/docs/@cubejs-client-core).

The `ResultSet.series` method returns an array of data series with key, title, and series data. The method accepts one argument—`pivotConfig`. It is an object, containing rules about how the data should be pivoted; we’ll talk about it a bit. In a line chart, each series is usually represented by a separate line. This method is useful for preparing data in the format expected by D3.

```javascript
// For query
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
    "key":"Stories.count",
    "title": "Stories Count",
    "series": [
      { "x":"2015-01-01T00:00:00", "value": 27120 },
      { "x":"2015-02-01T00:00:00", "value": 25861 },
      { "x": "2015-03-01T00:00:00", "value": 29661 },
      //...
    ]
  }
]
```

The next method we need is `ResultSet.chartPivot`. It accepts the same `pivotConfig` argument and returns an array of data with values for the X-axis and for every series we have.

```javascript
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

As mentioned above, the `pivotConfig` argument is an object for controlling how to transform, or pivot, data. The object has two properties: `x` and `y`, both are arrays. By adding measures or dimensions to one of them, you can control what goes to the X-axis and what goes to the Y-axis. For a query with one `measure` and one `timeDimension`, `pivotConfig` has the following default value:

```javascript
{
   x: `CubeName.myTimeDimension.granularity`,
   y: `measures`
}
```

Here, ‘measures’ is a special value, meaning that all the measures should go to the Y-axis. In most cases, the default value of the `pivotConfig` should work fine. In the next chapter, I’ll show you when and how we need to change it.

Now, let’s look at the frontend code playground generates when we select a D3 chart. Select a measure in the playground and change the visualization type to the D3. Next, click the **Code** to inspect the frontend code to render the chart.

![](/images/3-screenshot-2.png)

Here is the full source code from that page.

```jsx
import React from 'react';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin } from 'antd';

import * as d3 from 'd3';
const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

const draw = (node, resultSet, chartType) => {
  // Set the dimensions and margins of the graph
  const margin = {top: 10, right: 30, bottom: 30, left: 60},
    width = node.clientWidth - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;

  d3.select(node).html("");
  const svg = d3.select(node)
  .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");

  // Prepare data in D3 format
  const data = resultSet.series().map((series) => ({
    key: series.title, values: series.series
  }));

  // color palette
  const color = d3.scaleOrdinal()
    .domain(data.map(d => d.key ))
    .range(COLORS_SERIES)

  // Add X axis
  const x = d3.scaleTime()
    .domain(d3.extent(resultSet.chartPivot(), c => d3.isoParse(c.x)))
    .range([ 0, width ]);
  svg.append("g")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));

  // Add Y axis
  const y = d3.scaleLinear()
    .domain([0, d3.max(data.map((s) => d3.max(s.values, (i) => i.value)))])
    .range([ height, 0 ]);
  svg.append("g")
    .call(d3.axisLeft(y));

  // Draw the lines
  svg.selectAll(".line")
    .data(data)
    .enter()
    .append("path")
      .attr("fill", "none")
      .attr("stroke", d => color(d.key))
      .attr("stroke-width", 1.5)
      .attr("d", (d) => {
        return d3.line()
          .x(d => x(d3.isoParse(d.x)))
          .y(d => y(+d.value))
          (d.values)
      })

}

const lineRender = ({ resultSet }) => (
  <div ref={el => el && draw(el, resultSet, 'line')} />
)


const API_URL = "http://localhost:4000"; // change to your actual endpoint

const cubejsApi = cubejs(
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1NzkwMjU0ODcsImV4cCI6MTU3OTExMTg4N30.nUyJ4AEsNk9ks9C8OwGPCHrcTXyJtqJxm02df7RGnQU",
  { apiUrl: API_URL + "/cubejs-api/v1" }
);

const renderChart = (Component) => ({ resultSet, error }) => (
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) ||
  (<Spin />)
)

const ChartRenderer = () => <QueryRenderer
  query={{
    "measures": [
      "Orders.count"
    ],
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "month"
      }
    ],
    "filters": []
  }}
  cubejsApi={cubejsApi}
  render={renderChart(lineRender)}
/>;

export default ChartRenderer;
```

The React component that renders the chart is just a single line wrapping a `draw` function, which does the entire job.

```jsx
const lineRender = ({ resultSet }) => (
  <div ref={el => el && draw(el, resultSet, 'line')} />
)
```

There is a lot going on in this `draw` function. Although it renders a chart already, think about it as an example and a good starting point for customization. As we’ll work on our own dashboard in the next chapter, I’ll show you how to do it.

Feel free to click the **Edit** button and play around with the code in Code Sandbox.

![](/images/3-screenshot-3.png)
