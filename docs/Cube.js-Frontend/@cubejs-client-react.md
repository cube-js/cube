---
title: '@cubejs-client/react'
permalink: /@cubejs-client-react
category: Cube.js Frontend
subCategory: Reference
menuOrder: 3
---

`@cubejs-client/react` provides React Components for easy integration Cube.js
into React app.

## QueryRenderer

`<QueryRenderer />` React component takes a query, fetches the given query, and uses the render prop to render the resulting data.

### Props

- `query`: analytic query. [Learn more about it's format](query-format).
- `cubejsApi`: `CubejsApi` instance to use.
- `render({ resultSet, error, loadingState })`: output of this function will be rendered by `QueryRenderer`.
  - `resultSet`: A `resultSet` is an object containing data obtained from the query.  If this object is not defined, it means that the data is still being fetched. [ResultSet](@cubejs-client-core#result-set) object provides a convient interface for data munipulation.
  - `error`: Error will be defined if an error has occurred while fetching the query.
  - `loadingState`: Provides information about the state of the query loading.

## QueryBuilder
`<QueryBuilder />` is used to  build interactive analytics query builders. It abstracts state management and API calls to Cube.js Backend. It uses render prop technique and doesn’t render anything itself, but calls the render function instead.

### Props

- `query`: default query.
- `cubejsApi`: `CubejsApi` instance to use. Required.
- `defaultChartType`: default value of chart type. Default: 'line'.
- `render(renderProps)`: output of this function will be rendered by `QueryBuilder`.
- `setQuery(query)`: called by `QueryBuilder` when query state changed. Use it when state is maintained outside of `QueryBuilder` component.

### Render Props

- `measurers`, `dimensions`, `segments`, `timeDimensions`, `filters` - arrays of
selected query builder members.

- `availableMeasures`, `availableDimensions`, `availableTimeDimensions`,
`availableSegments` - arrays of available to select members. They are loaded via
API from Cube.js Backend.

- `updateMeasures`, `updateDimensions`, `updateSegments`, `updateTimeDimensions` - objects with three functions: `add`, `remove`, and `update`. They are used to control the state of the query builder. Ex: `updateMeasures.add(newMeasure)`
- `updateOrder` - similar to the previous update methods but specific for order manipulation. It provides `set(memberId, order)`, `update(orderObject)` and `reorder(sourceIndex, destinationIndex)` methods.
- `orderMembers` - an array of available order members with the active order direction. 

### Example
[Open in CodeSandbox](https://codesandbox.io/s/react-query-builder-with-cubejs-b40pq)
```js
// Ex: `orderMembers`
// [
//   { 
//     id: 'Users.country', 
//     title: 'Users Country', 
//     order: 'desc' 
//   },
//   //...
// ]

import React from 'react';
import ReactDOM from 'react-dom';
import { Button, Layout, Divider, Empty, Select, Row, Col } from 'antd';
import { QueryBuilder } from '@cubejs-client/react';
import cubejs from '@cubejs-client/core';
import 'antd/dist/antd.css';

import ChartRenderer from './ChartRenderer';

const API_URL = 'https://react-dashboard.cubecloudapp.dev';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTE3MDcxNDgsImV4cCI6MTU5NDI5OTE0OH0.n5jGLQJ14igg6_Hri_Autx9qOIzVqp4oYxmX27V-4T4';

const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});

const App = () => (
  <QueryBuilder
    query={{
      measures: ['Orders.count'],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          granularity: 'month',
          dateRange: ['2016-01-01', '2016-12-31']
        }
      ]
    }}
    cubejsApi={cubejsApi}
    render={({ resultSet, measures, orderMembers, updateOrder }) => {
      return (
        <Layout.Content style={{ padding: 20 }}>
          {orderMembers.map((orderMember, index) => (
            <Row gutter={[8, 16]} align="middle">
              <Col span={6}>{orderMember.title}</Col>

              <Col>
                <Select
                  value={orderMember.order}
                  placeholder="Select order"
                  onSelect={(order) => updateOrder.set(orderMember.id, order)}
                >
                  {['none', 'asc', 'desc'].map((order) => (
                    <Select.Option key={order} value={order}>
                      {order}
                    </Select.Option>
                  ))}
                </Select>
              </Col>

              <Col>
                <Button
                  onClick={() => {
                    index - 1 >= 0 && updateOrder.reorder(index, index - 1);
                  }}
                >
                  Move up
                </Button>

                <Button
                  onClick={() => {
                    index + 1 < orderMembers.length && updateOrder.reorder(index, index + 1);
                  }}
                >
                  Move down
                </Button>
              </Col>
            </Row>
          ))}
          <Divider />
          {measures.length > 0 ? (
            <ChartRenderer resultSet={resultSet} />
          ) : (
            <Empty description="Select measure or dimension to get started" />
          )}
        </Layout.Content>
      );
    }}
  />
);

const rootElement = document.getElementById('root');
ReactDOM.render(<App />, rootElement);
```

- `chartType` - string, containing currently selected chart type.
- `updateChartType` - function-setter for chart type.
- `isQueryPresent` - Bool indicating whether is query ready to be displayed or
    not.
- `query` - current query, based on selected members.
- `resultSet`, `error`, `loadingState` - same as `<QueryRenderer />` [render props.](#query-renderer-props)

### Example
[Open in CodeSandbox](https://codesandbox.io/s/z6r7qj8wm)
```jsx
import React from "react";
import ReactDOM from "react-dom";
import { Layout, Divider, Empty, Select } from "antd";
import { QueryBuilder } from "@cubejs-client/react";
import cubejs from "@cubejs-client/core";
import "antd/dist/antd.css";

import ChartRenderer from "./ChartRenderer";

const cubejsApi = cubejs(
"YOUR-CUBEJS-API-TOKEN",
 { apiUrl: "http://localhost:4000/cubejs-api/v1" }
);

const App = () => (
 <QueryBuilder
   query={{
     timeDimensions: [
       {
         dimension: "LineItems.createdAt",
         granularity: "month"
       }
     ]
   }}
   cubejsApi={cubejsApi}
   render={({ resultSet, measures, availableMeasures, updateMeasures }) => (
     <Layout.Content style={{ padding: "20px" }}>
       <Select
         mode="multiple"
         style={{ width: "100%" }}
         placeholder="Please select"
         onSelect={measure => updateMeasures.add(measure)}
         onDeselect={measure => updateMeasures.remove(measure)}
       >
         {availableMeasures.map(measure => (
           <Select.Option key={measure.name} value={measure}>
             {measure.title}
           </Select.Option>
         ))}
       </Select>
       <Divider />
       {measures.length > 0 ? (
         <ChartRenderer resultSet={resultSet} />
       ) : (
         <Empty description="Select measure or dimension to get started" />
       )}
     </Layout.Content>
   )}
 />
);

const rootElement = document.getElementById("root");
ReactDOM.render(<App />, rootElement);
```
