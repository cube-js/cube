---
title: '@cubejs-client/react'
permalink: /@cubejs-client-react
category: Cube.js Frontend
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

### Render Props

- `measurers`, `dimensions`, `segments`, `timeDimensions`, `filters` - arrays of
selected query builder members.

- `availableMeasures`, `availableDimensions`, `availableTimeDimensions`,
`availableSegments` - arrays of available to select members. They are loaded via
API from Cube.js Backend.

- `updateMeasures`, `updateDimensions`, `updateSegments`, `updateTimeDimensions` - objects with three functions: `add`, `remove`, and `update`. They are used to control the state of the query builder. Ex: `updateMeasures.add(newMeasure)`

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
