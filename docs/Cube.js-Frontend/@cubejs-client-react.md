---
title: '@cubejs-client/react'
permalink: /@cubejs-client-react
category: Cube.js Frontend
---

`@cubejs-client/react` provides React Components for easy integration Cube.js
into React app.

## QueryRenderer

`<QueryRenderer />` React component takes a query, fetches the given query, and uses the render prop to render the resulting data.

Properties:

- `query`: analytic query. Learn more about it's format below.
- `cubejsApi`: `CubejsApi` instance to use.
- `render({ resultSet, error, loadingState })`: output of this function will be rendered by `QueryRenderer`.
  - `resultSet`: A `resultSet` is an object containing data obtained from the query.  If this object is not defined, it means that the data is still being fetched. [ResultSet](@cubejs-client-core#result-set) object provides a convient interface for data munipulation.
  - `error`: Error will be defined if an error has occurred while fetching the query.
  - `loadingState`: Provides information about the state of the query loading.
  
