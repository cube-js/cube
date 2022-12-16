---
title: Introduction for React Developers
menuTitle: Introduction
frameworkOfChoice: react
permalink: /frontend-introduction/react
category: Frontend Integrations
---

Cube is headless business intelligence for building data applications.
Cube is visualization-agnostic, so you can build any user interface for your
application.

You can directly query Cube Backend using
JSON [Query Format](https://cube.dev/docs/backend/rest/reference/query-format) via [HTTP API](https://cube.dev/docs/backend/rest/reference/api)
or [WebSockets](https://cube.dev/docs/real-time-data-fetch#web-sockets) and
visualize analytical data with tools of your choice. However, it's much easier
to use the Cube JavaScript client and bindings for popular frameworks such as
React, Angular, and Vue.

The client has methods to communicate with Cube API Gateway and retrieve and
process data. It is designed to work with existing charting libraries including
Chart.js, D3.js, and more.

## Cube JavaScript Client

The client provides methods to solve common tasks:

**Abstract from the transport and query data.** You can
[fetch data](https://cube.dev/docs/@cubejs-client-core#load) from Cube
Backend or subscribe to
[real-time updates](https://cube.dev/docs/real-time-data-fetch) regardless of
the protocol, be it HTTP or WebSockets.

**Transform data for visualization.** You can
[pivot](https://cube.dev/docs/@cubejs-client-core#pivot) the result set to
display as a [chart](https://cube.dev/docs/@cubejs-client-core#chart-pivot) or
as a [table](https://cube.dev/docs/@cubejs-client-core#table-pivot), split into
[series](https://cube.dev/docs/@cubejs-client-core#series) or
[table columns](https://cube.dev/docs/@cubejs-client-core#table-columns).

**Simplify work with complex query types.** You can build
[Drill Down](https://cube.dev/docs/@cubejs-client-core#drill-down) queries and
[decompose](https://cube.dev/docs/@cubejs-client-core#decompose) the results of
[compareDateRange](https://cube.dev/docs/backend/rest/reference/query-format#time-dimensions-format)
or [Data Blending](https://cube.dev/docs/recipes/data-blending) queries.

[Learn more](https://cube.dev/docs/@cubejs-client-core) in the documentation for
the `@cubejs-client/core` package.

## Cube React Package

The package provides convenient tools to work with Cube in React:

**Hooks.** You can add the
[useCubeQuery hook](https://cube.dev/docs/@cubejs-client-react#use-cube-query)
to functional React components to execute Cube queries.

**Components.** You can use
[QueryBuilder](https://cube.dev/docs/@cubejs-client-react#query-builder) and
[QueryRenderer](https://cube.dev/docs/@cubejs-client-react#query-renderer)
components to separate state management and API calls from your rendering code.
You can also use
[CubeProvider](https://cube.dev/docs/@cubejs-client-react#cube-provider) and
[CubeContext](https://cube.dev/docs/@cubejs-client-react#cube-context)
components for direct access to Cube Client anywhere in your application.

## Example Usage

Here are the typical steps to query and visualize analytical data in React:

- **Import `@cubejs-client/core` and `@cubejs-client/react` packages.** These
  packages provide all the necessary methods and convenient React tools.
- **Create an instance of Cube JavaScript Client.** The client is initialized
  with Cube API URL. In development mode, the default URL is
  [http://localhost:4000/cubejs-api/v1](http://localhost:4000/cubejs-api/v1).
  The client is also initialized with an
  [API token](https://cube.dev/docs/security), but it takes effect only in
  [production](https://cube.dev/docs/deployment/production-checklist).
- **Query data from Cube Backend.** In functional React components, use the
  `useCubeQuery` hook to execute a query, which is a plain JavaScript object.
  See [Query Format](https://cube.dev/docs/backend/rest/reference/query-format)
  for details.
- **Transform data for visualization.** The result set has convenient methods,
  such as `series` and `chartPivot`, to prepare data for charting.
- **Visualize the data.** Use tools of your choice to draw charts and create
  visualizations.

See an example of using Cube with React and Chart.js library. Note that you
can always use a different charting library that suits your needs:

<iframe src="https://codesandbox.io/embed/cube-js-react-client-39fzi?fontsize=14&hidenavigation=1&theme=dark" style="width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;" sandbox="allow-modals allow-forms allow-popups allow-scripts allow-same-origin"></iframe>

## Getting Started

You can install Cube JavaScript Client and the React package with npm or
Yarn:

```bash
# npm
$ npm install --save @cubejs-client/core @cubejs-client/react

# Yarn
$ yarn add @cubejs-client/core @cubejs-client/react
```

Now you can build your project from scratch or generate a
[sample dashboard application](https://cube.dev/docs/dashboard-app/) with
Cube Playground. You can also
[explore example applications](https://cube.dev/docs/examples) built with
Cube.
