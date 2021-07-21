---
title: Introduction for React Developers
menuTitle: Introduction
frameworkOfChoice: react
permalink: /frontend-introduction/react
category: Cube.js Frontend
---

Cube.js is an open-source analytical API platform, and it enables you to build internal business intelligence tools or add customer‑facing analytics to existing applications. Cube.js is visualization-agnostic, so you can build any user interface for your application.

You can directly query Cube.js Backend using JSON [Query Format](https://cube.dev/docs/query-format) via [HTTP API](https://cube.dev/docs/rest-api) or [WebSockets](https://cube.dev/docs/real-time-data-fetch#web-sockets) and visualize analytical data with tools of your choice. However, it's much easier to use Cube.js JavaScript Client and bindings for popular frameworks such as React, Angular, and Vue.

The client has methods to communicate with Cube.js API Gateway, retrieve, and process data. It is designed to work with existing charting libraries such as Chart.js, D3.js, and more.

## Cube.js JavaScript Client

The client provides methods to solve common tasks:

**Abstract from the transport and query data.** You can [fetch data](https://cube.dev/docs/@cubejs-client-core#cubejs-api-load)  from Cube.js Backend or subscribe to [real-time updates](https://cube.dev/docs/real-time-data-fetch) regardless of the protocol, be it HTTP or WebSockets.

**Transform data for visualization.** You can [pivot](https://cube.dev/docs/@cubejs-client-core#result-set-pivot) the result set to display as a [chart](https://cube.dev/docs/@cubejs-client-core#result-set-chart-pivot) or as a [table](https://cube.dev/docs/@cubejs-client-core#result-set-table-pivot), split into [series](https://cube.dev/docs/@cubejs-client-core#result-set-series) or [table columns](https://cube.dev/docs/@cubejs-client-core#result-set-table-columns).

**Simplify work with complex query types.** You can build [Drill Down](https://cube.dev/docs/@cubejs-client-core#result-set-drill-down) queries and [decompose](https://cube.dev/docs/@cubejs-client-core#result-set-decompose) the results of [compareDateRange](https://cube.dev/docs/query-format#time-dimensions-format) or [Data Blending](https://cube.dev/docs/data-blending) queries.

[Learn more](https://cube.dev/docs/@cubejs-client-core) in the documentation for the `@cubejs-client/core` package.

## Cube.js React Package

The package provides convenient tools to work with Cube.js in React:

**Hooks.** You can add the [useCubeQuery hook](https://cube.dev/docs/@cubejs-client-react#use-cube-query) to functional React components to execute Cube.js queries.

**Components.** You can use [QueryBuilder](https://cube.dev/docs/@cubejs-client-react#query-builder) and [QueryRenderer](https://cube.dev/docs/@cubejs-client-react#query-renderer) components to separate state management and API calls from your rendering code. You can also use [CubeProvider](https://cube.dev/docs/@cubejs-client-react#cube-provider) and [CubeContext](https://cube.dev/docs/@cubejs-client-react#cube-context) components for direct access to Cube.js Client anywhere in your application.

## Example Usage

Here are the typical steps to query and visualize analytical data in React:

- **Import `@cubejs-client/core` and `@cubejs-client/react` packages.** These packages provide all the necessary methods and convenient React tools.
- **Create an instance of Cube.js JavaScript Client.** The client is initialized with Cube.js API URL. In development mode, the default URL is [http://localhost:4000/cubejs-api/v1](http://localhost:4000/cubejs-api/v1). The client is also initialized with an [API token](https://cube.dev/docs/security), but it takes effect only in [production](https://cube.dev/docs/deployment/production-checklist).
- **Query data from Cube.js Backend.** In functional React components, use the `useCubeQuery` hook to execute a query, which is a plain JavaScript object. See [Query Format](https://cube.dev/docs/query-format) for details.
- **Transform data for visualization.** The result set has convenient methods, such as `series` and `chartPivot`, to prepare data for charting.
- **Visualize the data.** Use tools of your choice to draw charts and create visualizations.

See an example of using Cube.js with React and Chart.js library. Note that you can always use a different charting library that suits your needs:

<iframe src="https://codesandbox.io/embed/cubejs-react-client-5y9s1?fontsize=14&hidenavigation=1&theme=dark&view=preview" style="width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;" sandbox="allow-modals allow-forms allow-popups allow-scripts allow-same-origin"></iframe>

## Getting Started

You can install Cube.js JavaScript Client and the React package with npm or Yarn:

```bash
# npm
$ npm install --save @cubejs-client/core @cubejs-client/react

# Yarn
$ yarn add @cubejs-client/core @cubejs-client/react
```

Now you can build your project from scratch or generate a [sample dashboard application](https://cube.dev/docs/dashboard-app/) with Cube.js Playground. You can also [explore example applications](https://cube.dev/docs/examples) built with Cube.js.
