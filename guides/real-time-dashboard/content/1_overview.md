---
order: 1
title: "Overview"
---

A real time dashboard is a dashboard that contains charts that are automatically updated with the most current data available. The typical use case is to load chart with some historical data first and then live update it as new data comes in. In this tutorial you will learn how to build such real time dashboards with only open
source tools and without any third-party services.

The main challenge of
building such a dashboard is to design a proper architecture to react to changes in
data all the the way up from database to the charts on the frontend. The part
from server to the fronetnd is a simple one, since we have a lot of technologies
and frameworks built to handle real time data updates. Going from database to
server is much trickier. The underlying problem is that most of the databases,
which are good for analytic workload, don't provide out-of-the-box way to
subscribe to changes in the data. Instead, they are designed to be polled.

DEMO

[Cube.js](https://github.com/cube-js/cube.js), which acts as a middleman between your database and analytics
dashboard, can provide a real-time WebSockets-based API for frontend, while
polling the database for changes in data.

![](/images/schema-1.png)

On the frontend, Cube.js provides an API to load initial historical data and
subscribe to all subsequent updates.

```javascript
import cubejs from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: CUBEJS_TOKEN,
    apiUrl: 'ws://localhost:4000/'
  })
});

cubejsApi.subscribe({
  measures: ['Logs.count'],
  timeDimensions: [{
    dimension: 'Logs.time',
    granularity: 'hour',
    dateRange: 'last 1440 minutes'
  }]
}, (e, result) => {
  if (e) {
    // handle new error
  } else {
    // handle new result set
  }
});
```

In our tutorial, we are going to use React as a frontend framework. Cube.js has a `@cubejs-client/react` package, which provides React components for easy integration of Cube.js into React app. It uses React hooks to load query and subscribe for changes.

```jsx
import { useCubeQuery } from '@cubejs-client/react';

const Chart = ({ query, cubejsApi }) => {
  const {
    resultSet,
    error,
    isLoading
  } = useCubeQuery(query, { subscribe: true, cubejsApi });

  if (isLoading) {
    return <div>Loading...</div>;
  }

  if (error) {
    return <pre>{error.toString()}</pre>;
  }

  if (!resultSet) {
    return null;
  }

  return <LineChart resultSet={resultSet}/>;
};
```

In this tutorial, I'll show you how to build real time dashboard either with
MongoDB or BigQuery. The same approach could be used for [any
databases, which Cube.js supports](https://cube.dev/docs/connecting-to-the-database).



