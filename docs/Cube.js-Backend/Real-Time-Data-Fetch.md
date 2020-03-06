---
title: Real-Time Data Fetch
permalink: /real-time-data-fetch
category: Cube.js Backend
menuOrder: 6
---

Most of databases supported by Cube.js are retroactive.
It means Cube.js should continuously poll for changes rather than receive updates as subscribed listener.
Cube.js provides convenient way to create such polling database subscriptions on your behalf.

## Web Sockets

To provide best real-time experience it's recommended to use Web Sockets transport instead of default http long polling.
Web sockets on backend can be enabled using `CUBEJS_WEB_SOCKETS` environment variable:

**.env:**
```
CUBEJS_WEB_SOCKETS=true
```

Clients can be switched to Web Sockets by passing `WebSocketTransport` to `CubejsApi` constructor:

```javascript
import cubejs from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';

const cubejsApi = cubejs({
  transport: new WebSocketTransport({ authorization: CUBEJS_TOKEN, apiUrl: 'ws://localhost:4000/' })
});
```

## Client Subscriptions

Multiple APIs are provided to support data subscription scenarios.

### Vanilla JavaScript

```javascript
import cubejs from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';

const cubejsApi = cubejs({
  transport: new WebSocketTransport({ authorization: CUBEJS_TOKEN, apiUrl: 'ws://localhost:4000/' })
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

### React hooks

```javascript
import { useCubeQuery } from '@cubejs-client/react';

const Chart = ({ query, cubejsApi }) => {
  const { resultSet, error, isLoading } = useCubeQuery(query, { subscribe: true, cubejsApi });
  
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

## Refresh Rate

As in case of regular data fetch real-time data fetch obeys [refreshKey refresh rules](caching#refresh-keys).
In order to provide desired refresh rate `refreshKey` should reflect changes of underlying data set as well it's querying time should be much less than desired refresh rate.
Please use [refreshKey every](cube#parameters-refresh-key) parameter to adjust refresh interval.

