---
title: Real-Time Data Fetch
permalink: /real-time-data-fetch
category: Frontend Integrations
menuOrder: 3
---

Most of databases supported by Cube are retroactive. It means Cube should
continuously poll for changes rather than receive updates as a subscribed
listener. Cube provides a convenient way to create such polling database
subscriptions on your behalf.

## Web Sockets

To provide the best real-time experience it's recommended to use Web Sockets
transport instead of the default http long polling. Web sockets on backend can
be enabled using `CUBEJS_WEB_SOCKETS` environment variable:

**.env:**

```bash
CUBEJS_WEB_SOCKETS=true
```

Clients can be switched to Web Sockets by passing `WebSocketTransport` to
`CubejsApi` constructor:

```javascript
import cubejs from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: CUBEJS_TOKEN,
    apiUrl: 'ws://localhost:4000/',
  }),
});
```

## Client Subscriptions

Multiple APIs are provided to support data subscription scenarios.

### <--{"id" : "Client Subscriptions"}--> Vanilla JavaScript

```javascript
import cubejs from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: CUBEJS_TOKEN,
    apiUrl: 'ws://localhost:4000/',
  }),
});

cubejsApi.subscribe(
  {
    measures: ['Logs.count'],
    timeDimensions: [
      {
        dimension: 'Logs.time',
        granularity: 'hour',
        dateRange: 'last 1440 minutes',
      },
    ],
  },
  options,
  (error, resultSet) => {
    if (!error) {
      // handle the update
    }
  }
);
```

### <--{"id" : "Client Subscriptions"}--> React hooks

```javascript
import { useCubeQuery } from '@cubejs-client/react';

const Chart = ({ query }) => {
  const { resultSet, error, isLoading } = useCubeQuery(query, {
    subscribe: true,
  });

  if (isLoading) {
    return <div>Loading...</div>;
  }

  if (error) {
    return <pre>{error.toString()}</pre>;
  }

  if (!resultSet) {
    return null;
  }

  return <LineChart resultSet={resultSet} />;
};
```

## Refresh Rate

As in the case of a regular data fetch, real-time data fetch obeys
[refreshKey refresh rules](caching#refresh-keys). In order to provide a desired
refresh rate `refreshKey` should reflect the rate of change of the underlying
data set; the querying time should also be much less than the desired refresh
rate. Please use the [refreshKey every](/schema/reference/cube#refresh-key)
parameter to adjust the refresh interval.
