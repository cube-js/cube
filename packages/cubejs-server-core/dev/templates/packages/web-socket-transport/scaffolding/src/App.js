import WebSocketTransport from '@cubejs-client/ws-transport';

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: CUBEJS_TOKEN,
    apiUrl: API_URL.replace('http', 'ws')
  })
});
