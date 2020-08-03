import React from 'react';
import './App.css';
import './body.css';
import 'antd/dist/antd.css';
import { ApolloProvider } from '@apollo/react-hooks';
import { Layout } from 'antd';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import client from './graphql/client';
import Header from './components/Header';
import Dashboard from './components/Dashboard';
import WebSocketTransport from '@cubejs-client/ws-transport';

const API_URL = 'http://localhost:4000';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTY0NTIxNDUsImV4cCI6MTU5NjUzODU0NX0.kf7_kYKxS6y9FTpsyshlpzxxx2P9KY9xAijvGWlUpnE';
const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: CUBEJS_TOKEN,
    apiUrl: API_URL.replace('http', 'ws'),
  }),
});

const AppLayout = () => (
  <Layout
    style={{
      height: '100%',
    }}
  >
    <Header />
    <Layout.Content>
      <Dashboard />
    </Layout.Content>
  </Layout>
);

const App = () => (
  <CubeProvider cubejsApi={cubejsApi}>
    <ApolloProvider client={client}>
      <AppLayout></AppLayout>
    </ApolloProvider>
  </CubeProvider>
);

export default App;
