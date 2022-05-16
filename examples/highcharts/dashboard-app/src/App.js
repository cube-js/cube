import React from 'react';
import 'antd/dist/antd.css';
import { ApolloProvider } from '@apollo/react-hooks';
import { Layout } from 'antd';
import cubejs from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { CubeProvider } from '@cubejs-client/react';
import client from './graphql/client';
import Dashboard from './components/Dashboard';

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: process.env.REACT_APP_CUBEJS_TOKEN,
    apiUrl: process.env.REACT_APP_API_URL
  })
});

const AppLayout = () => (
  <Layout>
    <Layout.Content className='examples__container'>
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
