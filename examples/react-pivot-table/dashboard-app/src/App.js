import 'antd/dist/antd.css';
import './App.css'
import React from 'react';
import '@ant-design/compatible';
import { ApolloProvider } from '@apollo/react-hooks';
import { Layout } from 'antd';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import client from './graphql/client';
import Grid from './components/Grid';

const API_URL = process.env.REACT_APP_API_URL;
const CUBEJS_TOKEN = process.env.REACT_APP_CUBEJS_TOKEN;
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});

const AppLayout = ({ children }) => (
  <Layout
    style={{
      height: '100%',
    }}
  >
    <Grid />
  </Layout>
);

const App = ({ children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <ApolloProvider client={client}>
      <AppLayout>{children}</AppLayout>
    </ApolloProvider>
  </CubeProvider>
);

export default App;
