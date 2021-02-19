import GameStock from './components/GameStock';
import './body.css';
import 'antd/dist/antd.css';
import React from 'react';
import '@ant-design/compatible';
import { ApolloProvider } from '@apollo/react-hooks';
import { Layout } from 'antd';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import client from './graphql/client';
import Header from './components/Header';
const API_URL = "http://localhost:4000";
const CUBEJS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MTM3MTMxNDEsImV4cCI6MTYxMzc5OTU0MX0.8E3GuqV84ng32qkMF1SD_LEvRNYYDsli6LvF1U1gtMQ";
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});

const AppLayout = ({
  children
}) => <Layout style={{
  height: '100%'
}}>
  <GameStock />
  </Layout>;

const App = ({
  children
}) => <CubeProvider cubejsApi={cubejsApi}>
    <ApolloProvider client={client}>
      <AppLayout>{children}</AppLayout>
    </ApolloProvider>
  </CubeProvider>;

export default App;
