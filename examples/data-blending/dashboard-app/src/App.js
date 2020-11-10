import React from 'react';
import 'antd/dist/antd.css';
import { Layout } from 'antd';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import Header from './components/Header';
import Dashboard from './components/Dashboard';

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const App = () => (
  <CubeProvider cubejsApi={cubejsApi}>
    <Layout>
      <Header />
      <Layout.Content className='examples__container'>
        <Dashboard />
      </Layout.Content>
    </Layout>
  </CubeProvider>
);

export default App;
