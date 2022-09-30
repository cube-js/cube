import '@ant-design/compatible';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { Layout } from 'antd';
import 'antd/dist/antd.css';
import React from 'react';
import './body.css';
import Header from './components/Header';

// Environment variables
const API_URL = process.env.REACT_APP_API_URL;
const CUBEJS_TOKEN = process.env.REACT_APP_CUBEJS_TOKEN;

console.log(process.env)

// Cube JS API
const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: CUBEJS_TOKEN,
    apiUrl: API_URL.replace('http', 'ws')
  })
});

// Main App Wrapper
const AppLayout = ({
  children
}) => <Layout style={{
  height: '100%',
  backgroundColor: '#F6EEE0'
}}>
    <Header />
    <Layout.Content>{children}</Layout.Content>
  </Layout>;

const App = ({
  children
}) => <CubeProvider cubejsApi={cubejsApi}>
    <AppLayout>{children}</AppLayout>
  </CubeProvider>;

export default App;