import React from 'react';
import 'antd/dist/antd.css';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import Dashboard from './components/Dashboard';

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const AppLayout = ({ children }) => (
  <div className="app">{children}</div>
);

const App = () => (
  <CubeProvider cubejsApi={cubejsApi}>
    <AppLayout>
      <Dashboard />
    </AppLayout>
  </CubeProvider>
);

export default App;
