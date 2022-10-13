import React, { useEffect } from "react";
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { Layout } from "antd";
import { AnalyticsBrowser } from '@segment/analytics-next';

import Header from "./components/Header";

import "./App.css";

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: process.env.REACT_APP_CUBEJS_TOKEN,
    apiUrl: process.env.REACT_APP_API_URL
  })
});

const analytics = AnalyticsBrowser.load({ writeKey: 'jGAwCyImvV290RaY6uit5tnq8TcDS9lr' });

const AppLayout = ({ children }) => (
  <Layout style={{ height: "100%" }}>
    <Header analytics={analytics} />
    <Layout.Content>{children}</Layout.Content>
  </Layout>
);



const App = ({ children }) => {
  useEffect(() => {
    analytics.page();
  })
  
  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <AppLayout>{children}</AppLayout>
    </CubeProvider>
  );
};

export default App;
