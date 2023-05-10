import React, { useEffect } from "react";
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { Layout } from "antd";
import Header from "./components/Header";
import tracker from "./tracker";
import "./App.css";

const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: process.env.REACT_APP_CUBEJS_TOKEN,
    apiUrl: process.env.REACT_APP_API_URL
  })
});

const AppLayout = ({ children }) => (
  <Layout style={{ height: "100%" }}>
    <Header />
    <Layout.Content>{children}</Layout.Content>
  </Layout>
);

const App = ({ children }) => {
  useEffect(() => tracker.pageview(), []);
  
  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <AppLayout>{children}</AppLayout>
    </CubeProvider>
  );
};

export default App;
