import './body.css';
import 'antd/dist/antd.css';
import React from "react";
import { Layout } from "antd";
import cubejs from '@cubejs-client/core';
import { CubeProvider } from "@cubejs-client/react";
import Header from "./components/Header";

const API_URL = undefined;

const CUBEJS_TOKEN = undefined;

const cubejsApi = cubejs(
  CUBEJS_TOKEN,
  { apiUrl: `${API_URL}/cubejs-api/v1` }
);

const AppLayout = ({ children }) => (
  <Layout
    style={{
      height: "100%"
    }}
  >
    <Header/>
    <Layout.Content>{children}</Layout.Content>
  </Layout>
);

const App = ({ children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <AppLayout>
      {children}
    </AppLayout>
  </CubeProvider>
);

export default App;
