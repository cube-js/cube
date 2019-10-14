import React from "react";
import { ApolloProvider } from '@apollo/react-hooks';
import { Layout } from "antd";
import cubejs from '@cubejs-client/core';
import { CubeProvider } from "@cubejs-client/react";
import client from "./graphql/client";
import Header from "./components/Header";

// change to your actual endpoint
const API_URL = undefined;
// should be refreshed by your backend API endpoint. More info: https://cube.dev/docs/security
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
    <ApolloProvider client={client}>
      <AppLayout>
        {children}
      </AppLayout>
    </ApolloProvider>
  </CubeProvider>
);

export default App;
