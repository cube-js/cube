import React from "react";
import logo from "./logo.svg";
import "./App.css";
import "./body.css";
import "antd/dist/antd.css";
import { ApolloProvider } from "@apollo/react-hooks";
import { Layout } from "antd";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import client from "./graphql/client";
import Header from "./components/Header";
import Dashboard from "./components/Dashboard";

const API_URL = "http://localhost:4000";
const CUBEJS_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTQyMDYxNTEsImV4cCI6MTU5NDI5MjU1MX0.DYrEvATxGzn6xEEVrTzS2sEHmAyXJYi8ZCcWoeZBE-0";
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});

const AppLayout = () => (
  <Layout
    style={{
      height: "100%"
    }}
  >
    <Header />
    <Layout.Content style={{ padding: '40px' }}>
      <Dashboard />
    </Layout.Content>
  </Layout>
);

const App = ({ children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <ApolloProvider client={client}>
      <AppLayout></AppLayout>
    </ApolloProvider>
  </CubeProvider>
);

export default App;
