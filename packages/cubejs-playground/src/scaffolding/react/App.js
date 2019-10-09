import React from "react";
import { ApolloProvider } from '@apollo/react-hooks';
import { Layout } from "antd";
import client from "./graphql/client";
import Header from "./components/Header";

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
  <ApolloProvider client={client}>
    <AppLayout>
      {children}
    </AppLayout>
  </ApolloProvider>
);

export default App;
