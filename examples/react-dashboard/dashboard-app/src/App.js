import React from "react";
import { useLocation } from "react-router";
import { Route, Routes, Outlet } from "react-router-dom";
import { Layout } from "antd";
import { ApolloProvider } from '@apollo/client';
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import client from "./graphql/client";

import Header from './components/Header';
import ExplorePage from "./pages/ExplorePage";
import DashboardPage from "./pages/DashboardPage";

const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2MDY1OTA0NjEsImV4cCI6MTkyMjE2NjQ2MX0.DdY7GaiHsQWyTH_xkslHb17Cbc3yLFfMFwoEpx89JiA'

const cubejsApi = cubejs(token, {
  apiUrl: 'https://harsh-eel.aws-us-east-2.cubecloudapp.dev/cubejs-api/v1'
});

const AppLayout = ({ location, children }) => (
  <Layout style={{ height: "100%" }}>
    <Header location={location} />
    <Layout.Content>
      <Outlet/>
    </Layout.Content>
  </Layout>
);

const App = ({ children }) => {
  const location = useLocation();

  return (
    <CubeProvider cubejsApi={cubejsApi}>
        <ApolloProvider client={client} >
          <Routes>
            <Route path="/" element={<AppLayout location={location}/>}>
              <Route path="/" element={<DashboardPage />} />
              <Route path="/explore" element={<ExplorePage />} />
            </Route>
          </Routes>
          </ApolloProvider>
    </CubeProvider>
  );
};

export default App;
