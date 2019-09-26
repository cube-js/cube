import React from "react";
import { Link } from "react-router-dom";
import { withRouter } from "react-router";
import { ApolloProvider } from '@apollo/react-hooks';
import "antd/dist/antd.css";
import "./index.css";
import { Layout, Menu } from "antd";
import { client } from "./DashboardStore";

const AppLayout = ({ location, children }) => (
  <Layout
    style={{
      height: "100%"
    }}
  >
    <Layout.Header
      style={{
        padding: "0 32px"
      }}
    >
      <div
        style={{
          float: "left"
        }}
      >
        <h2
          style={{
            color: "#fff",
            margin: 0,
            marginRight: "1em",
            display: "inline",
            width: 100,
            lineHeight: "54px"
          }}
        >
          My Dashboard
        </h2>
      </div>
      <Menu
        theme="dark"
        mode="horizontal"
        selectedKeys={[location.pathname]}
        style={{
          lineHeight: "64px"
        }}
      >
        <Menu.Item key="/explore">
          <Link to="/explore">Explore</Link>
        </Menu.Item>
        <Menu.Item key="/">
          <Link to="/">Dashboard</Link>
        </Menu.Item>
      </Menu>
    </Layout.Header>
    <Layout.Content>{children}</Layout.Content>
  </Layout>
);

const App = withRouter(({ location, children }) => (
  <ApolloProvider client={client}>
    <AppLayout location={location}>
      {children}
    </AppLayout>
  </ApolloProvider>
));

export default App;
