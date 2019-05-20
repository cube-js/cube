import React from "react";
import { Link, matchPath, withRouter } from 'react-router-dom'
import { Layout, Breadcrumb, Menu } from "antd";
import "antd/dist/antd.css";
import GithubCorner from 'react-github-corner';
const { Header, Content } = Layout;

function App({ children, location }) {
  const match = matchPath(location.pathname, {
    path: "/stories/:storyId",
    exact: true,
    strict: false
  });
  return (
    <div className="App">
      <Layout>
        <Header>
          <div style={{ float: 'left' }}>
            <h2
              style={{
                color: "#fff",
                margin: 0,
                marginRight: '1em'
              }}
            >
              HN Insights
            </h2>
          </div>
          <Menu
            theme="dark"
            mode="horizontal"
            selectedKeys={[location.pathname]}
            style={{ lineHeight: '64px' }}
          >
            <Menu.Item key="/"><Link to="/">Track Stories</Link></Menu.Item>
            <Menu.Item key="/statistics"><Link to="/statistics">Statistics</Link></Menu.Item>
          </Menu>
        </Header>
        <Content
          style={{
            padding: "0 25px 25px 25px",
            margin: "25px"
          }}
        >
          <Breadcrumb
            style={{
              margin: "0 0 16px 0"
            }}
          >
            <Breadcrumb.Item>
              <Link to="/">Dashboard</Link>
            </Breadcrumb.Item>
            {match && match.params && match.params.storyId && (
              <Breadcrumb.Item>Story #{match.params.storyId}</Breadcrumb.Item>
            )}
          </Breadcrumb>
          {children}
          <div style={{ textAlign: 'center' }}>
            <a href="https://cube.dev/">
              <img src='./powered-by-cubejs-color.svg' style={{ width: 200 }}/>
            </a>
          </div>
        </Content>
      </Layout>
      <GithubCorner size={120} href="https://github.com/statsbotco/cube.js/tree/master/examples/hn-insights" />
    </div>
  );
}

export default withRouter(App);
