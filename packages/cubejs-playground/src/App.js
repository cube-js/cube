import React, { Component } from 'react';
import { Link } from "react-router-dom";
import "antd/dist/antd.css";
import "./index.css";
import { Layout, Menu } from "antd";
import { fetch } from 'whatwg-fetch';
import { withRouter } from "react-router";

const { Header, Content } = Layout;

class App extends Component {
  async componentDidMount() {
    const res = await fetch('/playground/context');
    const result = await res.json();
    window.analytics && window.analytics.identify(result.anonymousId);
  }

  render() {
    return (
      <Layout style={{ height: '100%' }}>
        <Header style={{ padding: '0 32px'}}>
          <div style={{ float: 'left' }}>
            <img src='./cubejs-logo.svg' style={{ display: 'inline', width: 50}} />
          </div>
          <Menu
            theme="dark"
            mode="horizontal"
            selectedKeys={[this.props.location.pathname]}
            style={{ lineHeight: '64px' }}
          >
            <Menu.Item key="/explore"><Link to="/explore">Explore</Link></Menu.Item>
            <Menu.Item key="/dashboard"><Link to="/dashboard">Dashboard</Link></Menu.Item>
            <Menu.Item key="/schema"><Link to="/schema">Schema</Link></Menu.Item>
          </Menu>
        </Header>
        <Content style={{ height: '100%' }}>
          {this.props.children}
        </Content>
      </Layout>
    );
  }
}

export default withRouter(App);
