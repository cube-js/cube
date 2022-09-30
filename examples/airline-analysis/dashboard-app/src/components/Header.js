import { Layout, Menu } from 'antd';
import React from 'react';
import { withRouter } from 'react-router';
import { Link } from 'react-router-dom';

const Header = ({
  location
}) => <Layout.Header style={{
  padding: '0 32px'
}}>
    <div style={{
    float: 'left'
  }}>
      <h2 style={{
      color: '#fff',
      margin: 0,
      marginRight: '1em',
      display: 'inline',
      width: 100,
      lineHeight: '54px'
    }}>
        Airline Report
      </h2>
    </div>
    <Menu theme="dark" mode="horizontal" selectedKeys={[location.pathname]} style={{
    lineHeight: '64px'
  }}>
      <Menu.Item key="/">
        <Link to="/">React + Chart JS Example App</Link>
      </Menu.Item>
      <Menu.Item style={{float: 'right'}} key="/github">
        <Link to="/github">GitHub</Link>
      </Menu.Item>
    </Menu>
  </Layout.Header>;

export default withRouter(Header);