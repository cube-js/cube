import React from 'react';
import { Link } from 'react-router-dom';
import { withRouter } from 'react-router';
import { Layout, Menu } from 'antd';

const Header = ({
  location
}) => <Layout.Header style={{
  padding: '0 8px'
}}>
    <Menu theme="dark" mode="horizontal" selectedKeys={[location.pathname]}>
      <Menu.Item key="/explore">
        <Link to="/explore">Explore</Link>
      </Menu.Item>
      <Menu.Item key="/">
        <Link to="/">Dashboard</Link>
      </Menu.Item>
    </Menu>
  </Layout.Header>;

export default withRouter(Header);