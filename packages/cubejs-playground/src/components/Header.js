import React from 'react';
import { Link } from "react-router-dom";
import { Layout, Menu, Icon } from "antd";
import * as PropTypes from 'prop-types';
import styled from 'styled-components';

const StyledMenu = styled(Menu)`
  background: #EEEEF5;
  border-bottom: 0;
`

const StyledMenuItem = styled(Menu.Item)`
  font-size: 15px;
  font-weight: 500;
  & > a {
    opacity: 0.6;
  }
  &.ant-menu-item-selected,
  &.ant-menu-item-active {
    a {
      opacity: 1;
    }
  }

  &&:not(.ant-menu-item-selected) {
    &.ant-menu-item-active, &:hover {
      color: #43436B;
      border-bottom: 2px solid transparent;
    }
  }
`

const Header = ({ selectedKeys }) => (
  <Layout.Header style={{ padding: '0 32px' }}>
    <div style={{ float: 'left' }}>
      <img src="./cubejs-playground-logo.svg" style={{ display: 'inline', height: 28, marginRight: 28 }} alt="" />
    </div>
    <StyledMenu
      theme="light"
      mode="horizontal"
      selectedKeys={selectedKeys}
    >
      <StyledMenuItem key="/build"><Link to="/build">Build</Link></StyledMenuItem>
      <StyledMenuItem key="/dashboard"><Link to="/dashboard">Dashboard App</Link></StyledMenuItem>
      <StyledMenuItem key="/schema"><Link to="/schema">Schema</Link></StyledMenuItem>
      <Menu.Item
        key="docs"
        style={{ float: 'right' }}
      >
        <a href="https://cube.dev/docs" target="_blank">
          <Icon type="book" />
          Docs
        </a>
      </Menu.Item>
      <Menu.Item
        key="slack"
        style={{ float: 'right' }}
      >
        <a href="https://slack.cube.dev" target="_blank">
          <Icon type="slack" />
          Slack
        </a>
      </Menu.Item>
    </StyledMenu>
  </Layout.Header>
);

Header.propTypes = {
  selectedKeys: PropTypes.array.isRequired
};

export default Header;
