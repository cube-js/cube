import React from 'react';
import { Link } from 'react-router-dom';
import { BookOutlined, MenuOutlined, SlackOutlined } from '@ant-design/icons';
import { Layout, Menu, Dropdown } from 'antd';
import * as PropTypes from 'prop-types';
import styled from 'styled-components';
import { useMediaQuery } from 'react-responsive';

const StyledMenu = styled(Menu)`
  background: #eeeef5 !important;
  border-bottom: 0;
`;

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
    &.ant-menu-item-active,
    &:hover {
      color: #43436b;
      border-bottom: 2px solid transparent;
    }
  }
`;

const StyledMenuButton = styled.a`
  float: right;
  height: 32px;
  margin: 8px ${(props) => (props.noMargin ? '0' : '8px')};
  border: 0.5px solid rgba(67, 67, 107, 0.4);
  border-radius: 3px;
  display: flex;
  align-items: center;
  color: #43436b;
  transition: all 0.25s ease;
  padding: 0 10px;

  span {
    font-size: 20px;
    margin-right: 10px;
    opacity: 0.3;
  }

  &:hover {
    span {
      opacity: 0.6;
    }
    border-color: rgba(67, 67, 107, 1);
    color: #43436b;
  }
`;

const Header = ({ selectedKeys }) => {
  const isDesktopOrLaptop = useMediaQuery({
    query: '(min-device-width: 992px)',
  });

  const isMobileOrTable = useMediaQuery({
    query: '(max-device-width: 991px)',
  });

  return (
    <Layout.Header style={{ padding: '0 32px' }}>
      <div style={{ float: 'left' }}>
        <img
          src="./cubejs-playground-logo.svg"
          style={{ display: 'inline', height: 28, marginRight: 28 }}
          alt=""
        />
      </div>
      {isDesktopOrLaptop && (
        <StyledMenu theme="light" mode="horizontal" selectedKeys={selectedKeys}>
          <StyledMenuItem key="/build">
            <Link to="/build">Build</Link>
          </StyledMenuItem>
          <StyledMenuItem key="/dashboard">
            <Link to="/dashboard">Dashboard App</Link>
          </StyledMenuItem>
          <StyledMenuItem key="/schema">
            <Link to="/schema">Schema</Link>
          </StyledMenuItem>
          <StyledMenuButton
            noMargin
            key="slack"
            href="https://slack.cube.dev"
            target="_blank"
          >
            <SlackOutlined />
            Slack
          </StyledMenuButton>
          <StyledMenuButton
            key="docs"
            href="https://cube.dev/docs"
            target="_blank"
          >
            <BookOutlined />
            Docs
          </StyledMenuButton>
        </StyledMenu>
      )}
      {isMobileOrTable && (
        <div style={{ float: 'right' }}>
          <Dropdown
            overlay={
              <Menu>
                <Menu.Item key="/build">
                  <Link to="/build">Build</Link>
                </Menu.Item>
                <Menu.Item key="/dashboard">
                  <Link to="/dashboard">Dashboard App</Link>
                </Menu.Item>
                <Menu.Item key="/schema">
                  <Link to="/schema">Schema</Link>
                </Menu.Item>
              </Menu>
            }
          >
            <MenuOutlined />
          </Dropdown>
        </div>
      )}
    </Layout.Header>
  );
};

Header.propTypes = {
  selectedKeys: PropTypes.array.isRequired,
};

export default Header;
