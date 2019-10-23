import React from "react";
import { SignOut } from "aws-amplify-react";
import { Layout, Menu } from "antd";
import { Link } from "react-router-dom";
import styled from 'styled-components';
import logo from './logo.svg';

const StyledHeader = styled(Layout.Header)`
  padding: 0 28px
`

const StyledMenu = styled(Menu)`
  background: transparent;
  line-height: 41px;
`

const MenuItemStyled = styled(Menu.Item)`
  && {
    top: 4px;
    border-bottom: 4px solid transparent;

    &:hover {
      border-bottom: 4px solid transparent;
      & > a {
        color: #ffffff;
        opacity: 1;
      }
    }
  }
  &&.ant-menu-item-selected
  {
    color: white;
    border-bottom: 4px solid white;

    & > a {
      opacity: 1;
    }
  }
  && > a {
    color: #ffffff;
    opacity: 0.60;
    font-weight: bold;
    letter-spacing: 0.01em;
  }
`

const Logo = styled.div`
  float: left;
  margin-right 40px;
`

const Button = styled.a`
  && {
    background: rgba(237, 237, 255, 0.2);
    border-radius: 4px;
    padding: 8px 10px;
    opacity: 1 !important;
    line-height: 16px;
    min-width: 71px;
    text-align: center;
    margin-top: 5px;
    font-size: 13px;

    &:hover {
      background: rgba(243, 243, 251, 0.15);
    }
  }
`

const signOutStyles = {
  navButton: {
    color: "white",
    background: "none",
    textTransform: "none",
    fontSize: "13px",
    fontWeight: "bold",
    minWidth: 0
  }
}

const Header = ({ location }) => (
  <StyledHeader >
    <Logo>
      <img src={logo} />
    </Logo>
    <StyledMenu
      mode="horizontal"
      selectedKeys={[location.pathname]}
    >
      <MenuItemStyled key="/explore">
        <Link to="/explore">Explore</Link>
      </MenuItemStyled>
      <MenuItemStyled key="/">
        <Link to="/">Dashboard</Link>
      </MenuItemStyled>
      <MenuItemStyled style={{ float: "right", paddingRight: 0, paddingLeft: 8 }} key="sign-out">
        <Button href="https://slack.cube.dev">Slack</Button>
      </MenuItemStyled>
      <MenuItemStyled style={{ float: "right", paddingRight: 0, paddingLeft: 8 }} key="sign-out">
        <Button href="https://github.com/cube-js/cube.js">GitHub</Button>
      </MenuItemStyled>
      <MenuItemStyled style={{ float: "right", paddingRight: 0, paddingLeft: 8 }} key="sign-out">
        <Button href="https://cube.dev/docs">Docs</Button>
      </MenuItemStyled>
    </StyledMenu>
  </StyledHeader>
);

export default Header;
