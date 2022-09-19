import React from "react";
import { Layout, Menu } from "antd";
import { Link } from "react-router-dom";
import styled from 'styled-components';

const StyledHeader = styled(Layout.Header)`
  padding: 0 28px;
  line-height: 41px;
`

const StyledMenu = styled(Menu)`
  background: transparent;
  border: none;
`

const MenuItemStyled = styled(Menu.Item)`
  && {
    top: 4px;
    border-bottom: 4px solid transparent;

    &:hover {
      border-bottom: 4px solid transparent;
      & a {
        color: #ffffff;
        opacity: 1;
      }
    }
  }
  &&.ant-menu-item-selected
  {
    color: white;
    border-bottom: 4px solid white;

    & a {
      opacity: 1;
    }

    &:after {
      border-bottom: 0;
    }
  }
  && a {
    color: #ffffff;
    opacity: 0.60;
    font-weight: bold;
    letter-spacing: 0.01em;
  }
  &&:after {
    border-bottom: 0;
  }
  &&:hover {
    &&:after {
      border-bottom: 0; 
    }
  }
`

const Header = ({ location }) => (
  <StyledHeader >
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
    </StyledMenu>
  </StyledHeader>
);

export default Header;
