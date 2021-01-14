import { Link } from 'react-router-dom';
import { FileFilled, MenuOutlined, SlackOutlined } from '@ant-design/icons';
import { Dropdown, Layout, Menu } from 'antd';
import * as PropTypes from 'prop-types';
import styled from 'styled-components';
import { useMediaQuery } from 'react-responsive';

const StyledHeader = styled(Layout.Header)`
  && {
    background-color: var(--dark-02-color);
    color: white;
    padding: 0 16px; 
    line-height: 44px; 
    height: 48px;
  }
`;

const StyledMenu = styled(Menu)`
  && {
    background: transparent;
    border-bottom: 0;
  }
`;

const StyledMenuItem = styled(Menu.Item)`
  &&& {
    font-size: 15px;
    font-weight: 500;
    line-height: 48px;
    height: 49px;
    & > a {
      &, &:hover {
        opacity: 0.6;
        color: white;
      }
    }
    &.ant-menu-item-selected,
    &.ant-menu-item-active {
      color: white;
      border-bottom: 2px solid white;
      
      &:hover {
        border-bottom: 2px solid white;      
      }
    
      a {
        opacity: 1;
        color: white;
      }
    }
  
    &:not(.ant-menu-item-selected) {
      &.ant-menu-item-active,
      &:hover {
        color: white;
        border-bottom: 2px solid white;
      }
    }
  }
`;

const StyledMenuButton = styled.a`
  &&& {
    float: right;
    height: 32px;
    margin: 8px ${(props) => (props.noMargin ? '0' : '8px')};
    border: 1px solid rgba(255, 255, 255, 0.35);
    border-radius: 4px;
    display: flex;
    align-items: center;
    color: white;
    transition: all 0.25s ease;
    padding: 0 10px;
  
    span {
      font-size: 14px;
      margin-right: 10px;
    }
  
    &:hover {
      border-color: white;
      color: white;
    }
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
    <StyledHeader>
      <div style={{ float: 'left' }}>
        <img
          src="./cubejs-playground-logo.svg"
          style={{ height: 28, marginRight: 28 }}
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
            <FileFilled />
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
    </StyledHeader>
  );
};

Header.propTypes = {
  selectedKeys: PropTypes.array.isRequired,
};

export default Header;
