import Icon, {
  FileFilled,
  MenuOutlined,
  SlackOutlined,
} from '@ant-design/icons';
import { Dropdown, Layout, Menu } from 'antd';
import { useMediaQuery } from 'react-responsive';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { DiscourseIcon } from '../../shared/icons/DiscourseIcon';
import { StyledMenu, StyledMenuButton, StyledMenuItem } from './Menu';
import { RunOnCubeCloud } from './RunOnCubeCloud';

const StyledHeader = styled(Layout.Header)`
  && {
    background-color: var(--dark-02-color);
    color: white;
    padding: 0 16px;
    line-height: 44px;
    height: 48px;
  }
`;

export default function Header({ selectedKeys }) {
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
            key="slack"
            href="https://slack.cube.dev"
            target="_blank"
          >
            <SlackOutlined />
            Slack
          </StyledMenuButton>

          <StyledMenuButton
            noMargin
            key="discourse"
            href="https://forum.cube.dev/"
            target="_blank"
          >
            <Icon component={() => <DiscourseIcon />} />
            Discourse
          </StyledMenuButton>

          <StyledMenuButton
            key="docs"
            href="https://cube.dev/docs"
            target="_blank"
          >
            <FileFilled />
            Docs
          </StyledMenuButton>

          <RunOnCubeCloud />
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
}
