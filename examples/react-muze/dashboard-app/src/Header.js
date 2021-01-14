import { Layout, Button, Row, Col, Space, Image, Typography, Divider, Dropdown, Menu } from 'antd';
import { SlackOutlined, GithubOutlined, FileFilled, CloseOutlined, MenuOutlined } from '@ant-design/icons';
import { useMediaQuery } from 'react-responsive';
import logoCube from './logo-cube.svg';
import logoMuze from './logo-muze.svg';
import { divider, muze, logo, x } from './Header.module.less';

const { Header: AntHeader } = Layout;
const { Link, Text } = Typography;
const { Item, ItemGroup } = Menu;

const DesktopMenu = () => (
  <Space>
    <Text>cube.js</Text>
    <Button
      ghost
      target="_blank"
      href="https://cube.dev/docs"
      icon={<FileFilled />}
    >
      Docs
    </Button>
    <Button
      ghost
      target="_blank"
      href="https://github.com/cube-js/cube.js/tree/master/examples/react-muze"
      icon={<GithubOutlined />}
    >
      Github
    </Button>
    <Button
      ghost
      target="_blank"
      href="https://slack.cube.dev"
      icon={<SlackOutlined />}
    >
      Slack
    </Button>
    <Divider type="vertical" className={divider} />
    <Text>MuzeJS</Text>
    <Button
      ghost
      target="_blank"
      href="https://muzejs.org/docs"
      icon={<FileFilled />}
    >
      Docs
    </Button>
  </Space>
);

const MobileMenu = () => (
  <Dropdown
    trigger={['click']}
    overlay={
      <Menu>
        <ItemGroup title="cube.js">
          <Item>
            <Link
              target="_blank"
              href="https://cube.dev/docs"
            >
              <FileFilled />
              Docs
            </Link>
          </Item>
          <Item>
            <Link
              target="_blank"
              href="https://github.com/cube-js/cube.js/tree/master/examples/react-muze"
            >
              <GithubOutlined />
              Github
            </Link>
          </Item>
          <Item>
            <Link
              target="_blank"
              href="https://slack.cube.dev"
            >
              <SlackOutlined />
              Slack
            </Link>
          </Item>
        </ItemGroup>
        <ItemGroup title="MuzeJS">
          <Item>
            <Link
              target="_blank"
              href="https://muzejs.org/docs"
            >
              <FileFilled />
              Docs
            </Link>
          </Item>
        </ItemGroup>
      </Menu>
    }
  >
    <MenuOutlined />
  </Dropdown>
);

const Header = () => {
  const isDesktopOrLaptop = useMediaQuery({
    query: '(min-device-width: 992px)',
  });

  return (
    <AntHeader>
      <Row justify="space-between">
        <Col>
          <Space>
            <Link href="https://cube.dev" target="_blank">
              <Image
                className={logo}
                src={logoCube}
                alt="Cube.js"
                preview={false}
              />
            </Link>
            <CloseOutlined className={x} />
            <Link href="https://muzejs.org" target="_blank">
              <Space size={4}>
                <Image
                  className={logo}
                  src={logoMuze}
                  alt="MuzeJS"
                  preview={false}
                />
                <Text className={muze}>Muze</Text>
              </Space>
            </Link>
          </Space>
        </Col>
        <Col>
          {isDesktopOrLaptop ? <DesktopMenu /> : <MobileMenu />}
        </Col>
      </Row>
    </AntHeader>
  );
};

export default Header;
