import React from 'react';
import ReactDOM from 'react-dom';
import { Layout, Row, Col, Menu, Icon } from 'antd';
import chartsExamples from './chartsExamples';
import 'antd/dist/antd.css';
import './css/style.css';

import logo from './img/logo.svg';

const { Header, Footer, Sider, Content } = Layout;
class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = { activeChart: 'line' }
  }

  handleMenuChange(e) {
    this.setState({
      activeChart: e.key
    })
  }

  render() {
    return (
      <Layout>
        <Sider
          trigger={null}
          collapsible
          collapsed={this.state.collapsed}
        >
          <div className="logo">
            <img src={logo} height={42} />
          </div>
          <Menu
            mode="inline"
            theme="dark"
            onClick={this.handleMenuChange.bind(this)}
            defaultSelectedKeys={[this.state.activeChart]}
            defaultOpenKeys={["chartjs"]}
          >
            <Menu.SubMenu key="chartjs" title="Chart.js">
              <Menu.ItemGroup key="g1" title="Line">
                <Menu.Item key="line">Time series</Menu.Item>
                <Menu.Item key="lineMulti">Multi axis</Menu.Item>
              </Menu.ItemGroup>
              <Menu.ItemGroup key="g2" title="Bar">
                <Menu.Item key="bar">Basic</Menu.Item>
                <Menu.Item key="barStacked">Stacked</Menu.Item>
              </Menu.ItemGroup>
              <Menu.ItemGroup key="g3" title="Other">
                <Menu.Item key="pie">Pie</Menu.Item>
              </Menu.ItemGroup>
            </Menu.SubMenu>
          </Menu>
        </Sider>
        <Layout>
          <Header style={{ background: "#fff" }}>
          </Header>
          <Content style={{ padding: '30px', margin: '30px', background: '#fff' }}>
            { chartsExamples[this.state.activeChart].render() }
          </Content>
        </Layout>
      </Layout>
    );
  }
}

ReactDOM.render(<App />, document.getElementById('root'));
