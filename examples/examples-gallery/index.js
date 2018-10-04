import React from 'react';
import ReactDOM from 'react-dom';
import { Layout, Row, Col, Menu, Icon } from 'antd';
import chartsExamples from './chartsExamples';
import bizChartExamples from './bizChartExamples';
import 'antd/dist/antd.css';
import './css/style.css';

import logo from './img/logo.svg';

const { Header, Footer, Sider, Content } = Layout;

const allChartsExamples = {
  ...chartsExamples,
  ...Object.keys(bizChartExamples)
    .map(name => ({ [`bizcharts-${name}`]: bizChartExamples[name] })).reduce((a, b) => ({...a, ...b}))
};

class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = { activeChart: 'bizcharts-line' }
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
            defaultOpenKeys={["bizcharts"]}
          >
            <Menu.SubMenu key="bizcharts" title="Bizcharts">
              <Menu.ItemGroup key="g1" title="Line">
                <Menu.Item key="bizcharts-line">Time series</Menu.Item>
                <Menu.Item key="bizcharts-lineMulti">Multi axis</Menu.Item>
              </Menu.ItemGroup>
              <Menu.ItemGroup key="g2" title="Bar">
                <Menu.Item key="bizcharts-bar">Basic</Menu.Item>
                <Menu.Item key="bizcharts-barStacked">Stacked</Menu.Item>
              </Menu.ItemGroup>
              <Menu.ItemGroup key="g3" title="Other">
                <Menu.Item key="bizcharts-pie">Pie</Menu.Item>
              </Menu.ItemGroup>
            </Menu.SubMenu>
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
            { allChartsExamples[this.state.activeChart].render() }
          </Content>
        </Layout>
      </Layout>
    );
  }
}

ReactDOM.render(<App />, document.getElementById('root'));
