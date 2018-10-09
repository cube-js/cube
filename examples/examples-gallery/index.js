import React from 'react';
import ReactDOM from 'react-dom';
import { Layout, Row, Col, Menu, Icon, Radio } from 'antd';
import chartsExamples from './bizChartExamples';
import 'antd/dist/antd.css';
import './css/style.css';

import logo from './img/logo.svg';

const RadioButton = Radio.Button;
const RadioGroup = Radio.Group;

const { Header, Footer, Sider, Content } = Layout;

const allChartsExamples = chartsExamples;

class App extends React.Component {
  constructor(props) {
    super(props);

    this.state = { activeChart: 'line', chartLibrary: 'bizcharts' }
  }

  handleMenuChange(e) {
    this.setState({
      activeChart: e.key
    })
  }

  handleChartLibraryChange(e) {
    this.setState({
      chartLibrary: e.target.value
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
            defaultOpenKeys={["basic"]}
          >
            <Menu.SubMenu key="basic" title="Basic Charts">
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
            <Row gutter={24}>
              <Col span={12} style={{ paddingLeft: 20, paddingTop: 10 }}>
                <RadioGroup
                  value={this.state.chartLibrary}
                  onChange={this.handleChartLibraryChange.bind(this)}
                  size="large"
                >
                  <RadioButton value="bizcharts">BizCharts</RadioButton>
                  <RadioButton value="chartjs">Chart.js</RadioButton>
                </RadioGroup>
              </Col>
            </Row>
          </Header>
          <Content style={{ padding: '30px', margin: '30px', background: '#fff' }}>
            { allChartsExamples[this.state.activeChart].render({ chartLibrary: this.state.chartLibrary }) }
          </Content>
        </Layout>
      </Layout>
    );
  }
}

ReactDOM.render(<App />, document.getElementById('root'));
