import React from 'react';
import ReactDOM from 'react-dom';
import { Layout, Row, Col, Menu, Icon, Radio } from 'antd';
import chartsExamples from './bizChartExamples';
import { toPairs } from 'ramda';
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

    this.state = { activeChart: 'basic', chartLibrary: 'bizcharts' }
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

  renderGroup(group) {
    return toPairs(chartsExamples).filter(([n, c]) => c.group === group).map(([name, c]) =>
      (<div key={name} style={{ marginBottom: 24 }}>{c.render({ chartLibrary: this.state.chartLibrary })}</div>)
    );
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
          >
            <Menu.Item key="basic">Basic Charts</Menu.Item>
            <Menu.Item key="interaction">Interaction</Menu.Item>
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
            { this.renderGroup(this.state.activeChart) }
          </Content>
        </Layout>
      </Layout>
    );
  }
}

ReactDOM.render(<App />, document.getElementById('root'));
