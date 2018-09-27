import React from 'react';
import ReactDOM from 'react-dom';
import { Layout, Row, Col, Menu, Icon } from 'antd';
import chartsExamples from './chartsExamples';
import 'antd/dist/antd.css';
import './style.css';


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
        <Header style={{ background: "#fff" }}>
          <Menu
            mode="horizontal"
            onClick={this.handleMenuChange.bind(this)}
            defaultSelectedKeys={[this.state.activeChart]}
            style={{ lineHeight: '64px' }}
          >
            <Menu.SubMenu title={<span className="submenu-title-wrapper"><Icon type="line-chart" />Line</span>}>
              <Menu.Item key="line">Time series</Menu.Item>
              <Menu.Item key="lineMulti">Multi axis</Menu.Item>
            </Menu.SubMenu>
            <Menu.SubMenu
              title={
                <span className="submenu-title-wrapper">
                  <Icon type="bar-chart" />Bar
                </span>
              }
            >
              <Menu.Item key="bar">Basic</Menu.Item>
              <Menu.Item key="barStacked">Stacked</Menu.Item>
            </Menu.SubMenu>
            <Menu.Item key="pie">Pie</Menu.Item>
          </Menu>
        </Header>
        <Content style={{ padding: '30px', margin: '30px', background: '#fff' }}>
          { chartsExamples[this.state.activeChart].render() }
        </Content>
      </Layout>
    );
  }
}

ReactDOM.render(<App />, document.getElementById('root'));
